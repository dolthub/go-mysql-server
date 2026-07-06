package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	vsql "github.com/dolthub/vitess/go/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	gsql "github.com/dolthub/go-mysql-server/sql"
)

// TestServerCustomListener verifies a caller can provide their own net.Conn implementation for the server to use
func TestServerCustomListener(t *testing.T) {
	dbName := "mydb"
	// create a net.Conn thats based on a golang buffer
	buffer := 1024
	listener := bufconn.Listen(buffer)

	// create the memory database
	memdb := memory.NewDatabase(dbName)
	pro := memory.NewDBProvider(memdb)
	engine := sqle.NewDefault(pro)

	// server config with custom listener
	cfg := server.Config{Listener: listener}
	// since we're using a memory db, we can't rely on server.DefaultSessionBuilder as it causes panics, so explicitly build a memorySessionBuilder
	sessionBuilder := func(ctx context.Context, c *vsql.Conn, addr string) (gsql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := c.UserData.(gsql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := gsql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		return memory.NewSession(gsql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID), pro), nil
	}
	s, err := server.NewServer(cfg, engine, gsql.NewContext, sessionBuilder, nil)
	require.NoError(t, err)

	networkName := "testNetwork"
	// wire up go-mysql-driver to the listener
	mysql.RegisterDialContext(networkName, func(ctx context.Context, addr string) (net.Conn, error) {
		return listener.DialContext(ctx)
	})
	driver, err := mysql.NewConnector(&mysql.Config{
		DBName:               dbName,
		Addr:                 "bufconn",
		Net:                  networkName,
		Passwd:               "",
		User:                 "root",
		AllowNativePasswords: true,
	})
	require.NoError(t, err)

	// start go-mysql-server
	go func() {
		err := s.Start()
		require.NoError(t, err)
	}()

	// open the db, ping it, and run some execs/queries
	db := sql.OpenDB(driver)

	var pingErr error
	for i := 0; i < 3; i++ {
		if pingErr = db.Ping(); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, pingErr)

	_, err = db.Exec("CREATE TABLE table1 (id int)")
	require.NoError(t, err)

	row := db.QueryRow("SHOW TABLES")
	var tableName string
	err = row.Scan(&tableName)
	require.NoError(t, err)
	if tableName != "table1" {
		t.Fatalf("expected to find table1, but got %s", tableName)
	}
}

// startWatcherTestServer boots a go-mysql-server over a real TCP listener and
// returns the underlying engine (so tests can inspect server-side state) and the
// address to connect to. A real socket is used (rather than an in-memory
// bufconn) so that closing a client connection deterministically wakes the
// server's blocked read.
func startWatcherTestServer(t *testing.T, cfg server.Config) (engine *sqle.Engine, host string, port int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pro := memory.NewDBProvider(memory.NewDatabase("mydb"))
	engine = sqle.NewDefault(pro)

	cfg.Listener = listener
	sessionBuilder := func(ctx context.Context, c *vsql.Conn, addr string) (gsql.Session, error) {
		client := gsql.Client{User: "root", Capabilities: c.Capabilities}
		return memory.NewSession(gsql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID), pro), nil
	}
	s, err := server.NewServer(cfg, engine, gsql.NewContext, sessionBuilder, nil)
	require.NoError(t, err)

	go func() {
		_ = s.Start()
	}()
	t.Cleanup(func() { s.Close() })

	tcpAddr := listener.Addr().(*net.TCPAddr)
	return engine, "127.0.0.1", tcpAddr.Port
}

// runningQueries returns the number of processes that are actively executing a
// query (Command == Query). A merely-connected, idle session shows up as a
// Sleep process, so this filter is what distinguishes "a query is running" from
// "a connection exists".
func runningQueries(engine *sqle.Engine) int {
	n := 0
	for _, p := range engine.ProcessList.Processes() {
		if p.Command == gsql.ProcessCommandQuery {
			n++
		}
	}
	return n
}

// runSleepQuery connects with the vitess mysql client and runs SELECT SLEEP on a
// dedicated connection, returning the connection (so the caller can close its
// socket) and a channel that delivers the query's result. Using the vitess
// client (rather than database/sql) gives the test a single, fully-controlled
// connection whose socket it can close directly.
func runSleepQuery(t *testing.T, host string, port int, seconds int) (*vsql.Conn, <-chan error) {
	t.Helper()
	conn, err := vsql.Connect(context.Background(), &vsql.ConnParams{
		Host:   host,
		Port:   port,
		Uname:  "root",
		DbName: "mydb",
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := conn.ExecuteFetch(fmt.Sprintf("SELECT SLEEP(%d)", seconds), 1, false)
		done <- err
	}()
	return conn, done
}

// TestConnectionWatcherCancelsQueryOnDisconnect verifies the end-to-end payoff of
// the connection watcher: when a client drops its connection mid-query, the
// server cancels the running query promptly instead of letting it run to
// completion.
func TestConnectionWatcherCancelsQueryOnDisconnect(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{})

	// SLEEP(45) is far longer than the assertion windows below, so the only way
	// it stops running quickly is if the watcher cancels it.
	conn, queryDone := runSleepQuery(t, host, port, 45)

	// Wait until the query is actually executing on the server (not just that the
	// connection exists).
	require.Eventually(t, func() bool {
		return runningQueries(engine) > 0
	}, 10*time.Second, 10*time.Millisecond, "query never started running on server")

	// Abruptly close the client socket mid-query.
	require.NoError(t, conn.Conn.Close())

	// The watcher should observe the disconnect and cancel the running query
	// well before SLEEP(45) would finish.
	require.Eventually(t, func() bool {
		return runningQueries(engine) == 0
	}, 10*time.Second, 20*time.Millisecond, "query was not cancelled after client disconnect")

	select {
	case <-queryDone:
	case <-time.After(10 * time.Second):
		t.Fatal("client query did not return after disconnect")
	}
}

// TestConnectionWatcherDisabled verifies that DisableConnectionWatcher turns the
// watch off: a client disconnect mid-query is not noticed while the query runs,
// so it stays on the process list.
func TestConnectionWatcherDisabled(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{DisableConnectionWatcher: true})

	// Use a bounded sleep so the deliberately-orphaned query cleans itself up
	// shortly after the test, while still outlasting the assertion window.
	conn, _ := runSleepQuery(t, host, port, 5)

	require.Eventually(t, func() bool {
		return runningQueries(engine) > 0
	}, 10*time.Second, 10*time.Millisecond, "query never started running on server")

	// Abruptly close the client socket mid-query.
	require.NoError(t, conn.Conn.Close())

	// With the watcher disabled, the disconnect must NOT cancel the query: it
	// keeps running throughout the window. (The connection handler is blocked in
	// the running query, so it can't even observe the disconnect until SLEEP
	// finishes.)
	require.Never(t, func() bool {
		return runningQueries(engine) == 0
	}, 2*time.Second, 50*time.Millisecond, "query was cancelled despite watcher being disabled")
}
