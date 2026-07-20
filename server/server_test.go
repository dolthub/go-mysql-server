package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"runtime"
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

// sleepingConns counts connected sessions with no query running -- the state an
// idle-in-transaction connection sits in.
func sleepingConns(engine *sqle.Engine) int {
	n := 0
	for _, p := range engine.ProcessList.Processes() {
		if p.Command == gsql.ProcessCommandSleep {
			n++
		}
	}
	return n
}

// connectClient opens a vitess client connection without issuing a query.
func connectClient(t *testing.T, host string, port int) *vsql.Conn {
	t.Helper()
	conn, err := vsql.Connect(context.Background(), &vsql.ConnParams{
		Host:   host,
		Port:   port,
		Uname:  "root",
		DbName: "mydb",
	})
	require.NoError(t, err)
	return conn
}

// seedTable creates a table on a short-lived, cleanly-closed connection.
func seedTable(t *testing.T, host string, port int) {
	t.Helper()
	conn := connectClient(t, host, port)
	defer conn.Close()
	_, err := conn.ExecuteFetch("CREATE TABLE t (id INT PRIMARY KEY)", 1, false)
	require.NoError(t, err)
}

// TestConnectionWatcherHalfOpenNotReaped: unlike a clean disconnect, a client
// that stops reading without sending a FIN leaves the socket silent but open. The
// watcher's Peek never returns, so a mid-query disconnect is NOT reaped.
//
// The client here is alive-but-silent (its kernel still ACKs), which is
// indistinguishable from a legitimately long query the client is awaiting, so
// this is correctly NOT reapable by liveness detection -- TCP keepalive leaves it
// alone by design. The only thing that reaps an alive-but-silent socket is
// net_read_timeout; see TestConnReadTimeoutReapsHalfOpenQuery. (Keepalive instead
// targets a truly dead peer -- host crash / partition -- which cannot be
// simulated on loopback; see TestAcceptedConnHasKeepAlive for that wiring.)
func TestConnectionWatcherHalfOpenNotReaped(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{})

	// Long enough to outlast the window, short enough to self-clean after.
	conn, _ := runSleepQuery(t, host, port, 10)
	// Keep conn referenced so its fd isn't closed -- a close would make this the
	// clean-disconnect case the watcher handles.
	defer runtime.KeepAlive(conn)

	require.Eventually(t, func() bool {
		return runningQueries(engine) > 0
	}, 10*time.Second, 10*time.Millisecond, "query never started running on server")

	// Do NOT close conn: silent but open (half-open). ConnReadTimeout is unset, so
	// the watcher's Peek blocks forever and the query keeps running.
	require.Never(t, func() bool {
		return runningQueries(engine) == 0
	}, 3*time.Second, 50*time.Millisecond,
		"half-open client's query was reaped, but the watcher cannot see a silent socket")
}

// TestConnReadTimeoutReapsHalfOpenQuery: net_read_timeout is the only backstop
// for a half-open mid-query disconnect -- its read deadline wakes the watcher's
// Peek, cancelling the query. (Default is infinite in GMS, 8h in Dolt.)
func TestConnReadTimeoutReapsHalfOpenQuery(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{
		ConnReadTimeout: 2 * time.Second,
	})

	conn, _ := runSleepQuery(t, host, port, 30)
	defer runtime.KeepAlive(conn)

	require.Eventually(t, func() bool {
		return runningQueries(engine) > 0
	}, 10*time.Second, 10*time.Millisecond, "query never started running on server")

	// Silent (half-open): the read deadline fires ~2s later and cancels the query.
	require.Eventually(t, func() bool {
		return runningQueries(engine) == 0
	}, 10*time.Second, 50*time.Millisecond,
		"half-open query was not reaped by net_read_timeout backstop")
}

// TestIdleTransactionHalfOpenNotReaped: a client opens a transaction (BEGIN +
// INSERT) then goes silent while idle. The session sits in Sleep holding the
// transaction; the watcher only examines running queries, so it is never reaped.
//
// As in TestConnectionWatcherHalfOpenNotReaped, the client is alive-but-silent
// (kernel still ACKs), which is indistinguishable from a healthy idle connection,
// so it is correctly NOT reapable by liveness detection -- TCP keepalive leaves
// it alone. The only backstop for an alive-but-silent idle session is
// net_read_timeout; see TestConnReadTimeoutReapsIdleTransaction. (Keepalive
// targets a truly dead peer, which reaps the connection and rolls back its
// transaction via the existing ConnectionClosed teardown.)
func TestIdleTransactionHalfOpenNotReaped(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{})

	seedTable(t, host, port)

	conn := connectClient(t, host, port)
	defer runtime.KeepAlive(conn)
	_, err := conn.ExecuteFetch("BEGIN", 1, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("INSERT INTO t VALUES (1)", 1, false)
	require.NoError(t, err)

	// Write returned: session is idle (Sleep) but still holds the transaction.
	require.Eventually(t, func() bool {
		return sleepingConns(engine) >= 1
	}, 10*time.Second, 10*time.Millisecond, "session never reached idle (Sleep) state")

	// Do NOT close conn: silent but open (half-open). No query runs, so the watcher
	// never examines it and the transaction lingers.
	require.Never(t, func() bool {
		return sleepingConns(engine) == 0
	}, 3*time.Second, 50*time.Millisecond,
		"idle-in-transaction session was reaped, but the watcher never examines Sleep-state conns")
	require.Zero(t, runningQueries(engine), "no query should be running for an idle session")
}

// TestConnReadTimeoutReapsIdleTransaction: net_read_timeout is the only backstop
// for an idle-in-transaction half-open client -- the handler's next-command read
// deadline fires, closing the connection and reaping the transaction.
func TestConnReadTimeoutReapsIdleTransaction(t *testing.T) {
	engine, host, port := startWatcherTestServer(t, server.Config{
		ConnReadTimeout: 2 * time.Second,
	})

	seedTable(t, host, port)

	conn := connectClient(t, host, port)
	defer runtime.KeepAlive(conn)
	_, err := conn.ExecuteFetch("BEGIN", 1, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("INSERT INTO t VALUES (1)", 1, false)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return sleepingConns(engine) >= 1
	}, 10*time.Second, 10*time.Millisecond, "session never reached idle (Sleep) state")

	// Silent (half-open): the read deadline fires ~2s later and reaps the conn.
	require.Eventually(t, func() bool {
		return sleepingConns(engine) == 0
	}, 10*time.Second, 50*time.Millisecond,
		"idle-in-transaction session was not reaped by net_read_timeout backstop")
}
