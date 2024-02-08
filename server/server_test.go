package server_test

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	vsql "github.com/dolthub/vitess/go/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	gsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"google.golang.org/grpc/test/bufconn"
)

// TestSeverCustomListener verifies a caller can provide their own net.Conn implementation for the server to use
func TestSeverCustomListener(t *testing.T) {
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
		mysqlConnectionUser, ok := c.UserData.(mysql_db.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := gsql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		return memory.NewSession(gsql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID), pro), nil
	}
	s, err := server.NewServer(cfg, engine, sessionBuilder, nil)
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
	require.NoError(t, err)

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
