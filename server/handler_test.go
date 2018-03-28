package server

import (
	"testing"

	"github.com/opentracing/opentracing-go"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
	"gopkg.in/src-d/go-vitess.v0/sqltypes"

	"github.com/stretchr/testify/require"
)

func setupMemDB(require *require.Assertions) *sqle.Engine {
	e := sqle.New()
	db := mem.NewDatabase("test")
	e.AddDatabase(db)

	memDb, ok := db.(*mem.Database)
	require.True(ok)

	t1 := mem.NewTable("test", sql.Schema{{Name: "c1", Type: sql.Int32, Source: "test"}})

	for i := 0; i < 101; i++ {
		require.NoError(t1.Insert(sql.NewRow(int32(i))))
	}

	memDb.AddTable("test", t1)

	return e
}

func TestHandlerOutput(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	dummyConn := &mysql.Conn{ConnectionID: 1}

	handler := NewHandler(e, NewSessionManager(DefaultSessionBuilder, opentracing.NoopTracer{}))

	c := 0
	var lastRowsAffected uint64
	var lastRows int
	err := handler.ComQuery(dummyConn, "SELECT * FROM test limit 100", func(res *sqltypes.Result) error {
		c++
		lastRowsAffected = res.RowsAffected
		lastRows = len(res.Rows)
		return nil
	})
	require.NoError(err)
	require.Equal(1, c)
	require.Equal(100, lastRows)
	require.Equal(uint64(100), lastRowsAffected)

	c = 0
	lastRows = 0
	lastRowsAffected = 0
	err = handler.ComQuery(dummyConn, "SELECT * FROM test", func(res *sqltypes.Result) error {
		c++
		lastRowsAffected = res.RowsAffected
		lastRows = len(res.Rows)
		return nil
	})
	require.NoError(err)
	require.Equal(2, c)
	require.Equal(1, lastRows)
	require.Equal(uint64(1), lastRowsAffected)

}

func newConn(id uint32) *mysql.Conn {
	return &mysql.Conn{
		ConnectionID: id,
	}
}

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	handler := NewHandler(e,
		NewSessionManager(func(conn *mysql.Conn) sql.Session {
			return sql.NewBaseSession()
		}, opentracing.NoopTracer{}))

	require.Len(handler.c, 0)

	conn1 := newConn(1)

	handler.NewConnection(conn1)

	require.Len(handler.c, 1)
	c, ok := handler.c[1]
	require.True(ok)
	require.Equal(conn1, c)

	conn2 := newConn(2)

	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result) error {
		return nil
	})

	require.NoError(err)

	require.Len(handler.c, 1)
	c, ok = handler.c[1]
	require.True(ok)
	require.Equal(conn1, c)

	// Cannot test KILL CONNECTION as the connection can not be mocked. Calling
	// mysql.Conn.Close panics.
}
