package server

import (
	"context"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
	"gopkg.in/src-d/go-vitess.v0/sqltypes"

	"github.com/stretchr/testify/require"
)

func TestHandlerOutput(t *testing.T) {
	require := require.New(t)
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

	dummyConn := &mysql.Conn{ConnectionID: 1}

	handler := NewHandler(e,
		NewSessionManager(func(ctx context.Context, conn *mysql.Conn) sql.Session {
			return sql.NewBaseSession(ctx)
		}))

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
