package server

import (
	"net"
	"reflect"
	"testing"
	"unsafe"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v1/mysql"
	"gopkg.in/src-d/go-vitess.v1/sqltypes"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

func setupMemDB(require *require.Assertions) *sqle.Engine {
	e := sqle.NewDefault()
	db := mem.NewDatabase("test")
	e.AddDatabase(db)

	tableTest := mem.NewTable("test", sql.Schema{{Name: "c1", Type: sql.Int32, Source: "test"}})

	for i := 0; i < 1010; i++ {
		require.NoError(tableTest.Insert(
			sql.NewEmptyContext(),
			sql.NewRow(int32(i)),
		))
	}

	db.AddTable("test", tableTest)

	return e
}

func TestHandlerOutput(t *testing.T) {
	// This session builder is used as dummy mysql Conn is not complete and
	// causes panic when accessing remote address.
	testSessionBuilder := func(c *mysql.Conn, addr string) sql.Session {
		client := "127.0.0.1:34567"
		return sql.NewSession(addr, client, c.User, c.ConnectionID)
	}

	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(e, NewSessionManager(testSessionBuilder, opentracing.NoopTracer{}, "foo"))
	handler.NewConnection(dummyConn)

	type exptectedValues struct {
		callsToCallback  int
		lenLastBacth     int
		lastRowsAffected uint64
	}

	tests := []struct {
		name     string
		handler  *Handler
		conn     *mysql.Conn
		query    string
		expected exptectedValues
	}{
		{
			name:    "select all without limit",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test",
			expected: exptectedValues{
				callsToCallback:  11,
				lenLastBacth:     10,
				lastRowsAffected: uint64(10),
			},
		},
		{
			name:    "with limit equal to batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 100",
			expected: exptectedValues{
				callsToCallback:  1,
				lenLastBacth:     100,
				lastRowsAffected: uint64(100),
			},
		},
		{
			name:    "with limit less than batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 60",
			expected: exptectedValues{
				callsToCallback:  1,
				lenLastBacth:     60,
				lastRowsAffected: uint64(60),
			},
		},
		{
			name:    "with limit greater than batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 200",
			expected: exptectedValues{
				callsToCallback:  2,
				lenLastBacth:     100,
				lastRowsAffected: uint64(100),
			},
		},
		{
			name:    "with limit set to a number not multiple of the batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 530",
			expected: exptectedValues{
				callsToCallback:  6,
				lenLastBacth:     30,
				lastRowsAffected: uint64(30),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var callsToCallback int
			var lenLastBacth int
			var lastRowsAffected uint64
			err := handler.ComQuery(test.conn, test.query, func(res *sqltypes.Result) error {
				callsToCallback++
				lenLastBacth = len(res.Rows)
				lastRowsAffected = res.RowsAffected
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, test.expected.callsToCallback, callsToCallback)
			require.Equal(t, test.expected.lenLastBacth, lenLastBacth)
			require.Equal(t, test.expected.lastRowsAffected, lastRowsAffected)

		})
	}
}

func newConn(id uint32) *mysql.Conn {
	conn := &mysql.Conn{
		ConnectionID: id,
	}

	// Set conn so it does not panic when we close it
	val := reflect.ValueOf(conn).Elem()
	field := val.FieldByName("conn")
	field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	field.Set(reflect.ValueOf(new(mockConn)))

	return conn
}

type mockConn struct {
	net.Conn
}

func (c *mockConn) Close() error { return nil }

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	handler := NewHandler(
		e,
		NewSessionManager(
			func(conn *mysql.Conn, addr string) sql.Session {
				return sql.NewBaseSession()
			},
			opentracing.NoopTracer{},
			"foo",
		),
	)

	require.Len(handler.c, 0)

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	conn2 := newConn(2)
	handler.NewConnection(conn2)

	require.Len(handler.sm.sessions, 0)
	require.Len(handler.c, 2)

	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result) error {
		return nil
	})

	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	require.Len(handler.c, 2)
	require.Equal(conn1, handler.c[1])
	require.Equal(conn2, handler.c[2])

	err = handler.ComQuery(conn2, "KILL 1", func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)

	require.Len(handler.sm.sessions, 0)
	require.Len(handler.c, 1)
	require.Equal(conn1, handler.c[1])
}
