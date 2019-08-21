package server

import (
	"fmt"
	"net"
	"testing"
	"time"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

func TestHandlerOutput(t *testing.T) {

	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)
	handler.NewConnection(dummyConn)

	type expectedValues struct {
		callsToCallback  int
		lenLastBatch     int
		lastRowsAffected uint64
	}

	tests := []struct {
		name     string
		handler  *Handler
		conn     *mysql.Conn
		query    string
		expected expectedValues
	}{
		{
			name:    "select all without limit",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test",
			expected: expectedValues{
				callsToCallback:  11,
				lenLastBatch:     10,
				lastRowsAffected: uint64(10),
			},
		},
		{
			name:    "with limit equal to batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 100",
			expected: expectedValues{
				callsToCallback:  1,
				lenLastBatch:     100,
				lastRowsAffected: uint64(100),
			},
		},
		{
			name:    "with limit less than batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 60",
			expected: expectedValues{
				callsToCallback:  1,
				lenLastBatch:     60,
				lastRowsAffected: uint64(60),
			},
		},
		{
			name:    "with limit greater than batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 200",
			expected: expectedValues{
				callsToCallback:  2,
				lenLastBatch:     100,
				lastRowsAffected: uint64(100),
			},
		},
		{
			name:    "with limit set to a number not multiple of the batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 530",
			expected: expectedValues{
				callsToCallback:  6,
				lenLastBatch:     30,
				lastRowsAffected: uint64(30),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var callsToCallback int
			var lenLastBatch int
			var lastRowsAffected uint64
			err := handler.ComQuery(test.conn, test.query, func(res *sqltypes.Result) error {
				callsToCallback++
				lenLastBatch = len(res.Rows)
				lastRowsAffected = res.RowsAffected
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, test.expected.callsToCallback, callsToCallback)
			require.Equal(t, test.expected.lenLastBatch, lenLastBatch)
			require.Equal(t, test.expected.lastRowsAffected, lastRowsAffected)

		})
	}
}

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	handler := NewHandler(
		e,
		NewSessionManager(
			func(conn *mysql.Conn, addr string) sql.Session {
				return sql.NewSession(addr, "", "", conn.ConnectionID)
			},
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)

	require.Len(handler.c, 0)

	var dummyNetConn net.Conn
	conn1 := newConn(1)
	conntainer1 := conntainer{conn1, dummyNetConn}
	handler.NewConnection(conn1)

	conn2 := newConn(2)
	conntainer2 := conntainer{conn2, dummyNetConn}
	handler.NewConnection(conn2)

	require.Len(handler.sm.sessions, 0)
	require.Len(handler.c, 2)

	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result) error {
		return nil
	})

	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	require.Len(handler.c, 2)
	require.Equal(conntainer1, handler.c[1])
	require.Equal(conntainer2, handler.c[2])

	assertNoConnProcesses(t, e, conn2.ConnectionID)

	ctx1 := handler.sm.NewContextWithQuery(conn1, "SELECT 1")
	ctx1, err = handler.e.Catalog.AddProcess(ctx1, sql.QueryProcess, "SELECT 1")
	require.NoError(err)

	err = handler.ComQuery(conn2, "KILL "+fmt.Sprint(ctx1.ID()), func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	require.Len(handler.c, 1)
	_, ok := handler.c[1]
	require.False(ok)
	assertNoConnProcesses(t, e, conn1.ConnectionID)
}

func assertNoConnProcesses(t *testing.T, e *sqle.Engine, conn uint32) {
	t.Helper()

	for _, p := range e.Catalog.Processes() {
		if p.Connection == conn {
			t.Errorf("expecting no processes with connection id %d", conn)
		}
	}
}

func TestSchemaToFields(t *testing.T) {
	require := require.New(t)

	schema := sql.Schema{
		{Name: "foo", Type: sql.Blob},
		{Name: "bar", Type: sql.Text},
		{Name: "baz", Type: sql.Int64},
	}

	expected := []*query.Field{
		{Name: "foo", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary},
		{Name: "bar", Type: query.Type_TEXT, Charset: mysql.CharacterSetUtf8},
		{Name: "baz", Type: query.Type_INT64, Charset: mysql.CharacterSetUtf8},
	}

	fields := schemaToFields(schema)
	require.Equal(expected, fields)
}

func TestHandlerTimeout(t *testing.T) {
	require := require.New(t)

	e := setupMemDB(require)
	e2 := setupMemDB(require)

	timeOutHandler := NewHandler(
		e, NewSessionManager(testSessionBuilder,
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo"),
		1*time.Second)

	noTimeOutHandler := NewHandler(
		e2, NewSessionManager(testSessionBuilder,
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo"),
		0)
	require.Equal(1*time.Second, timeOutHandler.readTimeout)
	require.Equal(0*time.Second, noTimeOutHandler.readTimeout)

	connTimeout := newConn(1)
	timeOutHandler.NewConnection(connTimeout)

	connNoTimeout := newConn(2)
	noTimeOutHandler.NewConnection(connNoTimeout)

	err := timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result) error {
		return nil
	})
	require.EqualError(err, "row read wait bigger than connection timeout")

	err = timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(0.5)", func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)

	err = noTimeOutHandler.ComQuery(connNoTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)
}
func TestOkClosedConnection(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)
	port, err := getFreePort()
	require.NoError(err)

	ready := make(chan struct{})
	go okTestServer(t, ready, port)
	<-ready
	conn, err := net.Dial("tcp", "localhost:"+port)
	require.NoError(err)
	defer func() {
		_ = conn.Close()
	}()

	h := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)
	h.AddNetConnection(&conn)
	c2 := newConn(2)
	h.NewConnection(c2)

	q := fmt.Sprintf("SELECT SLEEP(%d)", tcpCheckerSleepTime*4)
	err = h.ComQuery(c2, q, func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)
}
