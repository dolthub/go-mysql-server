// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestHandlerOutput(t *testing.T) {

	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			opentracing.NoopTracer{},
			func(db string) bool { return db == "test" },
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
			handler.ComInitDB(test.conn, "test")
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
			func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error) {
				return sql.NewSession(addr, sql.Client{Capabilities: conn.Capabilities}, conn.ConnectionID), sql.NewIndexRegistry(), sql.NewViewRegistry(), nil
			},
			opentracing.NoopTracer{},
			func(db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)

	conn1 := newConn(1)
	handler.NewConnection(conn1)

	conn2 := newConn(2)
	handler.NewConnection(conn2)

	require.Len(handler.sm.sessions, 0)

	handler.ComInitDB(conn2, "test")
	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result) error {
		return nil
	})

	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	assertNoConnProcesses(t, e, conn2.ConnectionID)

	err = handler.sm.SetDB(conn1, "test")
	require.NoError(err)
	ctx1, err := handler.sm.NewContextWithQuery(conn1, "SELECT 1")
	require.NoError(err)
	ctx1, err = handler.e.Catalog.AddProcess(ctx1, "SELECT 1")
	require.NoError(err)

	err = handler.ComQuery(conn2, "KILL "+fmt.Sprint(ctx1.ID()), func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
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
			func(db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			"foo"),
		1*time.Second)

	noTimeOutHandler := NewHandler(
		e2, NewSessionManager(testSessionBuilder,
			opentracing.NoopTracer{},
			func(db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			"foo"),
		0)
	require.Equal(1*time.Second, timeOutHandler.readTimeout)
	require.Equal(0*time.Second, noTimeOutHandler.readTimeout)

	connTimeout := newConn(1)
	timeOutHandler.NewConnection(connTimeout)

	connNoTimeout := newConn(2)
	noTimeOutHandler.NewConnection(connNoTimeout)

	timeOutHandler.ComInitDB(connTimeout, "test")
	err := timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result) error {
		return nil
	})
	require.EqualError(err, "row read wait bigger than connection timeout (errno 1105) (sqlstate HY000)")

	err = timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(0.5)", func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)

	noTimeOutHandler.ComInitDB(connNoTimeout, "test")
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
			func(db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)
	c := newConn(1)
	h.NewConnection(c)

	q := fmt.Sprintf("SELECT SLEEP(%d)", tcpCheckerSleepTime*4)
	h.ComInitDB(c, "test")
	err = h.ComQuery(c, q, func(res *sqltypes.Result) error {
		return nil
	})
	require.NoError(err)
}

func TestBindingsToExprs(t *testing.T) {
	type tc struct {
		Name     string
		Bindings map[string]*query.BindVariable
		Result   map[string]sql.Expression
		Err      bool
	}

	cases := []tc{
		{
			"Empty",
			map[string]*query.BindVariable{},
			map[string]sql.Expression{},
			false,
		},
		{
			"BadInt",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_INT8, Value: []byte("axqut")},
			},
			nil,
			true,
		},
		{
			"BadUint",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_UINT8, Value: []byte("-12")},
			},
			nil,
			true,
		},
		{
			"BadDecimal",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DECIMAL, Value: []byte("axqut")},
			},
			nil,
			true,
		},
		{
			"BadBit",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_BIT, Value: []byte{byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0)}},
			},
			nil,
			true,
		},
		{
			"BadDate",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DATE, Value: []byte("00000000")},
			},
			nil,
			true,
		},
		{
			"BadYear",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_YEAR, Value: []byte("asdf")},
			},
			nil,
			true,
		},
		{
			"BadDatetime",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DATETIME, Value: []byte("0000")},
			},
			nil,
			true,
		},
		{
			"BadTimestamp",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_TIMESTAMP, Value: []byte("0000")},
			},
			nil,
			true,
		},
		{
			"SomeTypes",
			map[string]*query.BindVariable{
				"i8":        &query.BindVariable{Type: query.Type_INT8, Value: []byte("12")},
				"u64":       &query.BindVariable{Type: query.Type_UINT64, Value: []byte("4096")},
				"bin":       &query.BindVariable{Type: query.Type_VARBINARY, Value: []byte{byte(0xC0), byte(0x00), byte(0x10)}},
				"text":      &query.BindVariable{Type: query.Type_TEXT, Value: []byte("four score and seven years ago...")},
				"bit":       &query.BindVariable{Type: query.Type_BIT, Value: []byte{byte(0x0f)}},
				"date":      &query.BindVariable{Type: query.Type_DATE, Value: []byte("2020-10-20")},
				"year":      &query.BindVariable{Type: query.Type_YEAR, Value: []byte("2020")},
				"datetime":  &query.BindVariable{Type: query.Type_DATETIME, Value: []byte("2020-10-20T12:00:00Z")},
				"timestamp": &query.BindVariable{Type: query.Type_TIMESTAMP, Value: []byte("2020-10-20T12:00:00Z")},
			},
			map[string]sql.Expression{
				"i8":        expression.NewLiteral(int64(12), sql.Int64),
				"u64":       expression.NewLiteral(uint64(4096), sql.Uint64),
				"bin":       expression.NewLiteral(string([]byte{byte(0xC0), byte(0x00), byte(0x10)}), sql.MustCreateBinary(query.Type_VARBINARY, int64(3))),
				"text":      expression.NewLiteral("four score and seven years ago...", sql.MustCreateStringWithDefaults(query.Type_TEXT, 33)),
				"bit":       expression.NewLiteral(uint64(0x0f), sql.MustCreateBitType(sql.BitTypeMaxBits)),
				"date":      expression.NewLiteral(time.Date(2020, time.Month(10), 20, 0, 0, 0, 0, time.UTC), sql.Date),
				"year":      expression.NewLiteral(int16(2020), sql.Year),
				"datetime":  expression.NewLiteral(time.Date(2020, time.Month(10), 20, 12, 0, 0, 0, time.UTC), sql.Datetime),
				"timestamp": expression.NewLiteral(time.Date(2020, time.Month(10), 20, 12, 0, 0, 0, time.UTC), sql.Timestamp),
			},
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := bindingsToExprs(c.Bindings)
			if !c.Err {
				require.NoError(t, err)
				require.Equal(t, c.Result, res)
			} else {
				require.Error(t, err, "%v", res)
			}
		})
	}
}

// Tests the CLIENT_FOUND_ROWS capabilities flag
func TestHandlerFoundRowsCapabilities(t *testing.T) {
	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}

	// Set the capabilities to include found rows
	dummyConn.Capabilities = mysql.CapabilityClientFoundRows

	// Setup the handler
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			opentracing.NoopTracer{},
			func(db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)

	tests := []struct {
		name                 string
		handler              *Handler
		conn                 *mysql.Conn
		query                string
		expectedRowsAffected uint64
	}{
		{
			name:                 "Update query should return number of rows matched instead of rows affected",
			handler:              handler,
			conn:                 dummyConn,
			query:                "UPDATE test set c1 = c1 where c1 < 10",
			expectedRowsAffected: uint64(10),
		},
		{
			name:                 "INSERT ON UPDATE returns +1 for every row that already exists",
			handler:              handler,
			conn:                 dummyConn,
			query:                "INSERT INTO test VALUES (1), (2), (3) ON DUPLICATE KEY UPDATE c1=c1",
			expectedRowsAffected: uint64(3),
		},
		{
			name:                 "SQL_CALC_ROWS should not affect CLIENT_FOUND_ROWS output",
			handler:              handler,
			conn:                 dummyConn,
			query:                "SELECT SQL_CALC_FOUND_ROWS * FROM test LIMIT 5",
			expectedRowsAffected: uint64(5),
		},
		{
			name:                 "INSERT returns rows affected",
			handler:              handler,
			conn:                 dummyConn,
			query:                "INSERT into test VALUES (10000),(10001),(10002)",
			expectedRowsAffected: uint64(3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(test.conn, "test")
			var rowsAffected uint64
			err := handler.ComQuery(test.conn, test.query, func(res *sqltypes.Result) error {
				rowsAffected = uint64(res.RowsAffected)
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, test.expectedRowsAffected, rowsAffected)
		})
	}
}
