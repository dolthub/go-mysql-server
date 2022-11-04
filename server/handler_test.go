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
	"io/ioutil"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestHandlerOutput(t *testing.T) {

	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		time.Second,
		false,
		nil,
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
				callsToCallback:  8,
				lenLastBatch:     114,
				lastRowsAffected: uint64(114),
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
				lenLastBatch:     72,
				lastRowsAffected: uint64(72),
			},
		},
		{
			name:    "with limit set to a number not multiple of the batch capacity",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 530",
			expected: expectedValues{
				callsToCallback:  5,
				lenLastBatch:     18,
				lastRowsAffected: uint64(18),
			},
		},
		{
			name:    "with limit zero",
			handler: handler,
			conn:    dummyConn,
			query:   "SELECT * FROM test limit 0",
			expected: expectedValues{
				callsToCallback:  1,
				lenLastBatch:     0,
				lastRowsAffected: uint64(0),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var callsToCallback int
			var lenLastBatch int
			var lastRowsAffected uint64
			handler.ComInitDB(test.conn, "test")
			err := handler.ComQuery(test.conn, test.query, func(res *sqltypes.Result, more bool) error {
				callsToCallback++
				lenLastBatch = len(res.Rows)
				lastRowsAffected = res.RowsAffected
				return nil
			})

			require.NoError(t, err)
			assert.Equal(t, test.expected.callsToCallback, callsToCallback)
			assert.Equal(t, test.expected.lenLastBatch, lenLastBatch)
			assert.Equal(t, test.expected.lastRowsAffected, lastRowsAffected)

		})
	}
}

func TestHandlerComPrepare(t *testing.T) {
	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		0,
		false,
		nil,
	)
	handler.NewConnection(dummyConn)

	type testcase struct {
		name        string
		statement   string
		expected    []*query.Field
		expectedErr *mysql.SQLError
	}

	for _, test := range []testcase{
		{
			name:      "insert statement returns nil schema",
			statement: "insert into test (c1) values (?)",
			expected:  nil,
		},
		{
			name:      "update statement returns nil schema",
			statement: "update test set c1 = ?",
			expected:  nil,
		},
		{
			name:      "delete statement returns nil schema",
			statement: "delete from test where c1 = ?",
			expected:  nil,
		},
		{
			name:      "select statement returns non-nil schema",
			statement: "select c1 from test where c1 > ?",
			expected: []*query.Field{
				{Name: "c1", Type: query.Type_INT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 11},
			},
		},
		{
			name:        "errors are cast to SQLError",
			statement:   "SELECT * from doesnotexist LIMIT ?",
			expectedErr: mysql.NewSQLError(mysql.ERNoSuchTable, "", "table not found: %s", "doesnotexist"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(dummyConn, "test")
			schema, err := handler.ComPrepare(dummyConn, test.statement)
			if test.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, test.expected, schema)
			} else {
				require.NotNil(t, err)
				sqlErr, isSqlError := err.(*mysql.SQLError)
				require.True(t, isSqlError)
				require.Equal(t, test.expectedErr.Number(), sqlErr.Number())
				require.Equal(t, test.expectedErr.SQLState(), sqlErr.SQLState())
				require.Equal(t, test.expectedErr.Error(), sqlErr.Error())
			}
		})
	}
}

func TestHandlerComPrepareExecute(t *testing.T) {
	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		0,
		false,
		nil,
	)
	handler.NewConnection(dummyConn)

	type testcase struct {
		name     string
		prepare  *mysql.PrepareData
		execute  map[string]*query.BindVariable
		schema   []*query.Field
		expected []sql.Row
	}

	for _, test := range []testcase{
		{
			name: "select statement returns nil schema",
			prepare: &mysql.PrepareData{
				StatementID: 0,
				PrepareStmt: "select c1 from test where c1 < ?",
				ParamsCount: 0,
				ParamsType:  nil,
				ColumnNames: nil,
				BindVars: map[string]*query.BindVariable{
					"v1": {Type: query.Type_INT8, Value: []byte("5")},
				},
			},
			schema: []*query.Field{
				{Name: "c1", Type: query.Type_INT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 11},
			},
			expected: []sql.Row{
				{0}, {1}, {2}, {3}, {4},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(dummyConn, "test")
			schema, err := handler.ComPrepare(dummyConn, test.prepare.PrepareStmt)
			require.NoError(t, err)
			require.Equal(t, test.schema, schema)

			var res []sql.Row
			callback := func(r *sqltypes.Result) error {
				for _, r := range r.Rows {
					var vals []interface{}
					for _, v := range r {
						val, err := strconv.ParseInt(string(v.Raw()), 0, 64)
						if err != nil {
							return err
						}
						vals = append(vals, int(val))
					}
					res = append(res, sql.NewRow(vals...))
				}
				return nil
			}
			err = handler.ComStmtExecute(dummyConn, test.prepare, callback)
			require.NoError(t, err)
			require.Equal(t, test.expected, res)
		})
	}
}

func TestHandlerComPrepareExecuteWithPreparedDisabled(t *testing.T) {
	e := setupMemDB(require.New(t))
	dummyConn := &mysql.Conn{ConnectionID: 1}
	handler := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		0,
		false,
		nil,
	)
	handler.NewConnection(dummyConn)
	analyzer.SetPreparedStmts(true)
	defer func() {
		analyzer.SetPreparedStmts(false)
	}()
	type testcase struct {
		name     string
		prepare  *mysql.PrepareData
		execute  map[string]*query.BindVariable
		schema   []*query.Field
		expected []sql.Row
	}

	for _, test := range []testcase{
		{
			name: "select statement returns nil schema",
			prepare: &mysql.PrepareData{
				StatementID: 0,
				PrepareStmt: "select c1 from test where c1 < ?",
				ParamsCount: 0,
				ParamsType:  nil,
				ColumnNames: nil,
				BindVars: map[string]*query.BindVariable{
					"v1": {Type: query.Type_INT8, Value: []byte("5")},
				},
			},
			schema: []*query.Field{
				{Name: "c1", Type: query.Type_INT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 11},
			},
			expected: []sql.Row{
				{0}, {1}, {2}, {3}, {4},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(dummyConn, "test")
			schema, err := handler.ComPrepare(dummyConn, test.prepare.PrepareStmt)
			require.NoError(t, err)
			require.Equal(t, test.schema, schema)

			var res []sql.Row
			callback := func(r *sqltypes.Result) error {
				for _, r := range r.Rows {
					var vals []interface{}
					for _, v := range r {
						val, err := strconv.ParseInt(string(v.Raw()), 0, 64)
						if err != nil {
							return err
						}
						vals = append(vals, int(val))
					}
					res = append(res, sql.NewRow(vals...))
				}
				return nil
			}
			err = handler.ComStmtExecute(dummyConn, test.prepare, callback)
			require.NoError(t, err)
			require.Equal(t, test.expected, res)
		})
	}
}

type TestListener struct {
	Connections int
	Queries     int
	Disconnects int
	Successes   int
	Failures    int
}

func (tl *TestListener) ClientConnected() {
	tl.Connections++
}

func (tl *TestListener) ClientDisconnected() {
	tl.Disconnects++
}

func (tl *TestListener) QueryStarted() {
	tl.Queries++
}

func (tl *TestListener) QueryCompleted(success bool, duration time.Duration) {
	if success {
		tl.Successes++
	} else {
		tl.Failures++
	}
}

func TestServerEventListener(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)
	listener := &TestListener{}
	handler := NewHandler(
		e,
		NewSessionManager(
			func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
				return sql.NewBaseSessionWithClientServer(addr, sql.Client{Capabilities: conn.Capabilities}, conn.ConnectionID), nil
			},
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			e.MemoryManager,
			e.ProcessList,
			"foo",
		),
		0,
		false,
		listener,
	)

	cb := func(res *sqltypes.Result, more bool) error {
		return nil
	}

	require.Equal(listener.Connections, 0)
	require.Equal(listener.Disconnects, 0)
	require.Equal(listener.Queries, 0)
	require.Equal(listener.Successes, 0)
	require.Equal(listener.Failures, 0)

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	require.Equal(listener.Connections, 1)
	require.Equal(listener.Disconnects, 0)

	err := handler.sm.SetDB(conn1, "test")
	require.NoError(err)

	err = handler.ComQuery(conn1, "SELECT 1", cb)
	require.NoError(err)
	require.Equal(listener.Queries, 1)
	require.Equal(listener.Successes, 1)
	require.Equal(listener.Failures, 0)

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	require.Equal(listener.Connections, 2)
	require.Equal(listener.Disconnects, 0)

	handler.ComInitDB(conn2, "test")
	err = handler.ComQuery(conn2, "select 1", cb)
	require.NoError(err)
	require.Equal(listener.Queries, 2)
	require.Equal(listener.Successes, 2)
	require.Equal(listener.Failures, 0)

	err = handler.ComQuery(conn1, "select bad_col from bad_table with illegal syntax", cb)
	require.Error(err)
	require.Equal(listener.Queries, 3)
	require.Equal(listener.Successes, 2)
	require.Equal(listener.Failures, 1)

	handler.ConnectionClosed(conn1)
	require.Equal(listener.Connections, 2)
	require.Equal(listener.Disconnects, 1)

	handler.ConnectionClosed(conn2)
	require.Equal(listener.Connections, 2)
	require.Equal(listener.Disconnects, 2)

	conn3 := newConn(3)
	_, err = handler.ComPrepare(conn3, "SELECT ?")
	require.NoError(err)
	require.Equal(1, len(e.PreparedData))
	require.NotNil(e.PreparedData[conn3.ConnectionID])

	handler.ConnectionClosed(conn3)
	require.Equal(0, len(e.PreparedData))
}

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	handler := NewHandler(
		e,
		NewSessionManager(
			func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
				return sql.NewBaseSessionWithClientServer(addr, sql.Client{Capabilities: conn.Capabilities}, conn.ConnectionID), nil
			},
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			e.MemoryManager,
			e.ProcessList,
			"foo",
		),
		0,
		false,
		nil,
	)

	conn1 := newConn(1)
	handler.NewConnection(conn1)

	conn2 := newConn(2)
	handler.NewConnection(conn2)

	require.Len(handler.sm.sessions, 0)

	handler.ComInitDB(conn2, "test")
	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result, more bool) error {
		return nil
	})

	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	assertNoConnProcesses(t, e, conn2.ConnectionID)

	err = handler.sm.SetDB(conn1, "test")
	require.NoError(err)
	ctx1, err := handler.sm.NewContextWithQuery(conn1, "SELECT 1")
	require.NoError(err)
	ctx1, err = handler.e.ProcessList.AddProcess(ctx1, "SELECT 1")
	require.NoError(err)

	err = handler.ComQuery(conn2, "KILL "+fmt.Sprint(ctx1.ID()), func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	require.Len(handler.sm.sessions, 1)
	assertNoConnProcesses(t, e, conn1.ConnectionID)
}

func assertNoConnProcesses(t *testing.T, e *sqle.Engine, conn uint32) {
	t.Helper()

	for _, p := range e.ProcessList.Processes() {
		if p.Connection == conn {
			t.Errorf("expecting no processes with connection id %d", conn)
		}
	}
}

func TestSchemaToFields(t *testing.T) {
	require := require.New(t)

	schema := sql.Schema{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", Type: sql.TinyBlob},
		{Name: "blob", Type: sql.Blob},
		{Name: "mediumblob", Type: sql.MediumBlob},
		{Name: "longblob", Type: sql.LongBlob},
		{Name: "tinytext", Type: sql.TinyText},
		{Name: "text", Type: sql.Text},
		{Name: "mediumtext", Type: sql.MediumText},
		{Name: "longtext", Type: sql.LongText},
		{Name: "json", Type: sql.JSON},

		// Geometry Types
		{Name: "geometry", Type: sql.GeometryType{}},
		{Name: "point", Type: sql.PointType{}},
		{Name: "polygon", Type: sql.PolygonType{}},
		{Name: "linestring", Type: sql.LineStringType{}},

		// Integer Types
		{Name: "uint8", Type: sql.Uint8},
		{Name: "int8", Type: sql.Int8},
		{Name: "uint16", Type: sql.Uint16},
		{Name: "int16", Type: sql.Int16},
		{Name: "uint24", Type: sql.Uint24},
		{Name: "int24", Type: sql.Int24},
		{Name: "uint32", Type: sql.Uint32},
		{Name: "int32", Type: sql.Int32},
		{Name: "uint64", Type: sql.Uint64},
		{Name: "int64", Type: sql.Int64},

		// Floating Point and Decimal Types
		{Name: "float32", Type: sql.Float32},
		{Name: "float64", Type: sql.Float64},
		{Name: "decimal10_0", Type: sql.MustCreateDecimalType(10, 0)},
		{Name: "decimal60_30", Type: sql.MustCreateDecimalType(60, 30)},

		// Char, Binary, and Bit Types
		{Name: "varchar50", Type: sql.MustCreateString(sqltypes.VarChar, 50, sql.Collation_Default)},
		{Name: "varbinary12345", Type: sql.MustCreateBinary(sqltypes.VarBinary, 12345)},
		{Name: "binary123", Type: sql.MustCreateBinary(sqltypes.Binary, 123)},
		{Name: "char123", Type: sql.MustCreateString(sqltypes.Char, 123, sql.Collation_Default)},
		{Name: "bit12", Type: sql.MustCreateBitType(12)},

		// Dates
		{Name: "datetime", Type: sql.MustCreateDatetimeType(sqltypes.Datetime)},
		{Name: "timestamp", Type: sql.MustCreateDatetimeType(sqltypes.Timestamp)},
		{Name: "date", Type: sql.MustCreateDatetimeType(sqltypes.Date)},
		{Name: "time", Type: sql.Time},
		{Name: "year", Type: sql.Year},

		// Set and Enum Types
		{Name: "set", Type: sql.MustCreateSetType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
		{Name: "enum", Type: sql.MustCreateEnumType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
	}

	expected := []*query.Field{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 255},
		{Name: "blob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 65_535},
		{Name: "mediumblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 16_777_215},
		{Name: "longblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295},
		{Name: "tinytext", Type: query.Type_TEXT, Charset: mysql.CharacterSetUtf8, ColumnLength: 1020},
		{Name: "text", Type: query.Type_TEXT, Charset: mysql.CharacterSetUtf8, ColumnLength: 262_140},
		{Name: "mediumtext", Type: query.Type_TEXT, Charset: mysql.CharacterSetUtf8, ColumnLength: 67_108_860},
		{Name: "longtext", Type: query.Type_TEXT, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},
		{Name: "json", Type: query.Type_JSON, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},

		// Geometry Types
		{Name: "geometry", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},
		{Name: "point", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},
		{Name: "polygon", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},
		{Name: "linestring", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetUtf8, ColumnLength: 4_294_967_295},

		// Integer Types
		{Name: "uint8", Type: query.Type_UINT8, Charset: mysql.CharacterSetUtf8, ColumnLength: 3},
		{Name: "int8", Type: query.Type_INT8, Charset: mysql.CharacterSetUtf8, ColumnLength: 4},
		{Name: "uint16", Type: query.Type_UINT16, Charset: mysql.CharacterSetUtf8, ColumnLength: 5},
		{Name: "int16", Type: query.Type_INT16, Charset: mysql.CharacterSetUtf8, ColumnLength: 6},
		{Name: "uint24", Type: query.Type_UINT24, Charset: mysql.CharacterSetUtf8, ColumnLength: 8},
		{Name: "int24", Type: query.Type_INT24, Charset: mysql.CharacterSetUtf8, ColumnLength: 9},
		{Name: "uint32", Type: query.Type_UINT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 10},
		{Name: "int32", Type: query.Type_INT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 11},
		{Name: "uint64", Type: query.Type_UINT64, Charset: mysql.CharacterSetUtf8, ColumnLength: 20},
		{Name: "int64", Type: query.Type_INT64, Charset: mysql.CharacterSetUtf8, ColumnLength: 20},

		// Floating Point and Decimal Types
		{Name: "float32", Type: query.Type_FLOAT32, Charset: mysql.CharacterSetUtf8, ColumnLength: 12},
		{Name: "float64", Type: query.Type_FLOAT64, Charset: mysql.CharacterSetUtf8, ColumnLength: 22},
		{Name: "decimal10_0", Type: query.Type_DECIMAL, Charset: mysql.CharacterSetUtf8, ColumnLength: 11},
		{Name: "decimal60_30", Type: query.Type_DECIMAL, Charset: mysql.CharacterSetUtf8, ColumnLength: 62},

		// Char, Binary, and Bit Types
		{Name: "varchar50", Type: query.Type_VARCHAR, Charset: mysql.CharacterSetUtf8, ColumnLength: 50 * 4},
		{Name: "varbinary12345", Type: query.Type_VARBINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 12345},
		{Name: "binary123", Type: query.Type_BINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 123},
		{Name: "char123", Type: query.Type_CHAR, Charset: mysql.CharacterSetUtf8, ColumnLength: 123 * 4},
		{Name: "bit12", Type: query.Type_BIT, Charset: mysql.CharacterSetUtf8, ColumnLength: 12},

		// Dates
		{Name: "datetime", Type: query.Type_DATETIME, Charset: mysql.CharacterSetUtf8, ColumnLength: 26},
		{Name: "timestamp", Type: query.Type_TIMESTAMP, Charset: mysql.CharacterSetUtf8, ColumnLength: 26},
		{Name: "date", Type: query.Type_DATE, Charset: mysql.CharacterSetUtf8, ColumnLength: 10},
		{Name: "time", Type: query.Type_TIME, Charset: mysql.CharacterSetUtf8, ColumnLength: 17},
		{Name: "year", Type: query.Type_YEAR, Charset: mysql.CharacterSetUtf8, ColumnLength: 4},

		// Set and Enum Types
		{Name: "set", Type: query.Type_SET, Charset: mysql.CharacterSetUtf8, ColumnLength: 72},
		{Name: "enum", Type: query.Type_ENUM, Charset: mysql.CharacterSetUtf8, ColumnLength: 20},
	}

	require.Equal(len(schema), len(expected))

	fields := schemaToFields(schema)
	for i := 0; i < len(fields); i++ {
		t.Run(schema[i].Name, func(t *testing.T) {
			assert.Equal(t, expected[i], fields[i])
		})
	}
}

func TestHandlerTimeout(t *testing.T) {
	require := require.New(t)

	e := setupMemDB(require)
	e2 := setupMemDB(require)

	timeOutHandler := NewHandler(
		e, NewSessionManager(testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo"),
		1*time.Second,
		false,
		nil,
	)

	noTimeOutHandler := NewHandler(
		e2, NewSessionManager(testSessionBuilder,
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo"),
		0,
		false,
		nil,
	)
	require.Equal(1*time.Second, timeOutHandler.readTimeout)
	require.Equal(0*time.Second, noTimeOutHandler.readTimeout)

	connTimeout := newConn(1)
	timeOutHandler.NewConnection(connTimeout)

	connNoTimeout := newConn(2)
	noTimeOutHandler.NewConnection(connNoTimeout)

	timeOutHandler.ComInitDB(connTimeout, "test")
	err := timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.EqualError(err, "row read wait bigger than connection timeout (errno 1105) (sqlstate HY000)")

	err = timeOutHandler.ComQuery(connTimeout, "SELECT SLEEP(0.5)", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	noTimeOutHandler.ComInitDB(connNoTimeout, "test")
	err = noTimeOutHandler.ComQuery(connNoTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result, more bool) error {
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
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		0,
		false,
		nil,
	)
	c := newConn(1)
	h.NewConnection(c)

	q := fmt.Sprintf("SELECT SLEEP(%d)", (tcpCheckerSleepDuration * 4 / time.Second))
	h.ComInitDB(c, "test")
	err = h.ComQuery(c, q, func(res *sqltypes.Result, more bool) error {
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
				"bin":       expression.NewLiteral([]byte{byte(0xC0), byte(0x00), byte(0x10)}, sql.MustCreateBinary(query.Type_VARBINARY, int64(3))),
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
			sql.NoopTracer,
			func(ctx *sql.Context, db string) bool { return db == "test" },
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		0,
		false,
		nil,
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
			err := handler.ComQuery(test.conn, test.query, func(res *sqltypes.Result, more bool) error {
				rowsAffected = uint64(res.RowsAffected)
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, test.expectedRowsAffected, rowsAffected)
		})
	}
}

func setupMemDB(require *require.Assertions) *sqle.Engine {
	db := memory.NewDatabase("test")
	pro := memory.NewMemoryDBProvider(db)
	e := sqle.NewDefault(pro)

	tableTest := memory.NewTable("test", sql.NewPrimaryKeySchema(sql.Schema{{Name: "c1", Type: sql.Int32, Source: "test"}}), nil)
	tableTest.EnablePrimaryKeyIndexes()

	for i := 0; i < 1010; i++ {
		require.NoError(tableTest.Insert(
			sql.NewEmptyContext(),
			sql.NewRow(int32(i)),
		))
	}

	db.AddTable("test", tableTest)

	return e
}

func getFreePort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

func testServer(t *testing.T, ready chan struct{}, port string, breakConn bool) {
	l, err := net.Listen("tcp", ":"+port)
	defer func() {
		_ = l.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	conn, err := l.Accept()
	if err != nil {
		return
	}

	if !breakConn {
		defer func() {
			_ = conn.Close()
		}()

		_, err = ioutil.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
	} // else: dirty return without closing or reading to force the socket into TIME_WAIT
}
func okTestServer(t *testing.T, ready chan struct{}, port string) {
	testServer(t, ready, port, false)
}
func brokenTestServer(t *testing.T, ready chan struct{}, port string) {
	testServer(t, ready, port, true)
}

// This session builder is used as dummy mysql Conn is not complete and
// causes panic when accessing remote address.
func testSessionBuilder(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
	return sql.NewBaseSessionWithClientServer(addr, sql.Client{Address: "127.0.0.1:34567", User: c.User, Capabilities: c.Capabilities}, c.ConnectionID), nil
}

type mockConn struct {
	net.Conn
}

func (c *mockConn) Close() error { return nil }

func newConn(id uint32) *mysql.Conn {
	conn := &mysql.Conn{
		ConnectionID: id,
	}

	// Set conn so it does not panic when we close it
	val := reflect.ValueOf(conn).Elem()
	field := val.FieldByName("Conn")
	field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	field.Set(reflect.ValueOf(new(mockConn)))

	return conn
}
