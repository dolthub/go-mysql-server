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
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestHandlerOutput(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database

	dummyConn := newConn(1)
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}
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

	t.Run("sum aggregation type is correct", func(t *testing.T) {
		handler.ComInitDB(dummyConn, "test")
		var result *sqltypes.Result
		err := handler.ComQuery(dummyConn, "select sum(1) from test", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Float64, result.Rows[0][0].Type())
		require.Equal(t, []byte("1010"), result.Rows[0][0].ToBytes())
	})

	t.Run("avg aggregation type is correct", func(t *testing.T) {
		handler.ComInitDB(dummyConn, "test")
		var result *sqltypes.Result
		err := handler.ComQuery(dummyConn, "select avg(1) from test", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Float64, result.Rows[0][0].Type())
		require.Equal(t, []byte("1"), result.Rows[0][0].ToBytes())
	})

	t.Run("if() type is correct", func(t *testing.T) {
		handler.ComInitDB(dummyConn, "test")
		var result *sqltypes.Result
		err := handler.ComQuery(dummyConn, "select if(1, 123, 'def')", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Text, result.Rows[0][0].Type())
		require.Equal(t, []byte("123"), result.Rows[0][0].ToBytes())

		err = handler.ComQuery(dummyConn, "select if(0, 123, 456)", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Int64, result.Rows[0][0].Type())
		require.Equal(t, []byte("456"), result.Rows[0][0].ToBytes())
	})
}

func TestHandlerComPrepare(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dummyConn := newConn(1)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}
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
				{Name: "c1", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11},
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
	e, pro := setupMemDB(require.New(t))
	dummyConn := newConn(1)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}
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
				{Name: "c1", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11},
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
	e, pro := setupMemDB(require.New(t))
	dummyConn := newConn(1)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}
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
				{Name: "c1", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11},
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
	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	listener := &TestListener{}
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
				return sql.NewBaseSessionWithClientServer(addr, sql.Client{Capabilities: conn.Capabilities}, conn.ConnectionID), nil
			},
			sql.NoopTracer,
			dbFunc,
			e.MemoryManager,
			e.ProcessList,
			"foo",
		),
		sel: listener,
	}

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
	query := "SELECT ?"
	_, err = handler.ComPrepare(conn3, query)
	require.NoError(err)
	require.Equal(1, len(e.PreparedDataCache.GetSessionData(conn3.ConnectionID)))
	require.NotNil(e.PreparedDataCache.GetCachedStmt(conn3.ConnectionID, query))

	handler.ConnectionClosed(conn3)
	require.Equal(0, len(e.PreparedDataCache.GetSessionData(conn3.ConnectionID)))
}

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
				return sql.NewBaseSessionWithClientServer(addr, sql.Client{Capabilities: conn.Capabilities}, conn.ConnectionID), nil
			},
			sql.NoopTracer,
			dbFunc,
			e.MemoryManager,
			e.ProcessList,
			"foo",
		),
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)

	conn2 := newConn(2)
	handler.NewConnection(conn2)

	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 0)

	handler.ComInitDB(conn2, "test")
	err := handler.ComQuery(conn2, "KILL QUERY 1", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	require.False(conn1.Conn.(*mockConn).closed)
	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 1)

	err = handler.sm.SetDB(conn1, "test")
	require.NoError(err)
	ctx1, err := handler.sm.NewContextWithQuery(conn1, "SELECT 1")
	require.NoError(err)
	ctx1, err = handler.e.ProcessList.BeginQuery(ctx1, "SELECT 1")
	require.NoError(err)

	err = handler.ComQuery(conn2, "KILL "+fmt.Sprint(ctx1.ID()), func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	require.Error(ctx1.Err())
	require.True(conn1.Conn.(*mockConn).closed)
	handler.ConnectionClosed(conn1)
	require.Len(handler.sm.sessions, 1)
}

func TestSchemaToFields(t *testing.T) {
	require := require.New(t)

	schema := sql.Schema{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", Type: types.TinyBlob},
		{Name: "blob", Type: types.Blob},
		{Name: "mediumblob", Type: types.MediumBlob},
		{Name: "longblob", Type: types.LongBlob},
		{Name: "tinytext", Type: types.TinyText},
		{Name: "text", Type: types.Text},
		{Name: "mediumtext", Type: types.MediumText},
		{Name: "longtext", Type: types.LongText},
		{Name: "json", Type: types.JSON},

		// Geometry Types
		{Name: "geometry", Type: types.GeometryType{}},
		{Name: "point", Type: types.PointType{}},
		{Name: "polygon", Type: types.PolygonType{}},
		{Name: "linestring", Type: types.LineStringType{}},

		// Integer Types
		{Name: "uint8", Type: types.Uint8},
		{Name: "int8", Type: types.Int8},
		{Name: "uint16", Type: types.Uint16},
		{Name: "int16", Type: types.Int16},
		{Name: "uint24", Type: types.Uint24},
		{Name: "int24", Type: types.Int24},
		{Name: "uint32", Type: types.Uint32},
		{Name: "int32", Type: types.Int32},
		{Name: "uint64", Type: types.Uint64},
		{Name: "int64", Type: types.Int64},

		// Floating Point and Decimal Types
		{Name: "float32", Type: types.Float32},
		{Name: "float64", Type: types.Float64},
		{Name: "decimal10_0", Type: types.MustCreateDecimalType(10, 0)},
		{Name: "decimal60_30", Type: types.MustCreateDecimalType(60, 30)},

		// Char, Binary, and Bit Types
		{Name: "varchar50", Type: types.MustCreateString(sqltypes.VarChar, 50, sql.Collation_Default)},
		{Name: "varbinary12345", Type: types.MustCreateBinary(sqltypes.VarBinary, 12345)},
		{Name: "binary123", Type: types.MustCreateBinary(sqltypes.Binary, 123)},
		{Name: "char123", Type: types.MustCreateString(sqltypes.Char, 123, sql.Collation_Default)},
		{Name: "bit12", Type: types.MustCreateBitType(12)},

		// Dates
		{Name: "datetime", Type: types.MustCreateDatetimeType(sqltypes.Datetime, 0)},
		{Name: "timestamp", Type: types.MustCreateDatetimeType(sqltypes.Timestamp, 0)},
		{Name: "date", Type: types.MustCreateDatetimeType(sqltypes.Date, 0)},
		{Name: "time", Type: types.Time},
		{Name: "year", Type: types.Year},

		// Set and Enum Types
		{Name: "set", Type: types.MustCreateSetType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
		{Name: "enum", Type: types.MustCreateEnumType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
	}

	expected := []*query.Field{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 255},
		{Name: "blob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 65_535},
		{Name: "mediumblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 16_777_215},
		{Name: "longblob", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295},
		{Name: "tinytext", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 1020},
		{Name: "text", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 262_140},
		{Name: "mediumtext", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 67_108_860},
		{Name: "longtext", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},
		{Name: "json", Type: query.Type_JSON, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},

		// Geometry Types
		{Name: "geometry", Type: query.Type_GEOMETRY, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},
		{Name: "point", Type: query.Type_GEOMETRY, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},
		{Name: "polygon", Type: query.Type_GEOMETRY, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},
		{Name: "linestring", Type: query.Type_GEOMETRY, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295},

		// Integer Types
		{Name: "uint8", Type: query.Type_UINT8, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 3},
		{Name: "int8", Type: query.Type_INT8, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4},
		{Name: "uint16", Type: query.Type_UINT16, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 5},
		{Name: "int16", Type: query.Type_INT16, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 6},
		{Name: "uint24", Type: query.Type_UINT24, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 8},
		{Name: "int24", Type: query.Type_INT24, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 9},
		{Name: "uint32", Type: query.Type_UINT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 10},
		{Name: "int32", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11},
		{Name: "uint64", Type: query.Type_UINT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20},
		{Name: "int64", Type: query.Type_INT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20},

		// Floating Point and Decimal Types
		{Name: "float32", Type: query.Type_FLOAT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 12},
		{Name: "float64", Type: query.Type_FLOAT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 22},
		{Name: "decimal10_0", Type: query.Type_DECIMAL, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Decimals: 0},
		{Name: "decimal60_30", Type: query.Type_DECIMAL, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 62, Decimals: 30},

		// Char, Binary, and Bit Types
		{Name: "varchar50", Type: query.Type_VARCHAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 50 * 4},
		{Name: "varbinary12345", Type: query.Type_VARBINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 12345},
		{Name: "binary123", Type: query.Type_BINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 123},
		{Name: "char123", Type: query.Type_CHAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 123 * 4},
		{Name: "bit12", Type: query.Type_BIT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 12},

		// Dates
		{Name: "datetime", Type: query.Type_DATETIME, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 26},
		{Name: "timestamp", Type: query.Type_TIMESTAMP, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 26},
		{Name: "date", Type: query.Type_DATE, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 10},
		{Name: "time", Type: query.Type_TIME, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 17},
		{Name: "year", Type: query.Type_YEAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4},

		// Set and Enum Types
		{Name: "set", Type: query.Type_SET, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 72},
		{Name: "enum", Type: query.Type_ENUM, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20},
	}

	require.Equal(len(schema), len(expected))

	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn := newConn(1)
	handler.NewConnection(conn)

	ctx, err := handler.sm.NewContextWithQuery(conn, "SELECT 1")
	require.NoError(err)

	fields := schemaToFields(ctx, schema)
	for i := 0; i < len(fields); i++ {
		t.Run(schema[i].Name, func(t *testing.T) {
			assert.Equal(t, expected[i], fields[i])
		})
	}
}

// TestHandlerMaxTextResponseBytes tests that the handler calculates the correct max text response byte
// metadata for TEXT types, including honoring the character_set_results session variable. This is tested
// here, instead of in string type unit tests, because of the dependency on system variables being loaded.
func TestHandlerMaxTextResponseBytes(t *testing.T) {
	variables.InitSystemVariables()
	session := sql.NewBaseSession()
	ctx := sql.NewContext(
		context.Background(),
		sql.WithSession(session),
	)

	tinyTextUtf8mb4 := types.MustCreateString(sqltypes.Text, types.TinyTextBlobMax, sql.Collation_Default)
	textUtf8mb4 := types.MustCreateString(sqltypes.Text, types.TextBlobMax, sql.Collation_Default)
	mediumTextUtf8mb4 := types.MustCreateString(sqltypes.Text, types.MediumTextBlobMax, sql.Collation_Default)
	longTextUtf8mb4 := types.MustCreateString(sqltypes.Text, types.LongTextBlobMax, sql.Collation_Default)

	// When character_set_results is set to utf8mb4, the multibyte character multiplier is 4
	require.NoError(t, session.SetSessionVariable(ctx, "character_set_results", "utf8mb4"))
	require.EqualValues(t, types.TinyTextBlobMax*4, tinyTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.TextBlobMax*4, textUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.MediumTextBlobMax*4, mediumTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.LongTextBlobMax, longTextUtf8mb4.MaxTextResponseByteLength(ctx))

	// When character_set_results is set to utf8mb3, the multibyte character multiplier is 3
	require.NoError(t, session.SetSessionVariable(ctx, "character_set_results", "utf8mb3"))
	require.EqualValues(t, types.TinyTextBlobMax*3, tinyTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.TextBlobMax*3, textUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.MediumTextBlobMax*3, mediumTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.LongTextBlobMax, longTextUtf8mb4.MaxTextResponseByteLength(ctx))

	// When character_set_results is set to utf8, the multibyte character multiplier is 3
	require.NoError(t, session.SetSessionVariable(ctx, "character_set_results", "utf8"))
	require.EqualValues(t, types.TinyTextBlobMax*3, tinyTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.TextBlobMax*3, textUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.MediumTextBlobMax*3, mediumTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.LongTextBlobMax, longTextUtf8mb4.MaxTextResponseByteLength(ctx))

	// When character_set_results is set to NULL, the multibyte character multiplier is taken from
	// the type's charset (4 in this case)
	require.NoError(t, session.SetSessionVariable(ctx, "character_set_results", nil))
	require.EqualValues(t, types.TinyTextBlobMax*4, tinyTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.TextBlobMax*4, textUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.MediumTextBlobMax*4, mediumTextUtf8mb4.MaxTextResponseByteLength(ctx))
	require.EqualValues(t, types.LongTextBlobMax, longTextUtf8mb4.MaxTextResponseByteLength(ctx))
}

func TestHandlerTimeout(t *testing.T) {
	require := require.New(t)

	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	e2, pro2 := setupMemDB(require)
	dbFunc2 := pro2.Database

	timeOutHandler := &Handler{
		e: e,
		sm: NewSessionManager(testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo"),
		readTimeout: 1 * time.Second,
	}

	noTimeOutHandler := &Handler{
		e: e2,
		sm: NewSessionManager(testSessionBuilder(pro2),
			sql.NoopTracer,
			dbFunc2,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo"),
	}
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
	e, pro := setupMemDB(require)
	dbFunc := pro.Database

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

	h := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}
	c := newConn(1)
	h.NewConnection(c)

	q := fmt.Sprintf("SELECT SLEEP(%d)", (tcpCheckerSleepDuration * 4 / time.Second))
	h.ComInitDB(c, "test")
	err = h.ComQuery(c, q, func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)
}

// Tests the CLIENT_FOUND_ROWS capabilities flag
func TestHandlerFoundRowsCapabilities(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	dummyConn := newConn(1)

	// Set the capabilities to include found rows
	dummyConn.Capabilities = mysql.CapabilityClientFoundRows

	// Setup the handler
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}

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

func setupMemDB(require *require.Assertions) (*sqle.Engine, *memory.DbProvider) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	e := sqle.NewDefault(pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))

	tableTest := memory.NewTable(db, "test", sql.NewPrimaryKeySchema(sql.Schema{{Name: "c1", Type: types.Int32, Source: "test"}}), nil)
	tableTest.EnablePrimaryKeyIndexes()

	for i := 0; i < 1010; i++ {
		require.NoError(tableTest.Insert(
			ctx,
			sql.NewRow(int32(i)),
		))
	}

	db.AddTable("test", tableTest)

	return e, pro
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

		_, err = io.ReadAll(conn)
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
func testSessionBuilder(pro *memory.DbProvider) func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
		base := sql.NewBaseSessionWithClientServer(addr, sql.Client{Address: "127.0.0.1:34567", User: c.User, Capabilities: c.Capabilities}, c.ConnectionID)
		return memory.NewSession(base, pro), nil
	}
}

type mockConn struct {
	net.Conn
	closed bool
}

func (c *mockConn) Close() error {
	c.closed = true
	return nil
}

func (c *mockConn) RemoteAddr() net.Addr {
	return mockAddr{}
}

type mockAddr struct{}

func (mockAddr) Network() string {
	return "tcp"
}

func (mockAddr) String() string {
	return "localhost"
}

func newConn(id uint32) *mysql.Conn {
	return &mysql.Conn{
		ConnectionID: id,
		Conn:         new(mockConn),
	}
}
