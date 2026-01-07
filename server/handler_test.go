// Copyright 2020-2024 Dolthub, Inc.
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/race"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

var samplePrepareData = &mysql.PrepareData{
	StatementID: 42,
	ParamsCount: 1,
}

func TestHandlerOutput(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database

	dummyConn := newConn(1)
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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
			err := handler.ComQuery(context.Background(), test.conn, test.query, func(res *sqltypes.Result, more bool) error {
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
		err := handler.ComQuery(context.Background(), dummyConn, "select sum(1) from test", func(res *sqltypes.Result, more bool) error {
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
		err := handler.ComQuery(context.Background(), dummyConn, "select avg(1) from test", func(res *sqltypes.Result, more bool) error {
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
		err := handler.ComQuery(context.Background(), dummyConn, "select if(1, 123, 'def')", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Text, result.Rows[0][0].Type())
		require.Equal(t, []byte("123"), result.Rows[0][0].ToBytes())

		err = handler.ComQuery(context.Background(), dummyConn, "select if(0, 123, 456)", func(res *sqltypes.Result, more bool) error {
			result = res
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, sqltypes.Int16, result.Rows[0][0].Type())
		require.Equal(t, []byte("456"), result.Rows[0][0].ToBytes())
	})
}

func TestHandlerErrors(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database

	dummyConn := newConn(1)
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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

	setupCommands := []string{"CREATE TABLE `test_table` ( `id` INT NOT NULL PRIMARY KEY, `v` INT );"}

	tests := []struct {
		name              string
		query             string
		expectedErrorCode int
	}{
		{
			name:              "insert with nonexistent field name",
			query:             "INSERT INTO `test_table` (`id`, `v_`) VALUES (1, 2)",
			expectedErrorCode: mysql.ERBadFieldError,
		},
		{
			name:              "insert into nonexistent table",
			query:             "INSERT INTO `test`.`no_such_table` (`id`, `v`) VALUES (1, 2)",
			expectedErrorCode: mysql.ERNoSuchTable,
		},
		{
			name:              "insert into same column twice",
			query:             "INSERT INTO `test`.`test_table` (`id`, `id`, `v`) VALUES (1, 2, 3)",
			expectedErrorCode: mysql.ERFieldSpecifiedTwice,
		},
		{
			name:              "use database that doesn't exist'",
			query:             "USE does_not_exist_db;",
			expectedErrorCode: mysql.ERBadDb,
		},
	}

	handler.ComInitDB(dummyConn, "test")
	for _, setupCommand := range setupCommands {
		err := handler.ComQuery(context.Background(), dummyConn, setupCommand, func(res *sqltypes.Result, more bool) error {
			return nil
		})
		require.NoError(t, err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := handler.ComQuery(context.Background(), dummyConn, test.query, func(res *sqltypes.Result, more bool) error {
				return nil
			})
			require.NotNil(t, err)
			sqlErr, isSqlError := err.(*mysql.SQLError)
			require.True(t, isSqlError)
			require.Equal(t, test.expectedErrorCode, sqlErr.Number())
		})
	}
}

// TestHandlerComReset asserts that the Handler.ComResetConnection method correctly clears all session
// state (e.g. table locks, prepared statements, user variables, session variables), and keeps the current
// database selected.
func TestHandlerComResetConnection(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dummyConn := newConn(1)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}
	handler.NewConnection(dummyConn)
	handler.ComInitDB(dummyConn, "test")

	prepareData := &mysql.PrepareData{
		StatementID: 0,
		PrepareStmt: "select 42 + ? from dual",
		ParamsCount: 0,
		ParamsType:  nil,
		ColumnNames: nil,
		BindVars: map[string]*query.BindVariable{
			"v1": {Type: query.Type_INT8, Value: []byte("5")},
		},
	}

	// Create a prepared statement, a table lock, and a user var in the current session
	_, err := handler.ComPrepare(context.Background(), dummyConn, prepareData.PrepareStmt, prepareData)
	require.NoError(t, err)
	_, cached := e.PreparedDataCache.GetCachedStmt(dummyConn.ConnectionID, prepareData.PrepareStmt)
	require.True(t, cached)
	err = handler.ComQuery(context.Background(), dummyConn, "SET @userVar = 42;", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(t, err)

	// Reset the connection to clear all session state
	err = handler.ComResetConnection(dummyConn)
	require.NoError(t, err)

	// Assert that the session is clean – the selected database should not change, and all session state
	// such as user vars, session vars, prepared statements, table locks, and temporary tables should be cleared.
	err = handler.ComQuery(context.Background(), dummyConn, "SELECT database()", func(res *sqltypes.Result, more bool) error {
		require.Equal(t, "test", res.Rows[0][0].ToString())
		return nil
	})
	require.NoError(t, err)
	_, cached = e.PreparedDataCache.GetCachedStmt(dummyConn.ConnectionID, prepareData.PrepareStmt)
	require.False(t, cached)
	err = handler.ComQuery(context.Background(), dummyConn, "SELECT @userVar;", func(res *sqltypes.Result, more bool) error {
		require.True(t, res.Rows[0][0].IsNull())
		return nil
	})
	require.NoError(t, err)
}

func TestHandlerComPrepare(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dummyConn := newConn(1)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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
				{Name: "c1", OrgName: "c1", Table: "test", OrgTable: "test", Database: "test", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
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
			schema, err := handler.ComPrepare(context.Background(), dummyConn, test.statement, samplePrepareData)
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
			sql.NewContext,
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
				{Name: "c1", OrgName: "c1", Table: "test", OrgTable: "test", Database: "test", Type: query.Type_INT32,
					Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
			},
			expected: []sql.Row{
				{0}, {1}, {2}, {3}, {4},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(dummyConn, "test")
			schema, err := handler.ComPrepare(context.Background(), dummyConn, test.prepare.PrepareStmt, samplePrepareData)
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
			err = handler.ComStmtExecute(context.Background(), dummyConn, test.prepare, callback)
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
			sql.NewContext,
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
			name: "select statement returns nil schema bug",
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
				{Name: "c1", OrgName: "c1", Table: "test", OrgTable: "test", Database: "test", Type: query.Type_INT32,
					Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
			},
			expected: []sql.Row{
				{0}, {1}, {2}, {3}, {4},
			},
		},
		{
			name: "ifnull typing",
			prepare: &mysql.PrepareData{
				StatementID: 0,
				PrepareStmt: "select ifnull(not null, 1000) as a",
				ParamsCount: 0,
				ParamsType:  nil,
				ColumnNames: nil,
				BindVars:    nil,
			},
			schema: []*query.Field{
				{Name: "a", OrgName: "a", Table: "", OrgTable: "", Database: "", Type: query.Type_INT16,
					Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 6, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
			},
			expected: []sql.Row{
				{1000},
			},
		},
		{
			name: "ifnull typing negative",
			prepare: &mysql.PrepareData{
				StatementID: 0,
				PrepareStmt: "select ifnull(not null, -129) as a",
				ParamsCount: 0,
				ParamsType:  nil,
				ColumnNames: nil,
				BindVars:    nil,
			},
			schema: []*query.Field{
				{Name: "a", OrgName: "a", Table: "", OrgTable: "", Database: "", Type: query.Type_INT16,
					Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 6, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
			},
			expected: []sql.Row{
				{-129},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler.ComInitDB(dummyConn, "test")
			schema, err := handler.ComPrepare(context.Background(), dummyConn, test.prepare.PrepareStmt, samplePrepareData)
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
			err = handler.ComStmtExecute(context.Background(), dummyConn, test.prepare, callback)
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
			sql.NewContext,
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

	err := handler.sm.SetDB(context.Background(), conn1, "test")
	require.NoError(err)

	err = handler.ComQuery(context.Background(), conn1, "SELECT 1", cb)
	require.NoError(err)
	require.Equal(listener.Queries, 1)
	require.Equal(listener.Successes, 1)
	require.Equal(listener.Failures, 0)

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	require.Equal(listener.Connections, 2)
	require.Equal(listener.Disconnects, 0)

	handler.ComInitDB(conn2, "test")
	err = handler.ComQuery(context.Background(), conn2, "select 1", cb)
	require.NoError(err)
	require.Equal(listener.Queries, 2)
	require.Equal(listener.Successes, 2)
	require.Equal(listener.Failures, 0)

	err = handler.ComQuery(context.Background(), conn1, "select bad_col from bad_table with illegal syntax", cb)
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
	handler.NewConnection(conn3)
	query := "SELECT ?"
	_, err = handler.ComPrepare(context.Background(), conn3, query, samplePrepareData)
	require.NoError(err)
	require.Equal(1, len(e.PreparedDataCache.CachedStatementsForSession(conn3.ConnectionID)))
	require.NotNil(e.PreparedDataCache.GetCachedStmt(conn3.ConnectionID, query))

	handler.ConnectionClosed(conn3)
	require.Equal(0, len(e.PreparedDataCache.CachedStatementsForSession(conn3.ConnectionID)))
}

func TestHandlerKill(t *testing.T) {
	require := require.New(t)
	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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
	err := handler.ComQuery(context.Background(), conn2, "KILL QUERY 1", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	require.False(conn1.Conn.(*mockConn).closed)
	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 1)

	err = handler.sm.SetDB(context.Background(), conn1, "test")
	require.NoError(err)
	ctx1, err := handler.sm.NewContextWithQuery(context.Background(), conn1, "SELECT 1")
	require.NoError(err)
	ctx1, err = handler.e.ProcessList.BeginQuery(ctx1, "SELECT 1")
	require.NoError(err)

	err = handler.ComQuery(context.Background(), conn2, "KILL "+fmt.Sprint(ctx1.ID()), func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	require.Error(ctx1.Err())
	require.True(conn1.Conn.(*mockConn).closed)
	handler.ConnectionClosed(conn1)
	require.Len(handler.sm.sessions, 1)
}

func TestHandlerKillQuery(t *testing.T) {
	if race.Enabled {
		t.Skip("this test is inherently racey")
	}
	require := require.New(t)
	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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

	var err error
	conn1 := newConn(1)
	handler.NewConnection(conn1)

	conn2 := newConn(2)
	handler.NewConnection(conn2)

	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 0)

	handler.ComInitDB(conn1, "test")
	err = handler.sm.SetDB(context.Background(), conn1, "test")
	require.NoError(err)

	err = handler.sm.SetDB(context.Background(), conn2, "test")
	require.NoError(err)

	require.False(conn1.Conn.(*mockConn).closed)
	require.False(conn2.Conn.(*mockConn).closed)
	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	sleepQuery := "SELECT SLEEP(100000)"
	var sleepErr error
	go func() {
		defer wg.Done()
		// need a local |err| variable to avoid being overwritten
		sleepErr = handler.ComQuery(context.Background(), conn1, sleepQuery, func(res *sqltypes.Result, more bool) error {
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	var sleepQueryID string
	err = handler.ComQuery(context.Background(), conn2, "SHOW PROCESSLIST", func(res *sqltypes.Result, more bool) error {
		// 1,  ,  , test, Query, 0, ...    , SELECT SLEEP(1000)
		// 2,  ,  , test, Query, 0, running, SHOW PROCESSLIST
		require.Equal(2, len(res.Rows))
		hasSleepQuery := false
		fmt.Println(res.Rows[0][0], res.Rows[0][4], res.Rows[0][7])
		fmt.Println(res.Rows[1][0], res.Rows[1][4], res.Rows[1][7])
		for _, row := range res.Rows {
			if row[7].ToString() != sleepQuery {
				continue
			}
			hasSleepQuery = true
			// the values inside a callback are generally only valid for the
			// duration of the query, and need to be copied to avoid being
			// overwritten
			sleepQueryID = strings.Clone(row[0].ToString())
			require.Equal("Query", row[4].ToString())
		}
		require.True(hasSleepQuery)
		return nil
	})
	require.NoError(err)

	time.Sleep(100 * time.Millisecond)
	err = handler.ComQuery(context.Background(), conn2, "KILL QUERY "+sleepQueryID, func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)
	wg.Wait()
	require.Error(sleepErr)

	time.Sleep(100 * time.Millisecond)
	err = handler.ComQuery(context.Background(), conn2, "SHOW PROCESSLIST", func(res *sqltypes.Result, more bool) error {
		// 1,  ,  , test, Sleep, 0,        ,
		// 2,  ,  , test, Query, 0, running, SHOW PROCESSLIST
		require.Equal(2, len(res.Rows))
		hasSleepQueryID := false
		for _, row := range res.Rows {
			if row[0].ToString() != sleepQueryID {
				continue
			}
			hasSleepQueryID = true
			require.Equal("Sleep", row[4].ToString())
			require.Equal("", row[7].ToString())
		}
		require.True(hasSleepQueryID)
		return nil
	})
	require.NoError(err)

	require.False(conn1.Conn.(*mockConn).closed)
	require.False(conn2.Conn.(*mockConn).closed)
	require.Len(handler.sm.connections, 2)
	require.Len(handler.sm.sessions, 2)
}

func TestSchemaToFields(t *testing.T) {
	require := require.New(t)

	schema := sql.Schema{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", Source: "table1", DatabaseSource: "db1", Type: types.TinyBlob},
		{Name: "blob", Source: "table1", DatabaseSource: "db1", Type: types.Blob},
		{Name: "mediumblob", Source: "table1", DatabaseSource: "db1", Type: types.MediumBlob},
		{Name: "longblob", Source: "table1", DatabaseSource: "db1", Type: types.LongBlob},
		{Name: "tinytext", Source: "table1", DatabaseSource: "db1", Type: types.TinyText},
		{Name: "text", Source: "table1", DatabaseSource: "db1", Type: types.Text},
		{Name: "mediumtext", Source: "table1", DatabaseSource: "db1", Type: types.MediumText},
		{Name: "longtext", Source: "table1", DatabaseSource: "db1", Type: types.LongText},
		{Name: "json", Source: "table1", DatabaseSource: "db1", Type: types.JSON},

		// Geometry Types
		{Name: "geometry", Source: "table1", DatabaseSource: "db1", Type: types.GeometryType{}},
		{Name: "point", Source: "table1", DatabaseSource: "db1", Type: types.PointType{}},
		{Name: "polygon", Source: "table1", DatabaseSource: "db1", Type: types.PolygonType{}},
		{Name: "linestring", Source: "table1", DatabaseSource: "db1", Type: types.LineStringType{}},

		// Integer Types
		{Name: "uint8", Source: "table1", DatabaseSource: "db1", Type: types.Uint8},
		{Name: "int8", Source: "table1", DatabaseSource: "db1", Type: types.Int8},
		{Name: "uint16", Source: "table1", DatabaseSource: "db1", Type: types.Uint16},
		{Name: "int16", Source: "table1", DatabaseSource: "db1", Type: types.Int16},
		{Name: "uint24", Source: "table1", DatabaseSource: "db1", Type: types.Uint24},
		{Name: "int24", Source: "table1", DatabaseSource: "db1", Type: types.Int24},
		{Name: "uint32", Source: "table1", DatabaseSource: "db1", Type: types.Uint32},
		{Name: "int32", Source: "table1", DatabaseSource: "db1", Type: types.Int32},
		{Name: "uint64", Source: "table1", DatabaseSource: "db1", Type: types.Uint64},
		{Name: "int64", Source: "table1", DatabaseSource: "db1", Type: types.Int64},

		// Floating Point and Decimal Types
		{Name: "float32", Source: "table1", DatabaseSource: "db1", Type: types.Float32},
		{Name: "float64", Source: "table1", DatabaseSource: "db1", Type: types.Float64},
		{Name: "decimal10_0", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateDecimalType(10, 0)},
		{Name: "decimal60_30", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateDecimalType(60, 30)},

		// Char, Binary, and Bit Types
		{Name: "varchar50", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateString(sqltypes.VarChar, 50, sql.Collation_Default)},
		{Name: "varbinary12345", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateBinary(sqltypes.VarBinary, 12345)},
		{Name: "binary123", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateBinary(sqltypes.Binary, 123)},
		{Name: "char123", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateString(sqltypes.Char, 123, sql.Collation_Default)},
		{Name: "bit12", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateBitType(12)},

		// Dates
		{Name: "datetime", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateDatetimeType(sqltypes.Datetime, 0)},
		{Name: "timestamp", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateDatetimeType(sqltypes.Timestamp, 0)},
		{Name: "date", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateDatetimeType(sqltypes.Date, 0)},
		{Name: "time", Source: "table1", DatabaseSource: "db1", Type: types.Time},
		{Name: "year", Source: "table1", DatabaseSource: "db1", Type: types.Year},

		// Set and Enum Types
		{Name: "set", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateSetType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
		{Name: "enum", Source: "table1", DatabaseSource: "db1", Type: types.MustCreateEnumType([]string{"one", "two", "three", "four"}, sql.Collation_Default)},
	}

	expected := []*query.Field{
		// Blob, Text, and JSON Types
		{Name: "tinyblob", OrgName: "tinyblob", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 255, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "blob", OrgName: "blob", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 65_535, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "mediumblob", OrgName: "mediumblob", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 16_777_215, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "longblob", OrgName: "longblob", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BLOB, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "tinytext", OrgName: "tinytext", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 1020, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "text", OrgName: "text", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 262_140, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "mediumtext", OrgName: "mediumtext", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 67_108_860, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "longtext", OrgName: "longtext", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TEXT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "json", OrgName: "json", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_JSON, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Geometry Types
		{Name: "geometry", OrgName: "geometry", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "point", OrgName: "point", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "polygon", OrgName: "polygon", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "linestring", OrgName: "linestring", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_GEOMETRY, Charset: mysql.CharacterSetBinary, ColumnLength: 4_294_967_295, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Integer Types
		{Name: "uint8", OrgName: "uint8", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_UINT8, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 3, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG | query.MySqlFlag_UNSIGNED_FLAG)},
		{Name: "int8", OrgName: "int8", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_INT8, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "uint16", OrgName: "uint16", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_UINT16, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 5, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG | query.MySqlFlag_UNSIGNED_FLAG)},
		{Name: "int16", OrgName: "int16", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_INT16, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 6, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "uint24", OrgName: "uint24", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_UINT24, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 8, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG | query.MySqlFlag_UNSIGNED_FLAG)},
		{Name: "int24", OrgName: "int24", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_INT24, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 9, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "uint32", OrgName: "uint32", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_UINT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 10, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG | query.MySqlFlag_UNSIGNED_FLAG)},
		{Name: "int32", OrgName: "int32", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_INT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "uint64", OrgName: "uint64", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_UINT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG | query.MySqlFlag_UNSIGNED_FLAG)},
		{Name: "int64", OrgName: "int64", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_INT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Floating Point and Decimal Types
		{Name: "float32", OrgName: "float32", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_FLOAT32, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 12, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "float64", OrgName: "float64", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_FLOAT64, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 22, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "decimal10_0", OrgName: "decimal10_0", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_DECIMAL, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 11, Decimals: 0, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "decimal60_30", OrgName: "decimal60_30", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_DECIMAL, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 62, Decimals: 30, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Char, Binary, and Bit Types
		{Name: "varchar50", OrgName: "varchar50", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_VARCHAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 50 * 4, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "varbinary12345", OrgName: "varbinary12345", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_VARBINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 12345, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "binary123", OrgName: "binary123", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BINARY, Charset: mysql.CharacterSetBinary, ColumnLength: 123, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "char123", OrgName: "char123", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_CHAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 123 * 4, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "bit12", OrgName: "bit12", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_BIT, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 12, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Dates
		{Name: "datetime", OrgName: "datetime", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_DATETIME, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 26, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "timestamp", OrgName: "timestamp", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TIMESTAMP, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 26, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "date", OrgName: "date", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_DATE, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 10, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "time", OrgName: "time", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_TIME, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 17, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "year", OrgName: "year", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_YEAR, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 4, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},

		// Set and Enum Types
		{Name: "set", OrgName: "set", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_SET, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 72, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "enum", OrgName: "enum", Table: "table1", OrgTable: "table1", Database: "db1", Type: query.Type_ENUM, Charset: uint32(sql.CharacterSet_utf8mb4), ColumnLength: 20, Flags: uint32(query.MySqlFlag_NOT_NULL_FLAG)},
	}

	require.Equal(len(schema), len(expected))

	e, pro := setupMemDB(require)
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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

	ctx, err := handler.sm.NewContextWithQuery(context.Background(), conn, "SELECT 1")
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
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo"),
		readTimeout: 1 * time.Second,
	}

	noTimeOutHandler := &Handler{
		e: e2,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro2),
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
	err := timeOutHandler.ComQuery(context.Background(), connTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.EqualError(err, "row read wait bigger than connection timeout (errno 1105) (sqlstate HY000)")

	err = timeOutHandler.ComQuery(context.Background(), connTimeout, "SELECT SLEEP(0.5)", func(res *sqltypes.Result, more bool) error {
		return nil
	})
	require.NoError(err)

	noTimeOutHandler.ComInitDB(connNoTimeout, "test")
	err = noTimeOutHandler.ComQuery(context.Background(), connNoTimeout, "SELECT SLEEP(2)", func(res *sqltypes.Result, more bool) error {
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
			sql.NewContext,
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
	err = h.ComQuery(context.Background(), c, q, func(res *sqltypes.Result, more bool) error {
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
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
	}

	handler.NewConnection(dummyConn)

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
			err := handler.ComQuery(context.Background(), test.conn, test.query, func(res *sqltypes.Result, more bool) error {
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

func dummyCb(_ *sqltypes.Result, _ bool) error {
	return nil
}

const waitTimeout = 500 * time.Millisecond

func checkGlobalStatVar(t *testing.T, name string, expected any) {
	start := time.Now()
	var globalVal interface{}
	var ok bool
	for time.Now().Sub(start) < waitTimeout {
		_, globalVal, ok = sql.StatusVariables.GetGlobal(name)
		require.True(t, ok)
		if globalVal == expected {
			return
		}
	}
	require.Fail(t, fmt.Sprintf("expected global status variable %s to be %d, got %d", name, expected, globalVal))
}

func checkSessionStatVar(t *testing.T, sess sql.Session, name string, expected uint64) {
	start := time.Now()
	var sessVal interface{}
	var err error
	for time.Now().Sub(start) < waitTimeout {
		sessVal, err = sess.GetStatusVariable(nil, name)
		require.NoError(t, err)
		if sessVal == expected {
			return
		}
	}
	require.Fail(t, fmt.Sprintf("expected session status variable %s to be %d, got %d", name, expected, sessVal))
}

func TestStatusVariableQuestions(t *testing.T) {
	t.Skipf("seems to flake quite a bit")
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)
	sess1 := handler.sm.sessions[1]

	checkGlobalStatVar(t, "Questions", uint64(0))
	checkSessionStatVar(t, sess1, "Questions", uint64(0))

	// Call ComQuery 5 times
	for i := 0; i < 5; i++ {
		err = handler.ComQuery(context.Background(), conn1, "SELECT 1", dummyCb)
		require.NoError(t, err)
	}

	checkGlobalStatVar(t, "Questions", uint64(5))
	checkSessionStatVar(t, sess1, "Questions", uint64(5))

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)
	sess2 := handler.sm.sessions[2]

	// Get 5 syntax errors
	for i := 0; i < 5; i++ {
		err = handler.ComQuery(context.Background(), conn2, "syntax error", dummyCb)
		require.Error(t, err)
	}

	checkGlobalStatVar(t, "Questions", uint64(10))
	checkSessionStatVar(t, sess1, "Questions", uint64(5))
	checkSessionStatVar(t, sess2, "Questions", uint64(5))

	conn3 := newConn(3)
	handler.NewConnection(conn3)
	err = handler.ComInitDB(conn3, "test")
	require.NoError(t, err)
	sess3 := handler.sm.sessions[3]

	err = handler.ComQuery(context.Background(), conn3, "create procedure p() begin select 1; select 2; select 3; end", dummyCb)
	require.NoError(t, err)

	checkGlobalStatVar(t, "Questions", uint64(11))
	checkSessionStatVar(t, sess3, "Questions", uint64(1))

	// Calling stored procedure with multiple queries only increment Questions once.
	err = handler.ComQuery(context.Background(), conn3, "call p()", dummyCb)
	require.NoError(t, err)

	checkGlobalStatVar(t, "Questions", uint64(12))
	checkSessionStatVar(t, sess1, "Questions", uint64(5))
	checkSessionStatVar(t, sess2, "Questions", uint64(5))
	checkSessionStatVar(t, sess3, "Questions", uint64(2))

	conn4 := newConn(4)
	handler.NewConnection(conn4)
	err = handler.ComInitDB(conn4, "test")
	require.NoError(t, err)
	sess4 := handler.sm.sessions[4]

	// TODO: implement and test that ComPing does not increment Questions
	// TODO: implement and test that ComStatistics does not increment Questions
	// TODO: implement and test that ComStmtClose does not increment Questions
	// TODO: implement and test that ComStmtReset does not increment Questions

	// Prepare does not increment Questions
	prepare := &mysql.PrepareData{
		StatementID: 0,
		PrepareStmt: "select ?",
		ParamsCount: 0,
		ParamsType:  nil,
		ColumnNames: nil,
		BindVars: map[string]*query.BindVariable{
			"v1": {Type: query.Type_INT8, Value: []byte("5")},
		},
	}

	_, err = handler.ComPrepare(context.Background(), conn4, prepare.PrepareStmt, samplePrepareData)
	require.NoError(t, err)

	checkGlobalStatVar(t, "Questions", uint64(12))
	checkSessionStatVar(t, sess4, "Questions", uint64(0))

	// Execute does increment Questions
	err = handler.ComStmtExecute(context.Background(), conn4, prepare, func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	checkGlobalStatVar(t, "Questions", uint64(13))
	checkSessionStatVar(t, sess4, "Questions", uint64(1))
}

func TestStatusVariableAbortedConnects(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	checkGlobalStatVar(t, "Aborted_connects", uint64(0))
	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ConnectionAborted(conn1, "test")
	require.NoError(t, err)
	checkGlobalStatVar(t, "Aborted_connects", uint64(1))
}

func TestStatusVariableMaxUsedConnections(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	checkGlobalStatVar(t, "Max_used_connections", uint64(0))
	checkGlobalStatVar(t, "Max_used_connections_time", uint64(0))

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)

	checkGlobalStatVar(t, "Max_used_connections", uint64(1))

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)

	checkGlobalStatVar(t, "Max_used_connections", uint64(2))

	conn3 := newConn(3)
	handler.NewConnection(conn3)
	err = handler.ComInitDB(conn3, "test")
	require.NoError(t, err)

	checkGlobalStatVar(t, "Max_used_connections", uint64(3))

	conn3.Close()
	checkGlobalStatVar(t, "Max_used_connections", uint64(3))
	conn2.Close()
	checkGlobalStatVar(t, "Max_used_connections", uint64(3))
	conn1.Close()
	checkGlobalStatVar(t, "Max_used_connections", uint64(3))
}

func TestStatusVariableThreadsConnected(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	checkGlobalStatVar(t, "Threads_connected", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(0))

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)

	checkGlobalStatVar(t, "Threads_connected", uint64(1))
	checkGlobalStatVar(t, "Connections", uint64(1))

	handler.sm.RemoveConn(conn1)

	checkGlobalStatVar(t, "Threads_connected", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(1))

	conns := make([]*mysql.Conn, 10)
	for i := 0; i < 10; i++ {
		conns[i] = newConn(uint32(i))
		handler.NewConnection(conns[i])
		err = handler.ComInitDB(conns[i], "test")
		require.NoError(t, err)
	}

	checkGlobalStatVar(t, "Threads_connected", uint64(10))
	checkGlobalStatVar(t, "Connections", uint64(11))

	for i := 0; i < 10; i++ {
		handler.sm.RemoveConn(conns[i])
		checkGlobalStatVar(t, "Threads_connected", uint64(10-i-1))
	}

	checkGlobalStatVar(t, "Threads_connected", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(11))
}

func TestStatusVariableThreadsRunning(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	checkGlobalStatVar(t, "Threads_running", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(0))

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ComQuery(context.Background(), conn1, "select sleep(1)", dummyCb)
	}()

	checkGlobalStatVar(t, "Threads_running", uint64(1))
	checkGlobalStatVar(t, "Connections", uint64(2))

	wg.Wait()
	checkGlobalStatVar(t, "Threads_running", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(2))

	wg.Add(2)
	go func() {
		defer wg.Done()
		handler.ComQuery(context.Background(), conn1, "select sleep(1)", dummyCb)
	}()
	go func() {
		defer wg.Done()
		handler.ComQuery(context.Background(), conn2, "select sleep(1)", dummyCb)
	}()

	checkGlobalStatVar(t, "Threads_running", uint64(2))
	checkGlobalStatVar(t, "Connections", uint64(2))

	wg.Wait()
	checkGlobalStatVar(t, "Threads_running", uint64(0))
	checkGlobalStatVar(t, "Connections", uint64(2))
}

func TestStatusVariableComSelect(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)
	sess1 := handler.sm.sessions[1]

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)
	sess2 := handler.sm.sessions[2]

	checkGlobalStatVar(t, "Com_select", uint64(0))
	checkSessionStatVar(t, sess1, "Com_select", uint64(0))
	checkSessionStatVar(t, sess2, "Com_select", uint64(0))

	// have session 1 call delete 5 times
	for i := 0; i < 5; i++ {
		handler.ComQuery(context.Background(), conn1, "select 1 from dual", dummyCb)
	}

	// have session 2 call delete 3 times
	for i := 0; i < 3; i++ {
		handler.ComQuery(context.Background(), conn2, "select 1 from dual", dummyCb)
	}

	checkGlobalStatVar(t, "Com_select", uint64(8))
	checkSessionStatVar(t, sess1, "Com_select", uint64(5))
	checkSessionStatVar(t, sess2, "Com_select", uint64(3))
}

func TestStatusVariableComDelete(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)
	sess1 := handler.sm.sessions[1]

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)
	sess2 := handler.sm.sessions[2]

	checkGlobalStatVar(t, "Com_delete", uint64(0))
	checkSessionStatVar(t, sess1, "Com_delete", uint64(0))
	checkSessionStatVar(t, sess2, "Com_delete", uint64(0))

	// have session 1 call delete 5 times
	for i := 0; i < 5; i++ {
		handler.ComQuery(context.Background(), conn1, "DELETE FROM doesnotmatter", dummyCb)
	}

	// have session 2 call delete 3 times
	for i := 0; i < 3; i++ {
		handler.ComQuery(context.Background(), conn2, "DELETE FROM doesnotmatter", dummyCb)
	}

	checkGlobalStatVar(t, "Com_delete", uint64(8))
	checkSessionStatVar(t, sess1, "Com_delete", uint64(5))
	checkSessionStatVar(t, sess2, "Com_delete", uint64(3))
}

func TestStatusVariableComInsert(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)
	sess1 := handler.sm.sessions[1]

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)
	sess2 := handler.sm.sessions[2]

	checkGlobalStatVar(t, "Com_insert", uint64(0))
	checkSessionStatVar(t, sess1, "Com_insert", uint64(0))
	checkSessionStatVar(t, sess2, "Com_insert", uint64(0))

	// have session 1 call delete 5 times
	for i := 0; i < 5; i++ {
		handler.ComQuery(context.Background(), conn1, "insert into blahblah values ()", dummyCb)
	}

	// have session 2 call delete 3 times
	for i := 0; i < 3; i++ {
		handler.ComQuery(context.Background(), conn2, "insert into blahblah values ()", dummyCb)
	}

	checkGlobalStatVar(t, "Com_insert", uint64(8))
	checkSessionStatVar(t, sess1, "Com_insert", uint64(5))
	checkSessionStatVar(t, sess2, "Com_insert", uint64(3))
}

func TestStatusVariableComUpdate(t *testing.T) {
	variables.InitStatusVariables()

	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database
	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	conn1 := newConn(1)
	handler.NewConnection(conn1)
	err := handler.ComInitDB(conn1, "test")
	require.NoError(t, err)
	sess1 := handler.sm.sessions[1]

	conn2 := newConn(2)
	handler.NewConnection(conn2)
	err = handler.ComInitDB(conn2, "test")
	require.NoError(t, err)
	sess2 := handler.sm.sessions[2]

	checkGlobalStatVar(t, "Com_update", uint64(0))
	checkSessionStatVar(t, sess1, "Com_update", uint64(0))
	checkSessionStatVar(t, sess2, "Com_update", uint64(0))

	// have session 1 call delete 5 times
	for i := 0; i < 5; i++ {
		handler.ComQuery(context.Background(), conn1, "update t set i = 10", dummyCb)
	}

	// have session 2 call delete 3 times
	for i := 0; i < 3; i++ {
		handler.ComQuery(context.Background(), conn2, "update t set i = 10", dummyCb)
	}

	checkGlobalStatVar(t, "Com_update", uint64(8))
	checkSessionStatVar(t, sess1, "Com_update", uint64(5))
	checkSessionStatVar(t, sess2, "Com_update", uint64(3))
}

// TestLoggerFieldsSetup tests that handler properly sets up logger fields including query time
func TestLoggerFieldsSetup(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
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
	err := handler.ComInitDB(conn, "test")
	require.NoError(t, err)

	// Execute a query and verify basic logging setup
	err = handler.ComQuery(context.Background(), conn, "SELECT 1", dummyCb)
	require.NoError(t, err)

	// Verify that the session's logger has the expected fields
	session := handler.sm.session(conn)
	logger := session.GetLogger()
	require.NotNil(t, logger, "Session should have a logger")

	// Verify that the logger has the expected fields
	require.Contains(t, logger.Data, sql.ConnectTimeLogKey, "Logger should contain connect time")
	require.Contains(t, logger.Data, sql.ConnectionIdLogField, "Logger should contain connection ID")

	// Verify that queryTime is actually used in logs by capturing a log entry
	var capturedFields logrus.Fields
	hook := &testHook{fields: &capturedFields}
	logrus.AddHook(hook)
	defer logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))

	// Execute a query that will trigger error logging (which includes queryTime)
	err = handler.ComQuery(context.Background(), conn, "SELECT * FROM nonexistent_table", dummyCb)
	require.Error(t, err) // This should cause an error log with queryTime

	// Verify that the log entry contained queryTime
	require.Contains(t, capturedFields, sql.QueryTimeLogKey, "Log entry should contain queryTime field")

	// Verify the values are of correct types
	connectTime, ok := logger.Data[sql.ConnectTimeLogKey].(time.Time)
	require.True(t, ok, "Connect time should be a time.Time")
	require.False(t, connectTime.IsZero(), "Connect time should not be zero")

	connID, ok := logger.Data[sql.ConnectionIdLogField].(uint32)
	require.True(t, ok, "Connection ID should be a uint32")
	require.Equal(t, conn.ConnectionID, connID, "Connection ID should match")
}

// Simple hook to capture log fields for testing
type testHook struct {
	fields *logrus.Fields
}

func (h *testHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.WarnLevel} // Only capture warning level (error logs)
}

func (h *testHook) Fire(entry *logrus.Entry) error {
	if entry.Message == "error running query" {
		*h.fields = entry.Data
	}
	return nil
}

func TestHandlerNewConnectionProcessListInteractions(t *testing.T) {
	e, pro := setupMemDB(require.New(t))
	dbFunc := pro.Database

	handler := &Handler{
		e: e,
		sm: NewSessionManager(
			sql.NewContext,
			testSessionBuilder(pro),
			sql.NoopTracer,
			dbFunc,
			sql.NewMemoryManager(nil),
			sqle.NewProcessList(),
			"foo",
		),
		readTimeout: time.Second,
	}

	// Process List starts empty.
	procs := handler.sm.processlist.Processes()
	assert.Len(t, procs, 0)

	// A new connection is in Connect state and shows "unauthenticated user" as the user.
	abortedConn := newConn(1)
	handler.NewConnection(abortedConn)
	procs = handler.sm.processlist.Processes()
	if assert.Len(t, procs, 1) {
		assert.Equal(t, "unauthenticated user", procs[0].User)
		assert.Equal(t, sql.ProcessCommandConnect, procs[0].Command)
	}

	// The connection being aborted does not effect the process list.
	handler.ConnectionAborted(abortedConn, "")
	procs = handler.sm.processlist.Processes()
	assert.Len(t, procs, 1)

	// After the ConnectionAborted called, the ConnectionClosed callback does
	// remove the connection from the processlist.
	handler.ConnectionClosed(abortedConn)
	procs = handler.sm.processlist.Processes()
	assert.Len(t, procs, 0)

	// A new connection gets updated with the authenticated user
	// and command Sleep when ConnectionAuthenticated is called.
	authenticatedConn := newConn(2)
	handler.NewConnection(authenticatedConn)
	authenticatedConn.User = "authenticated_user"
	handler.ConnectionAuthenticated(authenticatedConn)
	procs = handler.sm.processlist.Processes()
	if assert.Len(t, procs, 1) {
		assert.Equal(t, "authenticated_user", procs[0].User)
		assert.Equal(t, sql.ProcessCommandSleep, procs[0].Command)
		assert.Equal(t, "", procs[0].Database)
	}

	// After ComInitDB, the selected database is also reflected.
	handler.ComInitDB(authenticatedConn, "test")
	procs = handler.sm.processlist.Processes()
	if assert.Len(t, procs, 1) {
		assert.Equal(t, "test", procs[0].Database)
	}
}
