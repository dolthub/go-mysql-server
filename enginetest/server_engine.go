// Copyright 2023 Dolthub, Inc.
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

package enginetest

import (
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type ServerQueryEngine struct {
	engine *sqle.Engine
	server *server.Server
	t      *testing.T
	port   int
	conn   *gosql.DB
}

var _ QueryEngine = (*ServerQueryEngine)(nil)

var address = "localhost"

func NewServerQueryEngine(t *testing.T, engine *sqle.Engine, builder server.SessionBuilder) (*ServerQueryEngine, error) {
	ctx := sql.NewEmptyContext()

	if err := enableUserAccounts(ctx, engine); err != nil {
		panic(err)
	}

	p, err := findEmptyPort()
	if err != nil {
		return nil, err
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, p),
	}
	s, err := server.NewServer(config, engine, builder, nil)
	if err != nil {
		return nil, err
	}

	go func() {
		_ = s.Start()
	}()

	return &ServerQueryEngine{
		t:      t,
		engine: engine,
		server: s,
		port:   p,
	}, nil
}

// NewConnection creates a new connection to the server regardless of whether there is an existing connection.
// If there is an existing connection, it closes it and creates a new connection. New connection uses new session
// that the previous session state data will not persist. This function is also called when there is no connection
// when running a query.
func (s *ServerQueryEngine) NewConnection(ctx *sql.Context) error {
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return err
		}
	}

	db := ctx.GetCurrentDatabase()
	// https://stackoverflow.com/questions/29341590/how-to-parse-time-from-database/29343013#29343013
	conn, err := gosql.Open("mysql", fmt.Sprintf("root:@tcp(127.0.0.1:%d)/%s?parseTime=true", s.port, db))
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

func (s *ServerQueryEngine) AnalyzeQuery(ctx *sql.Context, query string) (sql.Node, error) {
	return s.engine.AnalyzeQuery(ctx, query)
}

func (s *ServerQueryEngine) PrepareQuery(ctx *sql.Context, query string) (sql.Node, error) {
	if s.conn == nil {
		err := s.NewConnection(ctx)
		if err != nil {
			return nil, err
		}
	}
	// TODO
	// q, bindVars, err := injectBindVarsAndPrepare(s.t, ctx, s.engine, query)
	return nil, nil
}

func (s *ServerQueryEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	if s.conn == nil {
		err := s.NewConnection(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// we prepare each query as prepared statement if possible to add more coverage to prepared tests
	q, bindVars, err := injectBindVarsAndPrepare(s.t, ctx, s.engine, query)
	if err != nil {
		// TODO: ctx being used does not get updated when running the queries through go sql driver.
		//  we can try preparing and if it errors, then pass the original query
		// For example, `USE db` does not change the db in the ctx.
		return s.QueryWithBindings(ctx, query, nil, nil, nil)
	}
	if _, ok := cannotBePrepared[query]; ok {
		return s.QueryWithBindings(ctx, query, nil, nil, nil)
	}
	return s.QueryWithBindings(ctx, q, nil, bindVars, nil)
}

func (s *ServerQueryEngine) EngineAnalyzer() *analyzer.Analyzer {
	return s.engine.Analyzer
}

func (s *ServerQueryEngine) EngineEventScheduler() sql.EventScheduler {
	return s.engine.EventScheduler
}

func (s *ServerQueryEngine) EnginePreparedDataCache() *sqle.PreparedDataCache {
	return s.engine.PreparedDataCache
}

func (s *ServerQueryEngine) QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]sqlparser.Expr, qFlags *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	if s.conn == nil {
		err := s.NewConnection(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var err error
	if parsed == nil {
		parsed, err = sqlparser.Parse(query)
		if err != nil {
			// TODO: conn.Query() empty query does not error
			if strings.HasSuffix(err.Error(), "empty statement") {
				return nil, sql.RowsToRowIter(), nil, nil
			}
			// Note: we cannot access sql_mode when using ServerEngine
			//  to use ParseWithOptions() method. Replacing double quotes
			//  because the 'ANSI' mode is not on by default and will not
			//  be set on the context after SET @@sql_mode = 'ANSI' query.
			ansiQuery := strings.Replace(query, "\"", "`", -1)
			parsed, err = sqlparser.Parse(ansiQuery)
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	// NOTE: MySQL does not support LOAD DATA query as PREPARED STATEMENT.
	//  However, Dolt supports, but not go-sql-driver client
	switch parsed.(type) {
	case *sqlparser.Load, *sqlparser.Execute, *sqlparser.Prepare:
		return s.queryOrExec(nil, parsed, query, []any{})
	}

	stmt, err := s.conn.Prepare(query)
	if err != nil {
		return nil, nil, nil, trimMySQLErrCodePrefix(err)
	}

	args, err := prepareBindingArgs(ctx, bindings)
	if err != nil {
		return nil, nil, nil, err
	}

	return s.queryOrExec(stmt, parsed, query, args)
}

// queryOrExec function use `query()` or `exec()` method of go-sql-driver depending on the sql parser plan.
// If |stmt| is nil, then we use the connection db to query/exec the given query statement because some queries cannot
// be run as prepared.
// TODO: for `EXECUTE` and `CALL` statements, it can be either query or exec depending on the statement that prepared or stored procedure holds.
//
//	for now, we use `query` to get the row results for these statements. For statements that needs `exec`, there will be no result.
func (s *ServerQueryEngine) queryOrExec(stmt *gosql.Stmt, parsed sqlparser.Statement, query string, args []any) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	var err error
	switch parsed.(type) {
	// TODO: added `FLUSH` stmt here (should be `exec`) because we don't support `FLUSH BINARY LOGS` or `FLUSH ENGINE LOGS`, so nil schema is returned.
	case *sqlparser.Select, *sqlparser.SetOp, *sqlparser.Show, *sqlparser.Set, *sqlparser.Call, *sqlparser.Begin, *sqlparser.Use, *sqlparser.Load, *sqlparser.Execute, *sqlparser.Analyze, *sqlparser.Flush:
		var rows *gosql.Rows
		if stmt != nil {
			rows, err = stmt.Query(args...)
		} else {
			rows, err = s.conn.Query(query, args...)
		}
		if err != nil {
			return nil, nil, nil, trimMySQLErrCodePrefix(err)
		}
		return convertRowsResult(rows)
	default:
		var res gosql.Result
		if stmt != nil {
			res, err = stmt.Exec(args...)
		} else {
			res, err = s.conn.Exec(query, args...)
		}
		if err != nil {
			return nil, nil, nil, trimMySQLErrCodePrefix(err)
		}
		return convertExecResult(res)
	}
}

// trimMySQLErrCodePrefix temporarily removes the error code part of the error message returned from the server.
// This allows us to assert the error message strings in the enginetest.
func trimMySQLErrCodePrefix(err error) error {
	errMsg := err.Error()
	r := strings.Split(errMsg, "(HY000): ")
	if len(r) == 2 {
		return errors.New(r[1])
	}
	if e, ok := err.(*mysql.MySQLError); ok {
		// Note: the error msg can be fixed to match with MySQLError at https://github.com/dolthub/vitess/blob/main/go/mysql/sql_error.go#L62
		return errors.New(fmt.Sprintf("%s (errno %v) (sqlstate %s)", e.Message, e.Number, e.SQLState))
	}
	if strings.HasPrefix(errMsg, "sql: expected") && strings.Contains(errMsg, "arguments, got") {
		// TODO: needs better error message for non matching number of binding argument
		//  for Dolt, this error is caught on the first binding variable
		err = sql.ErrUnboundPreparedStatementVariable.New("v1")
	}
	return err
}

func convertExecResult(exec gosql.Result) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	affected, err := exec.RowsAffected()
	if err != nil {
		return nil, nil, nil, err
	}
	lastInsertId, err := exec.LastInsertId()
	if err != nil {
		return nil, nil, nil, err
	}

	okResult := types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(lastInsertId),
		Info:         nil,
	}

	return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(okResult)), nil, nil
}

func convertRowsResult(rows *gosql.Rows) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	sch, err := schemaForRows(rows)
	if err != nil {
		return nil, nil, nil, err
	}

	rowIter, err := rowIterForGoSqlRows(sch, rows)
	if err != nil {
		return nil, nil, nil, err
	}

	return sch, rowIter, nil, nil
}

func rowIterForGoSqlRows(sch sql.Schema, rows *gosql.Rows) (sql.RowIter, error) {
	result := make([]sql.Row, 0)
	r, err := emptyRowForSchema(sch)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(r...)
		if err != nil {
			return nil, err
		}

		row, err := derefRow(r)
		if err != nil {
			return nil, err
		}

		row = convertValue(sch, row)

		result = append(result, row)
	}

	return sql.RowsToRowIter(result...), nil
}

// convertValue converts the row value scanned from go sql driver client to type that we expect.
// This method helps with testing existing enginetests that expects specific type as returned value.
func convertValue(sch sql.Schema, row sql.Row) sql.Row {
	for i, col := range sch {
		switch col.Type.Type() {
		case query.Type_GEOMETRY:
			if row.GetValue(i) != nil {
				r, _, err := types.GeometryType{}.Convert(row.GetValue(i).([]byte))
				if err != nil {
					//t.Skip(fmt.Sprintf("received error converting returned geometry result"))
				} else {
					row.SetValue(i, r)
				}
			}
		case query.Type_JSON:
			if row.GetValue(i) != nil {
				// TODO: dolt returns the json result without escaped quotes and backslashes, which does not Unmarshall
				r, err := attemptUnmarshalJSON(string(row.GetValue(i).([]byte)))
				if err != nil {
					//t.Skip(fmt.Sprintf("received error unmarshalling returned json result"))
					row.SetValue(i, nil)
				} else {
					row.SetValue(i, r)
				}
			}
		case query.Type_TIME:
			if row.GetValue(i) != nil {
				r, _, err := types.TimespanType_{}.Convert(string(row.GetValue(i).([]byte)))
				if err != nil {
					//t.Skip(fmt.Sprintf("received error converting returned timespan result"))
				} else {
					row.SetValue(i, r)
				}
			}
		case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
			// TODO: check todo in 'emptyValuePointerForType' method
			//  we try to cast any value we got to uint64
			if row.GetValue(i) != nil {
				r, err := castToUint64(row.GetValue(i))
				if err != nil {
					//t.Skip(fmt.Sprintf("received error converting returned unsigned int result"))
				} else {
					row.SetValue(i, r)
				}
			}
		}
	}
	return row
}

// attemptUnmarshalJSON is returns error if the result cannot be unmarshalled
// instead of panicking from using `types.MustJSON()` method.
func attemptUnmarshalJSON(s string) (types.JSONDocument, error) {
	var doc interface{}
	if err := json.Unmarshal([]byte(s), &doc); err != nil {
		return types.JSONDocument{}, err
	}
	return types.JSONDocument{Val: doc}, nil
}

func castToUint64(v any) (uint64, error) {
	switch val := v.(type) {
	case int8:
		return uint64(val), nil
	case int16:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint64:
		return val, nil
	case []byte:
		u, err := strconv.ParseUint(string(val), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("expected uint64 number, but received: %s", string(val))
		}
		return u, nil
	default:
		return 0, fmt.Errorf("expected uint64 number, but received unexpected type: %T", v)
	}
}

func derefRow(r []any) (sql.Row, error) {
	row := make(sql.UntypedSqlRow, len(r))
	for i, v := range r {
		dv, err := deref(v)
		if err != nil {
			return nil, err
		}
		row.SetValue(i, dv)
	}
	return row, nil
}

func deref(val any) (any, error) {
	switch v := val.(type) {
	case *int8:
		return *v, nil
	case *int16:
		return *v, nil
	case *int32:
		return *v, nil
	case *int64:
		return *v, nil
	case *uint8:
		return *v, nil
	case *uint16:
		return *v, nil
	case *uint32:
		return *v, nil
	case *uint64:
		return *v, nil
	case *gosql.NullInt32:
		if v.Valid {
			return v.Int32, nil
		}
		return nil, nil
	case *gosql.NullInt64:
		if v.Valid {
			return v.Int64, nil
		}
		return nil, nil
	case *float32:
		return *v, nil
	case *float64:
		return *v, nil
	case *gosql.NullFloat64:
		if v.Valid {
			return v.Float64, nil
		}
		return nil, nil
	case *string:
		return *v, nil
	case *gosql.NullString:
		if v.Valid {
			return v.String, nil
		}
		return nil, nil
	case *[]byte:
		if *v == nil {
			return nil, nil
		}
		return *v, nil
	case *bool:
		return *v, nil
	case *time.Time:
		return *v, nil
	case *gosql.NullTime:
		if v.Valid {
			return v.Time, nil
		}
		return nil, nil
	case *gosql.NullByte:
		if v.Valid {
			return v.Byte, nil
		}
		return nil, nil
	case *any:
		if *v == nil {
			return nil, nil
		}
		return *v, nil
	default:
		return nil, fmt.Errorf("unhandled type %T", v)
	}
}

func emptyRowForSchema(sch sql.Schema) ([]any, error) {
	result := make([]any, len(sch))
	for i, col := range sch {
		var err error
		result[i], err = emptyValuePointerForType(col.Type)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func emptyValuePointerForType(t sql.Type) (any, error) {
	switch t.Type() {
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT64,
		query.Type_BIT, query.Type_YEAR:
		var i gosql.NullInt64
		return &i, nil
	case query.Type_INT32:
		var i gosql.NullInt32
		return &i, nil
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		//var i uint64
		// TODO: currently there is no gosql.NullUint64 type, so null value for unsigned integer value cannot be scanned.
		//  this might be resolved in Go 1.22, that is not out yet, https://github.com/go-sql-driver/mysql/issues/1433
		var i any
		return &i, nil
	case query.Type_DATE, query.Type_DATETIME, query.Type_TIMESTAMP:
		var t gosql.NullTime
		return &t, nil
	case query.Type_TEXT, query.Type_VARCHAR, query.Type_CHAR, query.Type_BINARY, query.Type_VARBINARY,
		query.Type_ENUM, query.Type_SET, query.Type_DECIMAL:
		// We have DECIMAL type results in enginetests be checked in STRING format.
		var s gosql.NullString
		return &s, nil
	case query.Type_FLOAT32, query.Type_FLOAT64:
		var f gosql.NullFloat64
		return &f, nil
	case query.Type_JSON, query.Type_BLOB, query.Type_TIME, query.Type_GEOMETRY:
		var f []byte
		return &f, nil
	case query.Type_NULL_TYPE:
		var f gosql.NullByte
		return &f, nil
	default:
		return nil, fmt.Errorf("unsupported type %v", t.Type())
	}
}

func schemaForRows(rows *gosql.Rows) (sql.Schema, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	schema := make(sql.Schema, len(types))
	for i, columnType := range types {
		typ, err := convertGoSqlType(columnType)
		if err != nil {
			return nil, err
		}
		schema[i] = &sql.Column{
			Name: names[i],
			Type: typ,
		}
	}

	return schema, nil
}

func convertGoSqlType(columnType *gosql.ColumnType) (sql.Type, error) {
	switch strings.ToLower(columnType.DatabaseTypeName()) {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "bit":
		return types.Int64, nil
	case "unsigned tinyint", "unsigned smallint", "unsigned mediumint", "unsigned int", "unsigned bigint":
		return types.Uint64, nil
	case "float", "double":
		return types.Float64, nil
	case "decimal":
		precision, scale, ok := columnType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("could not get decimal size for column %s", columnType.Name())
		}
		decimalType, err := types.CreateDecimalType(uint8(precision), uint8(scale))
		if err != nil {
			return nil, err
		}
		return decimalType, nil
	case "date":
		return types.Date, nil
	case "datetime":
		precision, _, ok := columnType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("could not get precision size for column %s", columnType.Name())
		}
		dtType, err := types.CreateDatetimeType(sqltypes.Datetime, int(precision))
		if err != nil {
			return nil, err
		}
		return dtType, nil
	case "timestamp":
		return types.Timestamp, nil
	case "time":
		return types.Time, nil
	case "year":
		return types.Year, nil
	case "char", "varchar":
		length, _ := columnType.Length()
		if length == 0 {
			length = 255
		}
		return types.CreateString(query.Type_VARCHAR, length, sql.Collation_Default)
	case "tinytext", "text", "mediumtext", "longtext":
		return types.Text, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return types.Blob, nil
	case "json":
		return types.JSON, nil
	case "enum":
		return types.EnumType{}, nil
	case "set":
		return types.SetType{}, nil
	case "null":
		return types.Null, nil
	case "geometry":
		return types.GeometryType{}, nil
	default:
		return nil, fmt.Errorf("unhandled type %s", columnType.DatabaseTypeName())
	}
}

// prepareBindingArgs returns an array of the binding variable converted from given map.
// The binding variables need to be sorted in order of position in the query. The variable in binding map
// is in random order. The function expects binding variables starting with `:v1` and do not skip number.
// It cannot sort user-defined binding variables (e.g. :var, :foo)
func prepareBindingArgs(ctx *sql.Context, bindings map[string]sqlparser.Expr) ([]any, error) {
	// NOTE: using binder with nil catalog and parser since we're only using it to convert SQLVal.
	binder := planbuilder.New(ctx, nil, nil, nil)
	numBindVars := len(bindings)
	args := make([]any, numBindVars)
	for i := 0; i < numBindVars; i++ {
		k := fmt.Sprintf("v%d", i+1)
		sqlVal, ok := bindings[k].(*sqlparser.SQLVal)
		if !ok {
			return nil, fmt.Errorf("cannot get binding value")
		}
		v := binder.ConvertVal(sqlVal)
		lit, ok := v.(*expression.Literal)
		if !ok {
			return nil, fmt.Errorf("cannot get binding value")
		}
		args[i] = lit.Value()
	}
	return args, nil
}

func findEmptyPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return -1, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		return -1, err

	}
	return port, nil
}

func (s *ServerQueryEngine) CloseSession(connID uint32) {
	// TODO
}

func (s *ServerQueryEngine) Close() error {
	return s.server.Close()
}

// MySQLPersister is an example struct which handles the persistence of the data in the "mysql" database.
type MySQLPersister struct {
	Data []byte
}

var _ mysql_db.MySQLDbPersistence = (*MySQLPersister)(nil)

// Persist implements the interface mysql_db.MySQLDbPersistence. This function is simple, in that it simply stores
// the given data inside itself. A real application would persist to the file system.
func (m *MySQLPersister) Persist(ctx *sql.Context, data []byte) error {
	m.Data = data
	return nil
}

func enableUserAccounts(ctx *sql.Context, engine *sqle.Engine) error {
	mysqlDb := engine.Analyzer.Catalog.MySQLDb

	// The functions "AddRootAccount" and "LoadData" both automatically enable the "mysql" database, but this is just
	// to explicitly show how one can manually enable (or disable) the database.
	mysqlDb.SetEnabled(true)
	// The persister here simply stands-in for your provided persistence function. The database calls this whenever it
	// needs to save any changes to any of the "mysql" database's tables.
	persister := &MySQLPersister{}
	mysqlDb.SetPersister(persister)

	// AddRootAccount creates a password-less account named "root" that has all privileges. This is intended for use
	// with testing, and also to set up the initial user accounts. A real application may want to check that a
	// persisted file exists, and call "LoadData" if one does. If a file does not exist, it would call
	// "AddRootAccount".
	mysqlDb.AddRootAccount()

	return nil
}

// We skip preparing these queries using injectBindVarsAndPrepare() method. They fail because
// injectBindVarsAndPrepare() method causes the non-string sql values to become string values.
// Other queries simply cause incorrect type result, which is not checked for ServerEngine test for now.
// TODO: remove this map when we fix this issue.
var cannotBePrepared = map[string]bool{
	`select """""foo""""";`: true,
}
