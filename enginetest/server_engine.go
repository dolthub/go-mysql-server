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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	_ "github.com/go-sql-driver/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type ServerQueryEngine struct {
	engine *sqle.Engine
	server *server.Server
	t      *testing.T
}

var _ QueryEngine = (*ServerQueryEngine)(nil)

var address = "localhost"

// TODO: get random port
var port = 3306

func NewServerQueryEngine(t *testing.T, engine *sqle.Engine, builder server.SessionBuilder) (*ServerQueryEngine, error) {
	ctx := sql.NewEmptyContext()

	if err := enableUserAccounts(ctx, engine); err != nil {
		panic(err)
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
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
	}, nil
}

func newConnection(ctx *sql.Context) (*gosql.DB, error) {
	db := ctx.GetCurrentDatabase()
	// https://stackoverflow.com/questions/29341590/how-to-parse-time-from-database/29343013#29343013
	return gosql.Open("mysql", fmt.Sprintf("root:@tcp(127.0.0.1)/%s?parseTime=true", db))
}

func (s ServerQueryEngine) PrepareQuery(ctx *sql.Context, query string) (sql.Node, error) {
	// TODO
	// q, bindVars, err := injectBindVarsAndPrepare(s.t, ctx, s.engine, query)
	return nil, nil
}

func (s ServerQueryEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	q, bindVars, err := injectBindVarsAndPrepare(s.t, ctx, s.engine, query)
	if err != nil {
		return nil, nil, err
	}

	return s.QueryWithBindings(ctx, q, nil, bindVars)
}

func (s ServerQueryEngine) EngineAnalyzer() *analyzer.Analyzer {
	return s.engine.Analyzer
}

func (s ServerQueryEngine) EnginePreparedDataCache() *sqle.PreparedDataCache {
	return s.engine.PreparedDataCache
}

func (s ServerQueryEngine) QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]*query.BindVariable) (sql.Schema, sql.RowIter, error) {
	conn, err := newConnection(ctx)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, nil, err
	}

	args := prepareBindingArgs(bindings)

	if parsed == nil {
		parsed, err = sqlparser.Parse(query)
		if err != nil {
			return nil, nil, err
		}
	}

	switch parsed.(type) {
	case *sqlparser.Select, *sqlparser.SetOp, *sqlparser.Show, *sqlparser.Set:
		rows, err := stmt.Query(args...)
		if err != nil {
			return nil, nil, err
		}
		return convertRowsResult(rows)
	default:
		exec, err := stmt.Exec(args...)
		if err != nil {
			return nil, nil, err
		}
		return convertExecResult(exec)
	}
}

func convertExecResult(exec gosql.Result) (sql.Schema, sql.RowIter, error) {
	affected, err := exec.RowsAffected()
	if err != nil {
		return nil, nil, err
	}
	lastInsertId, err := exec.LastInsertId()
	if err != nil {
		return nil, nil, err
	}

	okResult := types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(lastInsertId),
		Info:         nil,
	}

	return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(okResult)), nil
}

func convertRowsResult(rows *gosql.Rows) (sql.Schema, sql.RowIter, error) {
	sch, err := schemaForRows(rows)
	if err != nil {
		return nil, nil, err
	}

	rowIter, err := rowIterForGoSqlRows(sch, rows)
	if err != nil {
		return nil, nil, err
	}

	return sch, rowIter, nil
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
			if row[i] != nil {
				r, _, err := types.GeometryType{}.Convert(row[i].([]byte))
				if err == nil {
					row[i] = r
				}
			}
		case query.Type_JSON:
			if row[i] != nil {
				row[i] = types.MustJSON(string(row[i].([]byte)))
			}
		case query.Type_TIME:
			if row[i] != nil {
				r, _, err := types.TimespanType_{}.Convert(string(row[i].([]byte)))
				if err == nil {
					row[i] = r
				}
			}
		case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
			// TODO: check todo in 'emptyValuePointerForType' method
			//  we try to cast any value we got to uint64
			if row[i] != nil {
				r, err := toUint64(row[i])
				if err == nil {
					row[i] = r
				}
			}
		}
	}
	return row
}

func toUint64(v any) (uint64, error) {
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
	row := make(sql.Row, len(r))
	for i, v := range r {
		var err error
		row[i], err = deref(v)
		if err != nil {
			return nil, err
		}
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
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
		query.Type_BIT, query.Type_YEAR:
		var i gosql.NullInt64
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
	case "tinyint", "smallint", "mediumint", "int", "bigint":
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
		return types.Datetime, nil
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
func prepareBindingArgs(bindings map[string]*query.BindVariable) []any {
	names := make([]string, len(bindings))
	var i int
	for name := range bindings {
		names[i] = name
		i++
	}

	// the binding values need in specific order
	// it is in random order as stored in a map
	sort.Strings(names)

	args := make([]any, len(bindings))
	for j, name := range names {
		args[j] = bindings[name].Value
	}

	return args
}

func (s ServerQueryEngine) CloseSession(connID uint32) {
	// TODO
}

func (s ServerQueryEngine) Close() error {
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
