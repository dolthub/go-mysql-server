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
	"strings"
	"testing"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	_ "github.com/go-sql-driver/mysql"
)

type ServerQueryEngine struct {
	engine 	*sqle.Engine
	server *server.Server
	t 		*testing.T
}

var _ QueryEngine = (*ServerQueryEngine)(nil)

var address   = "localhost"
// TODO: get random port
var port      = 3306

func NewServerQueryEngine(t *testing.T) (*ServerQueryEngine, error) {
	ctx := sql.NewEmptyContext()
	engine := sqle.NewDefault(memory.NewDBProvider())

	// This variable may be found in the "users_example.go" file. Please refer to that file for a walkthrough on how to
	// set up the "mysql" database to allow user creation and user checking when establishing connections. This is set
	// to false for this example, but feel free to play around with it and see how it works.
	if err := enableUserAccounts(ctx, engine); err != nil {
		panic(err)
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		return nil, err
	}

	err = s.Start()
	if err != nil {
		return nil, err
	}
	
	return &ServerQueryEngine{
		t: t,
		engine: engine,
		server: s,
	}, nil
}

func newConnection() (*gosql.DB, error) {
	return gosql.Open("mysql", "root:@tcp(127.0.0.1)")
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

	return s.QueryWithBindings(ctx, q, bindVars)
}

func (s ServerQueryEngine) EngineAnalyzer() *analyzer.Analyzer {
	return s.engine.Analyzer
}

func (s ServerQueryEngine) EnginePreparedDataCache() *sqle.PreparedDataCache {
	return s.engine.PreparedDataCache
}

func (s ServerQueryEngine) QueryWithBindings(ctx *sql.Context, query string, bindings map[string]*query.BindVariable) (sql.Schema, sql.RowIter, error) {
	conn, err := newConnection()
	if err != nil {
		return nil, nil, err
	}
	
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	
	bindingArgs := bindingArgs(bindings)

	parsed, err := sqlparser.Parse(query)
	if err != nil {
		return nil, nil, err
	}
	
	switch parsed.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		rows, err := stmt.Query(bindingArgs)
		if err != nil {
			return nil, nil, err
		}
		return convertRowsResult(rows)
	default:
		exec, err := stmt.Exec(bindingArgs)
		if err != nil {
			return nil, nil, err
		}
		return convertExecResult(exec)
	}
}

func convertExecResult(exec gosql.Result) (sql.Schema, sql.RowIter, error) {
	// TODO
	return nil, nil, nil
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
	
	for rows.Next() {
		r, err := emptyRowForSchema(sch)
		if err != nil {
			return nil, err
		}

		err = rows.Scan(r...)
		if err != nil {
			return nil, err
		}
	}
	
	return sql.RowsToRowIter(result...), nil
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
			query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64,
			query.Type_BIT:
			var i int64
			return &i, nil
		case query.Type_DATE, query.Type_DATETIME, query.Type_TIMESTAMP:
			var t time.Time
			return &t, nil
		case query.Type_TEXT, query.Type_VARCHAR, query.Type_CHAR:
			var s string
			return &s, nil
		case query.Type_FLOAT32, query.Type_FLOAT64, query.Type_DECIMAL:
			var f float64
			return &f, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", t)
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
	case "int", "integer", "tinyint", "smallint", "mediumint", "bigint":
		return types.Int64, nil
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
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return types.Text, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return types.Blob, nil
	default:
		return nil, fmt.Errorf("unhandled type %s", columnType.DatabaseTypeName())
	}
}

func bindingArgs(bindings map[string]*query.BindVariable) []any {
	names := make([]string, len(bindings))
	var i int
	for name := range bindings {
		names[i] = name
		i++
	}

	args := make([]any, len(bindings))
	for i, name := range names {
		args[i] = bindings[name].Value
	}
	
	return args
}

func (s ServerQueryEngine) CloseSession(connID uint32) {
	// TODO
}

func (s ServerQueryEngine) Close() error {
	return s.Close()
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