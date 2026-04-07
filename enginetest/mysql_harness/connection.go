// Copyright 2021 Dolthub, Inc.
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

package mysql_harness

import (
	"fmt"
	"strings"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// MySQLQueryEngine is a QueryEngine to run enginetest cases against a real MySQL server. It is incomplete but
// capable of running many ScriptTests.
type MySQLQueryEngine struct {
	conn      *dbr.Connection
	databases map[string]string
}

var _ enginetest.QueryEngine = (*MySQLQueryEngine)(nil)

func (m *MySQLQueryEngine) QueryWithBindings(ctx *sql.Context, query string, parsed vitess.Statement, bindings map[string]vitess.Expr, qFlags *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	// The MySQL shim doesn't support prepared statements with bindings.
	// For queries without bindings, delegate to the regular Query method.
	return m.Query(ctx, query)
}

func (m *MySQLQueryEngine) Close() error {
	// Don't close the connection here — it's shared across tests.
	// The MySQLHarness.Close() method handles cleanup.
	return nil
}

// NewMySQLShim returns a new MySQLQueryEngine.
func NewMySQLShim(user string, password string, host string, port int) (*MySQLQueryEngine, error) {
	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port), nil)
	if err != nil {
		return nil, err
	}
	err = conn.Ping()

	if err != nil {
		return nil, err
	}
	return &MySQLQueryEngine{conn, make(map[string]string)}, nil
}

// isWriteStatement returns true for SQL statements that modify data and return an OkResult.
func isWriteStatement(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "REPLACE") ||
		strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "DROP") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.HasPrefix(upper, "TRUNCATE") ||
		strings.HasPrefix(upper, "SET") ||
		strings.HasPrefix(upper, "USE")
}

// Query queries the connection and return a row iterator.
func (m *MySQLQueryEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	if len(ctx.GetCurrentDatabase()) > 0 {
		_, err := m.conn.Exec(fmt.Sprintf("USE `%s`;", ctx.GetCurrentDatabase()))
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// For write statements, use Exec and return an OkResult
	if isWriteStatement(query) {
		result, err := m.conn.Exec(query)
		if err != nil {
			return nil, nil, nil, err
		}
		affected, _ := result.RowsAffected()
		insertID, _ := result.LastInsertId()
		return types.OkResultSchema, sql.RowsToRowIter(sql.Row{types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertID),
		}}), nil, nil
	}

	rows, err := m.conn.Query(query)
	if err != nil {
		return nil, nil, nil, err
	}

	iter := newMySQLIter(rows)
	return iter.Schema(), iter, nil, nil
}

// QueryRows queries the connection and returns the rows returned.
func (m *MySQLQueryEngine) QueryRows(db string, query string) ([]sql.Row, error) {
	ctx := sql.NewEmptyContext()
	if len(db) > 0 {
		_, err := m.conn.Exec(fmt.Sprintf("USE `%s`;", db))
		if err != nil {
			return nil, err
		}
	}
	rows, err := m.conn.Query(query)
	if err != nil {
		return nil, err
	}
	iter := newMySQLIter(rows)
	defer iter.Close(ctx)
	allRows, err := sql.RowIterToRows(ctx, iter)
	if err != nil {
		return nil, err
	}
	return allRows, nil
}

// Exec executes the query on the connection.
func (m *MySQLQueryEngine) Exec(db string, query string) error {
	if len(db) > 0 {
		_, err := m.conn.Exec(fmt.Sprintf("USE `%s`;", db))
		if err != nil {
			return err
		}
	}
	_, err := m.conn.Exec(query)
	return err
}

func (m *MySQLQueryEngine) PrepareQuery(context *sql.Context, s string) (sql.Node, error) {
	panic("unimplemented")
}

func (m *MySQLQueryEngine) AnalyzeQuery(context *sql.Context, s string) (sql.Node, error) {
	panic("unimplemented")
}

func (m *MySQLQueryEngine) EngineAnalyzer() *analyzer.Analyzer {
	panic("unimplemented")
}

func (m *MySQLQueryEngine) EngineEventScheduler() sql.EventScheduler {
	panic("unimplemented")
}
