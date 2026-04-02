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

package mysqlshim

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
)

// MySQLHarness is a harness for a local MySQL server. This will modify databases and tables as the tests see fit, which
// may delete pre-existing data. Ensure that the MySQL instance may freely be modified without worry.
type MySQLHarness struct {
	shim           *MySQLShim
	skippedQueries map[string]struct{}
	setupData      []setup.SetupScript
	session        sql.Session
}

var _ enginetest.Harness = (*MySQLHarness)(nil)
var _ enginetest.ClientHarness = (*MySQLHarness)(nil)
var _ enginetest.SkippingHarness = (*MySQLHarness)(nil)
var _ enginetest.ResultEvaluationHarness = (*MySQLHarness)(nil)

func (m *MySQLHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
	return
}

func (m *MySQLHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	// Run setup scripts: first drop any databases that will be created, then execute all statements.
	for _, script := range m.setupData {
		for _, stmt := range script {
			lcStmt := strings.ToLower(strings.TrimSpace(stmt))
			if strings.HasPrefix(lcStmt, "create database") {
				// Extract the database name and drop it first for a clean slate.
				dbName := extractDatabaseName(stmt)
				if dbName != "" {
					_ = m.shim.Exec("", fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
				}
			}
		}
	}

	for _, script := range m.setupData {
		for _, stmt := range script {
			if err := m.shim.Exec("", stmt); err != nil {
				t.Fatalf("setup statement failed: %s\nerror: %v", stmt, err)
			}
		}
	}

	return m.shim, nil
}

// extractDatabaseName parses a CREATE DATABASE statement and returns the database name.
func extractDatabaseName(stmt string) string {
	// Tokenize by whitespace, then find the name token after "CREATE DATABASE [IF NOT EXISTS]"
	tokens := strings.Fields(stmt)
	i := 0
	// Skip "CREATE" and "DATABASE"
	if i < len(tokens) && strings.EqualFold(tokens[i], "create") {
		i++
	}
	if i < len(tokens) && strings.EqualFold(tokens[i], "database") {
		i++
	}
	// Skip optional "IF NOT EXISTS"
	if i+2 < len(tokens) &&
		strings.EqualFold(tokens[i], "if") &&
		strings.EqualFold(tokens[i+1], "not") &&
		strings.EqualFold(tokens[i+2], "exists") {
		i += 3
	}
	if i >= len(tokens) {
		return ""
	}
	name := tokens[i]
	// Strip backticks and trailing semicolons
	name = strings.Trim(name, "`")
	name = strings.TrimRight(name, ";")
	return name
}

func (m *MySQLHarness) NewContextWithClient(client sql.Client) *sql.Context {
	session := sql.NewBaseSessionWithClientServer("address", client, 1)
	return sql.NewContext(
		context.Background(),
		sql.WithSession(session),
	)
}

// MySQLDatabase represents a database for a local MySQL server.
type MySQLDatabase struct {
	harness *MySQLHarness
	dbName  string
}

// MySQLTable represents a table for a local MySQL server.
type MySQLTable struct {
	harness   *MySQLHarness
	tableName string
}

// NewMySQLHarness returns a new MySQLHarness.
func NewMySQLHarness(user string, password string, host string, port int) (*MySQLHarness, error) {
	shim, err := NewMySQLShim(user, password, host, port)
	if err != nil {
		return nil, err
	}
	return &MySQLHarness{shim, make(map[string]struct{}), nil, nil}, nil
}

// Parallelism implements the interface Harness.
func (m *MySQLHarness) Parallelism() int {
	return 1
}

// NewContext implements the interface Harness.
func (m *MySQLHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = enginetest.NewBaseSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

// SkipQueryTest implements the interface SkippingHarness.
func (m *MySQLHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

// QueriesToSkip adds queries that should be skipped.
func (m *MySQLHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

// SupportsNativeIndexCreation implements the interface IndexHarness.
func (m *MySQLHarness) SupportsNativeIndexCreation() bool {
	return true
}

// SupportsForeignKeys implements the interface ForeignKeyHarness.
func (m *MySQLHarness) SupportsForeignKeys() bool {
	return true
}

// SupportsKeylessTables implements the interface KeylessTableHarness.
func (m *MySQLHarness) SupportsKeylessTables() bool {
	return true
}

// EvaluateQueryResults implements ResultEvaluationHarness. It normalizes MySQL wire-protocol
// types to match the Go types used in GMS test expectations before comparing.
func (m *MySQLHarness) EvaluateQueryResults(
	t *testing.T,
	expectedRows []sql.Row,
	expectedCols []*sql.Column,
	expectedSch sql.Schema,
	actualRows []sql.Row,
	query string,
	wrapBehavior queries.WrapBehavior,
) {
	t.Helper()
	require := require.New(t)

	// Widen expected rows first (same as the default evaluator)
	for i, row := range expectedRows {
		for j, val := range row {
			expectedRows[i][j] = widenExpected(val)
		}
	}

	// Normalize actual rows: convert MySQL types to match GMS expected types
	for i, row := range actualRows {
		for j, val := range row {
			if i < len(expectedRows) && j < len(expectedRows[i]) {
				actualRows[i][j] = normalizeToExpected(val, expectedRows[i][j])
			}
		}
	}

	upperQuery := strings.ToUpper(query)
	orderBy := strings.Contains(upperQuery, "ORDER BY ")

	if orderBy || len(expectedRows) <= 1 {
		require.Equal(expectedRows, actualRows, "Unexpected result for query %s", query)
	} else {
		require.ElementsMatch(expectedRows, actualRows, "Unexpected result for query %s", query)
	}
}

// EvaluateExpectedError implements ResultEvaluationHarness.
func (m *MySQLHarness) EvaluateExpectedError(t *testing.T, expected string, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), expected)
}

// EvaluateExpectedErrorKind implements ResultEvaluationHarness.
func (m *MySQLHarness) EvaluateExpectedErrorKind(t *testing.T, expected *errors.Kind, err error) {
	t.Helper()
	require.Error(t, err)
}

// normalizeToExpected converts a MySQL actual value to match the type of the expected value.
func normalizeToExpected(actual, expected interface{}) interface{} {
	if actual == nil || expected == nil {
		return actual
	}

	switch expected.(type) {
	case bool:
		// MySQL returns 0/1 integers for boolean functions; convert to bool
		switch v := actual.(type) {
		case int64:
			return v != 0
		case int32:
			return v != 0
		case uint64:
			return v != 0
		case uint8:
			return v != 0
		case float64:
			return v != 0
		}
	case int:
		switch v := actual.(type) {
		case int64:
			return int(v)
		case int:
			return v
		}
	case int64:
		switch v := actual.(type) {
		case int:
			return int64(v)
		case int32:
			return int64(v)
		}
	case uint32:
		switch v := actual.(type) {
		case int64:
			return uint32(v)
		case uint64:
			return uint32(v)
		}
	case float64:
		switch v := actual.(type) {
		case int64:
			return float64(v)
		}
	case string:
		switch v := actual.(type) {
		case []byte:
			return string(v)
		}
	}

	return actual
}

// widenExpected normalizes expected values: widening small int types to int64.
func widenExpected(val interface{}) interface{} {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	default:
		return val
	}
}

// Close closes the connection. This will drop all databases created and accessed during the tests.
func (m *MySQLHarness) Close() {
	m.shim.Close()
}
