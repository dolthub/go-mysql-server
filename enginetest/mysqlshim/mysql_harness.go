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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
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

func (m *MySQLHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
	return
}

func (m *MySQLHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	// TODO: this needs to initialize database state by first dropping any databases about to be created by the setup
	//  statements, then running them all on the connection.
	return m.shim, nil
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

// Close closes the connection. This will drop all databases created and accessed during the tests.
func (m *MySQLHarness) Close() {
	m.shim.Close()
}
