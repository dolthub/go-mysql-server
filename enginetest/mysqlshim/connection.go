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
	"fmt"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"

	"github.com/dolthub/go-mysql-server/sql"
)

// MySQLShim is a shim for a local MySQL server. Ensure that a MySQL instance is running prior to using this shim. Note:
// this may be destructive to pre-existing data, as databases and tables will be created and destroyed.
type MySQLShim struct {
	conn      *dbr.Connection
	databases map[string]string
}

var _ sql.MutableDatabaseProvider = (*MySQLShim)(nil)

// NewMySQLShim returns a new MySQLShim.
func NewMySQLShim(user string, password string, host string, port int) (*MySQLShim, error) {
	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port), nil)
	if err != nil {
		return nil, err
	}
	err = conn.Ping()

	if err != nil {
		return nil, err
	}
	return &MySQLShim{conn, make(map[string]string)}, nil
}

// Database implements the interface sql.MutableDatabaseProvider.
func (m *MySQLShim) Database(ctx *sql.Context, name string) (sql.Database, error) {
	if dbName, ok := m.databases[strings.ToLower(name)]; ok {
		return Database{m, dbName}, nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements the interface sql.MutableDatabaseProvider.
func (m *MySQLShim) HasDatabase(ctx *sql.Context, name string) bool {
	_, ok := m.databases[strings.ToLower(name)]
	return ok
}

// AllDatabases implements the interface sql.MutableDatabaseProvider.
func (m *MySQLShim) AllDatabases(*sql.Context) []sql.Database {
	var dbStrings []string
	for _, dbName := range m.databases {
		dbStrings = append(dbStrings, dbName)
	}
	sort.Strings(dbStrings)
	dbs := make([]sql.Database, len(dbStrings))
	for i, dbString := range dbStrings {
		dbs[i] = Database{m, dbString}
	}
	return dbs
}

// CreateDatabase implements the interface sql.MutableDatabaseProvider.
func (m *MySQLShim) CreateDatabase(ctx *sql.Context, name string) error {
	_, err := m.conn.Exec(fmt.Sprintf("CREATE DATABASE `%s` DEFAULT COLLATE %s;", name, sql.Collation_Default.String()))
	if err != nil {
		return err
	}
	m.databases[strings.ToLower(name)] = name
	return nil
}

// DropDatabase implements the interface sql.MutableDatabaseProvider.
func (m *MySQLShim) DropDatabase(ctx *sql.Context, name string) error {
	_, err := m.conn.Exec(fmt.Sprintf("DROP DATABASE `%s`;", name))
	if err != nil {
		return err
	}
	delete(m.databases, strings.ToLower(name))
	return nil
}

// Close closes the shim. This will drop all databases created and accessed since this shim was created.
func (m *MySQLShim) Close() {
	for dbName := range m.databases {
		_, _ = m.conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
	}
	_ = m.conn.Close()
}

// Query queries the connection and return a row iterator.
func (m *MySQLShim) Query(db string, query string) (sql.RowIter, error) {
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
	return newMySQLIter(rows), nil
}

// QueryRows queries the connection and returns the rows returned.
func (m *MySQLShim) QueryRows(db string, query string) ([]sql.Row, error) {
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
func (m *MySQLShim) Exec(db string, query string) error {
	if len(db) > 0 {
		_, err := m.conn.Exec(fmt.Sprintf("USE `%s`;", db))
		if err != nil {
			return err
		}
	}
	_, err := m.conn.Exec(query)
	return err
}
