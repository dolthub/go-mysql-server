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

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/vitess/go/vt/proto/query"

	_ "github.com/go-sql-driver/mysql"
)

type ServerQueryEngine struct {
	server *server.Server
}

var _ QueryEngine = (*ServerQueryEngine)(nil)

var address   = "localhost"
// TODO: get random port
var port      = 3306

func NewServerQueryEngine() (*ServerQueryEngine, error) {
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
		server: s,
	}, nil
}

func newConnection() (*gosql.DB, error) {
	return gosql.Open("mysql", "root:@tcp(127.0.0.1)")
}

func (s ServerQueryEngine) PrepareQuery(ctx *sql.Context, query string) (sql.Node, error) {
	// TODO implement me
	panic("implement me")
}

func (s ServerQueryEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	// TODO implement me
	panic("implement me")
}

func (s ServerQueryEngine) EngineAnalyzer() *analyzer.Analyzer {
	// TODO implement me
	panic("implement me")
}

func (s ServerQueryEngine) EnginePreparedDataCache() *sqle.PreparedDataCache {
	// TODO implement me
	panic("implement me")
}

func (s ServerQueryEngine) QueryWithBindings(ctx *sql.Context, query string, bindings map[string]*query.BindVariable) (sql.Schema, sql.RowIter, error) {
	// TODO implement me
	panic("implement me")
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