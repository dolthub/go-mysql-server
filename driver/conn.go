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

package driver

import (
	"context"
	"database/sql/driver"

	"github.com/dolthub/go-mysql-server/sql"
)

// Conn is a connection to a database.
type Conn struct {
	options  *Options
	dbConn   *dbConn
	session  sql.Session
	contexts ContextBuilder
	indexes  *sql.IndexRegistry
	views    *sql.ViewRegistry
}

// Session returns the SQL session.
func (c *Conn) Session() sql.Session { return c.session }

// Prepare validates the query and returns a statement.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	ctx, err := c.newContextWithQuery(context.Background(), query)
	if err != nil {
		return nil, err
	}

	// validate the query
	_, err = c.dbConn.engine.PrepareQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Stmt{c, query}, nil
}

// Close does nothing.
func (c *Conn) Close() error {
	return nil
}

// Begin returns a fake transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return fakeTransaction{}, nil
}

func (c *Conn) newContextWithQuery(ctx context.Context, query string) (*sql.Context, error) {
	return c.contexts.NewContext(ctx, c,
		sql.WithSession(c.session),
		sql.WithQuery(query),
		sql.WithPid(c.dbConn.nextProcessID()),
		sql.WithMemoryManager(c.dbConn.engine.MemoryManager),
		sql.WithProcessList(c.dbConn.engine.ProcessList))
}

type fakeTransaction struct{}

func (fakeTransaction) Commit() error   { return nil }
func (fakeTransaction) Rollback() error { return nil }
