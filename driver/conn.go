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
	dsn      string
	options  *Options
	dbConn   *dbConn
	session  sql.Session
	contexts ContextBuilder
	indexes  *sql.IndexRegistry
	views    *sql.ViewRegistry
}

// DSN returns the driver connection string.
func (c *Conn) DSN() string { return c.dsn }

// Session returns the SQL session.
func (c *Conn) Session() sql.Session { return c.session }

// Prepare validates the query and returns a statement.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.newStmt(context.Background(), query)
}

// newStmt builds a new statement with the query.
func (c *Conn) newStmt(ctx context.Context, query string) (*Stmt, error) {
	sctx, err := c.newContextWithQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// validate the query
	_, err = c.dbConn.engine.PrepareQuery(sctx, query)
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

// Exec executes a query that doesn't return rows.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmt, err := c.newStmt(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Exec(args)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// ExecContext executes a query that doesn't return rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.newStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	res, err := stmt.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Query executes a query that may return rows.
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	stmt, err := c.newStmt(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// QueryContext executes a query that may return rows.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.newStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *Conn) newContextWithQuery(ctx context.Context, query string) (*sql.Context, error) {
	// TODO(cjs): parse the dsn at c.dsn and set sql.WithInitialDatabase(parseDbName(c.dsn)) ?
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

// _ is a type assertion
var (
	_ driver.Conn           = ((*Conn)(nil))
	_ driver.Execer         = ((*Conn)(nil))
	_ driver.ExecerContext  = ((*Conn)(nil))
	_ driver.Queryer        = ((*Conn)(nil))
	_ driver.QueryerContext = ((*Conn)(nil))
)
