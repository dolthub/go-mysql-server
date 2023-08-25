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

	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// Stmt is a prepared statement.
type Stmt struct {
	conn     *Conn
	queryStr string
}

// Close does nothing.
func (s *Stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	bindings, err := valuesToBindings(args)
	if err != nil {
		return nil, err
	}
	return s.exec(context.Background(), bindings)
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	bindings, err := valuesToBindings(args)
	if err != nil {
		return nil, err
	}
	return s.query(context.Background(), bindings)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	bindings, err := namedValuesToBindings(args)
	if err != nil {
		return nil, err
	}
	return s.exec(ctx, bindings)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	bindings, err := namedValuesToBindings(args)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, bindings)
}

func (s *Stmt) exec(ctx context.Context, bindings map[string]*querypb.BindVariable) (driver.Result, error) {
	qctx, err := s.conn.newContextWithQuery(ctx, s.queryStr)
	if err != nil {
		return nil, err
	}

	_, rows, err := s.conn.dbConn.engine.QueryWithBindings(qctx, s.queryStr, nil, bindings)
	if err != nil {
		return nil, err
	}

	okr, found, err := getOKResult(qctx, rows)
	if err != nil {
		return nil, err
	} else if !found {
		return &ResultNotFound{}, nil
	}

	return &Result{okr}, nil
}

func (s *Stmt) query(ctx context.Context, bindings map[string]*querypb.BindVariable) (driver.Rows, error) {
	qctx, err := s.conn.newContextWithQuery(ctx, s.queryStr)
	if err != nil {
		return nil, err
	}

	cols, rows, err := s.conn.dbConn.engine.QueryWithBindings(qctx, s.queryStr, nil, bindings)
	if err != nil {
		return nil, err
	}

	return &Rows{s.conn.options, qctx, cols, rows}, nil
}
