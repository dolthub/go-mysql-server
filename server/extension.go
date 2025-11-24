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

package server

import (
	"context"
	"sort"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	sqle "github.com/dolthub/go-mysql-server"
)

// InterceptorChain allows an integrator to build a chain of
// |Interceptor| instances which will wrap and intercept the server's
// mysql.Handler.
//
// Example usage:
//
// var ic InterceptorChain
// ic.WithInterceptor(metricsInterceptor)
// ic.WithInterceptor(authInterceptor)
// server, err := NewServer(Config{ ..., Options: []Option{ic.Option()}, ...}, ...)
type InterceptorChain struct {
	inters []Interceptor
}

func (ic *InterceptorChain) WithInterceptor(h Interceptor) {
	ic.inters = append(ic.inters, h)
}

func (ic *InterceptorChain) Option() Option {
	return func(e *sqle.Engine, sm *SessionManager, handler mysql.Handler) (*sqle.Engine, *SessionManager, mysql.Handler) {
		chainHandler := buildChain(handler, ic.inters)
		return e, sm, chainHandler
	}
}

func buildChain(h mysql.Handler, inters []Interceptor) mysql.Handler {
	// XXX: Mutates |inters|
	sort.Slice(inters, func(i, j int) bool {
		return inters[i].Priority() < inters[j].Priority()
	})
	var last Chain = h
	for i := len(inters) - 1; i >= 0; i-- {
		filter := inters[i]
		next := last
		last = &chainInterceptor{i: filter, c: next}
	}
	return &interceptorHandler{h: h, c: last}
}

type Interceptor interface {
	// Priority returns the priority of the interceptor.
	Priority() int

	// Query is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	Query(ctx context.Context, chain Chain, c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) error

	// ParsedQuery is called when a connection receives a
	// query that has already been parsed. Note the contents
	// of the query slice may change after the first call to
	// callback. So the Handler should not hang on to the byte
	// slice.
	ParsedQuery(chain Chain, c *mysql.Conn, query string, parsed sqlparser.Statement, callback func(res *sqltypes.Result, more bool) error) error

	// MultiQuery is called when a connection receives a query and the
	// client supports MULTI_STATEMENT. It should process the first
	// statement in |query| and return the remainder. It will be called
	// multiple times until the remainder is |""|.
	MultiQuery(ctx context.Context, chain Chain, c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) (string, error)

	// Prepare is called when a connection receives a prepared
	// statement query.
	Prepare(ctx context.Context, chain Chain, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error)

	// StmtExecute is called when a connection receives a statement
	// execute query.
	StmtExecute(ctx context.Context, chain Chain, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error
}

type Chain interface {
	// ComQuery is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error

	// ComMultiQuery is called when a connection receives a query and the
	// client supports MULTI_STATEMENT. It should process the first
	// statement in |query| and return the remainder. It will be called
	// multiple times until the remainder is |""|.
	ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error)

	// ComPrepare is called when a connection receives a prepared
	// statement query.
	ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error)

	// ComStmtExecute is called when a connection receives a statement
	// execute query.
	ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error
}

type chainInterceptor struct {
	i Interceptor
	c Chain
}

func (ci *chainInterceptor) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return ci.i.Query(ctx, ci.c, c, query, callback)
}

func (ci *chainInterceptor) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	return ci.i.MultiQuery(ctx, ci.c, c, query, callback)
}

func (ci *chainInterceptor) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	return ci.i.Prepare(ctx, ci.c, c, query, prepare)
}

func (ci *chainInterceptor) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return ci.i.StmtExecute(ctx, ci.c, c, prepare, callback)
}

type interceptorHandler struct {
	c Chain
	h mysql.Handler
}

var _ mysql.Handler = (*interceptorHandler)(nil)

func (ih *interceptorHandler) NewConnection(c *mysql.Conn) {
	ih.h.NewConnection(c)
}

func (ih *interceptorHandler) ConnectionClosed(c *mysql.Conn) {
	ih.h.ConnectionClosed(c)
}

func (ih *interceptorHandler) ConnectionAuthenticated(c *mysql.Conn) error {
	return ih.h.ConnectionAuthenticated(c)
}

func (ih *interceptorHandler) ConnectionAborted(c *mysql.Conn, reason string) error {
	return ih.h.ConnectionAborted(c, reason)
}

func (ih *interceptorHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	return ih.h.ComInitDB(c, schemaName)
}

func (ih *interceptorHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return ih.c.ComQuery(ctx, c, query, callback)
}

func (ih *interceptorHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	return ih.c.ComMultiQuery(ctx, c, query, callback)
}

func (ih *interceptorHandler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	return ih.c.ComPrepare(ctx, c, query, prepare)
}

func (ih *interceptorHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return ih.c.ComStmtExecute(ctx, c, prepare, callback)
}

func (ih *interceptorHandler) WarningCount(c *mysql.Conn) uint16 {
	return ih.h.WarningCount(c)
}

func (ih *interceptorHandler) ComResetConnection(c *mysql.Conn) error {
	return ih.h.ComResetConnection(c)
}

func (ih *interceptorHandler) ParserOptionsForConnection(c *mysql.Conn) (ast.ParserOptions, error) {
	return ih.h.ParserOptionsForConnection(c)
}
