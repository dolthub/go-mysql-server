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

package sqle

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/metrics/discard"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// Auth used for authentication and authorization.
	Auth auth.Auth
}

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
	Auth     auth.Auth
	LS       *sql.LockSubsystem
}

type ColumnWithRawDefault struct {
	SqlColumn *sql.Column
	Default   string
}

var (
	// QueryCounter describes a metric that accumulates number of queries monotonically.
	QueryCounter = discard.NewCounter()

	// QueryErrorCounter describes a metric that accumulates number of failed queries monotonically.
	QueryErrorCounter = discard.NewCounter()

	// QueryHistogram describes a queries latency.
	QueryHistogram = discard.NewHistogram()
)

func observeQuery(ctx *sql.Context, query string) func(err error) {
	logrus.WithField("query", query).Debug("executing query")
	span, _ := ctx.Span("query", opentracing.Tag{Key: "query", Value: query})

	t := time.Now()
	return func(err error) {
		if err != nil {
			QueryErrorCounter.With("query", query, "error", err.Error()).Add(1)
		} else {
			QueryCounter.With("query", query).Add(1)
			QueryHistogram.With("query", query, "duration", "seconds").Observe(time.Since(t).Seconds())
		}

		span.Finish()
	}
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`.
func New(c *sql.Catalog, a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
	}

	ls := sql.NewLockSubsystem()

	c.MustRegister(
		sql.FunctionN{
			Name: "version",
			Fn:   function.NewVersion(versionPostfix),
		},
		sql.Function0{
			Name: "database",
			Fn:   function.NewDatabase(c),
		},
		sql.Function0{
			Name: "schema",
			Fn:   function.NewDatabase(c),
		})

	c.MustRegister(function.Defaults...)
	c.MustRegister(function.GetLockingFuncs(ls)...)

	// use auth.None if auth is not specified
	var au auth.Auth
	if cfg == nil || cfg.Auth == nil {
		au = new(auth.None)
	} else {
		au = cfg.Auth
	}

	return &Engine{c, a, au, ls}
}

// NewDefault creates a new default Engine.
func NewDefault() *Engine {
	c := sql.NewCatalog()
	a := analyzer.NewDefault(c)

	return New(c, a, nil)
}

// AnalyzeQuery analyzes a query and returns its Schema.
func (e *Engine) AnalyzeQuery(
	ctx *sql.Context,
	query string,
) (sql.Schema, error) {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, err
	}

	analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}

	return analyzed.Schema(), nil
}

// Query executes a query.
func (e *Engine) Query(
	ctx *sql.Context,
	query string,
) (sql.Schema, sql.RowIter, error) {
	return e.QueryWithBindings(ctx, query, nil)
}

func (e *Engine) QueryWithBindings(
	ctx *sql.Context,
	query string,
	bindings map[string]sql.Expression,
) (sql.Schema, sql.RowIter, error) {
	var (
		parsed, analyzed sql.Node
		iter             sql.RowIter
		err              error
	)

	finish := observeQuery(ctx, query)
	defer finish(err)

	parsed, err = parse.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	var perm = auth.ReadPerm
	var typ = sql.QueryProcess
	switch parsed.(type) {
	case *plan.CreateIndex:
		typ = sql.CreateIndexProcess
		perm = auth.ReadPerm | auth.WritePerm
	case *plan.CreateForeignKey, *plan.DropForeignKey, *plan.AlterIndex, *plan.CreateView,
		*plan.DeleteFrom, *plan.DropIndex, *plan.DropView,
		*plan.InsertInto, *plan.LockTables, *plan.UnlockTables,
		*plan.Update:
		perm = auth.ReadPerm | auth.WritePerm
	}

	err = e.Auth.Allowed(ctx, perm)
	if err != nil {
		return nil, nil, err
	}

	ctx, err = e.Catalog.AddProcess(ctx, typ, query)
	defer func() {
		if err != nil && ctx != nil {
			e.Catalog.Done(ctx.Pid())
		}
	}()

	if err != nil {
		return nil, nil, err
	}

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, nil, err
	}

	if len(bindings) > 0 {
		analyzed, err = plan.ApplyBindings(analyzed, bindings)
		if err != nil {
			return nil, nil, err
		}
	}

	iter, err = analyzed.RowIter(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

// ParseDefaults takes in a schema, along with each column's default value in a string form, and returns the schema
// with the default values parsed and resolved.
func ResolveDefaults(tableName string, schema []*ColumnWithRawDefault) (sql.Schema, error) {
	ctx := sql.NewEmptyContext()
	e := NewDefault()
	db := plan.NewDummyResolvedDB("temporary")
	unresolvedSchema := make(sql.Schema, len(schema))
	defaultCount := 0
	for i, col := range schema {
		unresolvedSchema[i] = col.SqlColumn
		if col.Default != "" {
			var err error
			unresolvedSchema[i].Default, err = parse.StringToColumnDefaultValue(ctx, col.Default)
			if err != nil {
				return nil, err
			}
			defaultCount++
		}
	}
	// if all defaults are nil, we can skip the rest of this
	if defaultCount == 0 {
		return unresolvedSchema, nil
	}
	// *plan.CreateTable properly handles resolving default values, so we hijack it
	createTable := plan.NewCreateTable(db, tableName, unresolvedSchema, false, nil, nil)
	analyzed, err := e.Analyzer.Analyze(ctx, createTable, nil)
	if err != nil {
		return nil, err
	}
	analyzedQueryProcess, ok := analyzed.(*plan.QueryProcess)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown analyzed result type `%T`", analyzed)
	}
	analyzedCreateTable, ok := analyzedQueryProcess.Child.(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown query process child type `%T`", analyzedQueryProcess)
	}
	return analyzedCreateTable.Schema(), nil
}

// ApplyDefaults applies the default values of the given column indices to the given row, and returns a new row with the updated values.
// This assumes that the given row has placeholder `nil` values for the default entries, and also that each column in a table is
// present and in the order as represented by the schema. If no columns are given, then the given row is returned. Column indices should
// be sorted and in ascending order, however this is not enforced.
func ApplyDefaults(ctx *sql.Context, tblSch sql.Schema, cols []int, row sql.Row) (sql.Row, error) {
	if len(cols) == 0 {
		return row, nil
	}
	newRow := row.Copy()
	if len(tblSch) != len(row) {
		return nil, fmt.Errorf("any row given to ApplyDefaults must be of the same length as the table it represents")
	}
	var secondPass []int
	for _, col := range cols {
		if col < 0 || col > len(tblSch) {
			return nil, fmt.Errorf("column index `%d` is out of bounds, table schema has `%d` number of columns", col, len(tblSch))
		}
		if !tblSch[col].Default.IsLiteral() {
			secondPass = append(secondPass, col)
			continue
		}
		val, err := tblSch[col].Default.Eval(ctx, newRow)
		if err != nil {
			return nil, err
		}
		newRow[col], err = tblSch[col].Type.Convert(val)
		if err != nil {
			return nil, err
		}
	}
	for _, col := range secondPass {
		val, err := tblSch[col].Default.Eval(ctx, newRow)
		if err != nil {
			return nil, err
		}
		newRow[col], err = tblSch[col].Type.Convert(val)
		if err != nil {
			return nil, err
		}
	}
	return newRow, nil
}

// Async returns true if the query is async. If there are any errors with the
// query it returns false
func (e *Engine) Async(ctx *sql.Context, query string) bool {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return false
	}

	asyncNode, ok := parsed.(sql.AsyncNode)
	return ok && asyncNode.IsAsync()
}

// AddDatabase adds the given database to the catalog.
func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.AddDatabase(db)
}
