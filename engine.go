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

// Query executes a query. If parsed is non-nil, it will be used instead of parsing the query from text.
func (e *Engine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return e.QueryWithBindings(ctx, query, nil)
}

// QueryWithBindings executes the query given with the bindings provided
func (e *Engine) QueryWithBindings(
	ctx *sql.Context,
	query string,
	bindings map[string]sql.Expression,
) (sql.Schema, sql.RowIter, error) {
	return e.QueryNodeWithBindings(ctx, query, nil, bindings)
}

// QueryNodeWithBindings executes the query given with the bindings provided. If parsed is non-nil, it will be used
// instead of parsing the query from text.
func (e *Engine) QueryNodeWithBindings(
	ctx *sql.Context,
	query string,
	parsed sql.Node,
	bindings map[string]sql.Expression,
) (sql.Schema, sql.RowIter, error) {
	var (
		analyzed sql.Node
		iter     sql.RowIter
		err      error
	)

	if parsed == nil {
		parsed, err = parse.Parse(ctx, query)
		if err != nil {
			return nil, nil, err
		}
	}

	err = e.authCheck(ctx, parsed)
	if err != nil {
		return nil, nil, err
	}

	if len(bindings) > 0 {
		parsed, err = plan.ApplyBindings(ctx, parsed, bindings)
		if err != nil {
			return nil, nil, err
		}
	}

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, nil, err
	}

	transactionDatabase, err := e.beginTransaction(ctx, parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err = analyzed.RowIter(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	autoCommit, err := isSessionAutocommit(ctx)
	if err != nil {
		return nil, nil, err
	}

	if autoCommit {
		iter = transactionCommittingIter{iter, transactionDatabase}
	}

	return analyzed.Schema(), iter, nil
}

func (e *Engine) beginTransaction(ctx *sql.Context, parsed sql.Node) (string, error) {
	// Before we begin a transaction, we need to know if the database being operated on is not the one
	// currently selected
	transactionDatabase := determineTransactionDatabase(ctx, parsed)

	// TODO: this won't work with transactions that cross database boundaries, we need to detect that and error out
	beginNewTransaction := ctx.GetTransaction() == nil
	if beginNewTransaction {
		if len(transactionDatabase) > 0 {
			database, err := e.Catalog.Database(transactionDatabase)
			if err != nil {
				return "", err
			}

			tdb, ok := database.(sql.TransactionDatabase)
			if ok {
				tx, err := tdb.StartTransaction(ctx)
				if err != nil {
					return "", err
				}
				ctx.SetTransaction(tx)
			}
		}
	}

	return transactionDatabase, nil
}

// transactionCommittingIter is a simple RowIter wrapper to allow the engine to conditionally commit a transaction
// during the Close() operation
type transactionCommittingIter struct {
	childIter           sql.RowIter
	transactionDatabase string
}

func (t transactionCommittingIter) Next() (sql.Row, error) {
	return t.childIter.Next()
}

func (t transactionCommittingIter) Close(ctx *sql.Context) error {
	err := t.childIter.Close(ctx)
	if err != nil {
		return err
	}

	tx := ctx.GetTransaction()
	commitTransaction := (tx != nil) && !ctx.GetIgnoreAutoCommit()
	if commitTransaction {
		logrus.Tracef("committing transaction %s", tx)
		if err := ctx.Session.CommitTransaction(ctx, t.transactionDatabase, tx); err != nil {
			return err
		}

		// Clearing out the current transaction will tell us to start a new one the next time this session queries
		ctx.SetTransaction(nil)
	}

	return nil
}

func isSessionAutocommit(ctx *sql.Context) (bool, error) {
	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return sql.ConvertToBool(autoCommitSessionVar)
}

func determineTransactionDatabase(ctx *sql.Context, parsed sql.Node) string {
	// For USE DATABASE statements, we need to process them here, before executing the query, so that we can set the
	// database for transactions appropriately
	switch n := parsed.(type) {
	case *plan.Use:
		ctx.SetCurrentDatabase(n.Database().Name())
	}
	transactionDatabase := ctx.GetCurrentDatabase()

	switch n := parsed.(type) {
	case *plan.CreateTable:
		if n.Database() != nil && n.Database().Name() != "" {
			transactionDatabase = n.Database().Name()
		}
	case *plan.InsertInto:
		if n.Database() != nil && n.Database().Name() != "" {
			transactionDatabase = n.Database().Name()
		}
	case *plan.DeleteFrom:
		if n.Database() != "" {
			transactionDatabase = n.Database()
		}
	case *plan.Update:
		if n.Database() != "" {
			transactionDatabase = n.Database()
		}
	}

	return transactionDatabase
}

func (e *Engine) authCheck(ctx *sql.Context, node sql.Node) error {
	var perm = auth.ReadPerm
	if plan.IsDDLNode(node) {
		perm = auth.ReadPerm | auth.WritePerm
	}
	switch node.(type) {
	case
		*plan.DeleteFrom, *plan.InsertInto, *plan.Update, *plan.LockTables, *plan.UnlockTables:
		perm = auth.ReadPerm | auth.WritePerm
	}

	return e.Auth.Allowed(ctx, perm)
}

// ResolveDefaults takes in a schema, along with each column's default value in a string form, and returns the schema
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
	createTable := plan.NewCreateTable(db, tableName, false, false, &plan.TableSpec{Schema: unresolvedSchema})
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
		} else if tblSch[col].Default == nil && !tblSch[col].Nullable {
			val := tblSch[col].Type.Zero()
			var err error
			newRow[col], err = tblSch[col].Type.Convert(val)
			if err != nil {
				return nil, err
			}
		} else {
			val, err := tblSch[col].Default.Eval(ctx, newRow)
			if err != nil {
				return nil, err
			}
			newRow[col], err = tblSch[col].Type.Convert(val)
			if err != nil {
				return nil, err
			}
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

// AddDatabase adds the given database to the catalog.
func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.AddDatabase(db)
}
