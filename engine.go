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
	"os"
	"sync"

	"github.com/pkg/errors"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// IsReadOnly sets the engine to disallow modification queries.
	IsReadOnly bool
	// IncludeRootAccount adds the root account (with no password) to the list of accounts, and also enables
	// authentication.
	IncludeRootAccount bool
	// TemporaryUsers adds any users that should be included when the engine is created. By default, authentication is
	// disabled, and including any users here will enable authentication. All users in this list will have full access.
	// This field is only temporary, and will be removed as development on users and authentication continues.
	TemporaryUsers []TemporaryUser
}

// TemporaryUser is a user that will be added to the engine. This is for temporary use while the remaining features
// are implemented. Replaces the old "auth.New..." functions for adding a user.
type TemporaryUser struct {
	Username string
	Password string
}

// Engine is a SQL engine.
type Engine struct {
	Analyzer          *analyzer.Analyzer
	LS                *sql.LockSubsystem
	ProcessList       sql.ProcessList
	MemoryManager     *sql.MemoryManager
	BackgroundThreads *sql.BackgroundThreads
	IsReadOnly        bool
	PreparedData      map[uint32]PreparedData
	mu                *sync.Mutex
}

type ColumnWithRawDefault struct {
	SqlColumn *sql.Column
	Default   string
}

type PreparedData struct {
	Node  sql.Node
	Query string
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`. Should call Engine.Close() to finalize
// dependency lifecycles.
func New(a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	var isReadOnly bool
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
		isReadOnly = cfg.IsReadOnly
		if cfg.IncludeRootAccount {
			a.Catalog.MySQLDb.AddRootAccount()
		}
		for _, tempUser := range cfg.TemporaryUsers {
			a.Catalog.MySQLDb.AddSuperUser(tempUser.Username, tempUser.Password)
		}
	}

	ls := sql.NewLockSubsystem()

	emptyCtx := sql.NewEmptyContext()
	a.Catalog.RegisterFunction(emptyCtx, sql.FunctionN{
		Name: "version",
		Fn:   function.NewVersion(versionPostfix),
	})
	a.Catalog.RegisterFunction(emptyCtx, function.GetLockingFuncs(ls)...)

	return &Engine{
		Analyzer:          a,
		MemoryManager:     sql.NewMemoryManager(sql.ProcessMemory),
		ProcessList:       NewProcessList(),
		LS:                ls,
		BackgroundThreads: sql.NewBackgroundThreads(),
		IsReadOnly:        isReadOnly,
		PreparedData:      make(map[uint32]PreparedData),
		mu:                &sync.Mutex{},
	}
}

// NewDefault creates a new default Engine.
func NewDefault(pro sql.DatabaseProvider) *Engine {
	a := analyzer.NewDefault(pro)
	return New(a, nil)
}

// AnalyzeQuery analyzes a query and returns its sql.Node
func (e *Engine) AnalyzeQuery(
	ctx *sql.Context,
	query string,
) (sql.Node, error) {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, err
	}
	return e.Analyzer.Analyze(ctx, parsed, nil)
}

// PrepareQuery returns a partially analyzed query
func (e *Engine) PrepareQuery(
	ctx *sql.Context,
	query string,
) (sql.Node, error) {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, err
	}

	node, err := e.Analyzer.PrepareQuery(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}
	e.CachePreparedStmt(ctx, node, query)
	return node, nil
}

// Query executes a query.
func (e *Engine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return e.QueryWithBindings(ctx, query, nil)
}

// QueryWithBindings executes the query given with the bindings provided
func (e *Engine) QueryWithBindings(ctx *sql.Context, query string, bindings map[string]sql.Expression) (sql.Schema, sql.RowIter, error) {
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
		iter2    sql.RowIter2
		err      error
	)

	if parsed == nil {
		parsed, err = parse.Parse(ctx, query)
		if err != nil {
			return nil, nil, err
		}
	}

	_, err = e.beginTransaction(ctx, parsed)
	if err != nil {
		return nil, nil, err
	}

	if p, ok := e.preparedDataForSession(ctx.Session); ok && p.Query == query {
		analyzed, err = e.analyzePreparedQuery(ctx, query, bindings)
	} else {
		analyzed, err = e.analyzeQuery(ctx, query, parsed, bindings)
	}
	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			err = errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}

		return nil, nil, err
	}

	useIter2 := false
	if enableRowIter2 {
		useIter2 = allNode2(analyzed)
	}

	if useIter2 {
		iter2, err = analyzed.(sql.Node2).RowIter2(ctx, nil)
		iter = iter2
	} else {
		iter, err = analyzed.RowIter(ctx, nil)
	}
	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			err = errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}

		return nil, nil, err
	}

	if useIter2 {
		iter = rowFormatSelectorIter{
			iter:    iter,
			iter2:   iter2,
			isNode2: useIter2,
		}
	}

	return analyzed.Schema(), iter, nil
}

// clearAutocommitTransaction unsets the transaction from the current session if it is an implicitly
// created autocommit transaction. This enables the next request to have an autocommit transaction
// correctly started.
func clearAutocommitTransaction(ctx *sql.Context) error {
	// The GetIgnoreAutoCommit property essentially says the current transaction is an explicit,
	// user-created transaction and we should not process autocommit. So, if it's set, then we
	// don't need to do anything here to clear implicit transaction state.
	//
	// TODO: This logic would probably read more clearly if we could just ask the session/ctx if the
	//       current transaction is automatically created or explicitly created by the caller.
	if ctx.GetIgnoreAutoCommit() {
		return nil
	}

	autocommit, err := plan.IsSessionAutocommit(ctx)
	if err != nil {
		return err
	}

	if autocommit {
		ctx.SetTransaction(nil)
	}

	return nil
}

func (e *Engine) CachePreparedStmt(ctx *sql.Context, analyzed sql.Node, query string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.PreparedData[ctx.Session.ID()] = PreparedData{
		Query: query,
		Node:  analyzed,
	}
}

// preparedDataForSession returns the prepared data for a given session.
// Second parameter is false if the session has no prepared data.
func (e *Engine) preparedDataForSession(sess sql.Session) (PreparedData, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	data, ok := e.PreparedData[sess.ID()]
	return data, ok
}

// preparedQuery returns the prepared plan's query string for a given
// context's session id, or an empty string if the session has no prepared data.
func (e *Engine) preparedQuery(ctx *sql.Context) string {
	if data, ok := e.preparedDataForSession(ctx.Session); ok {
		return data.Query
	}
	return ""
}

// preparedNode returns the pre-analyzed plan for a given
// context's session id, or nil if the session has no prepared data.
func (e *Engine) preparedNode(ctx *sql.Context) sql.Node {
	e.mu.Lock()
	defer e.mu.Unlock()
	if data, ok := e.PreparedData[ctx.Session.ID()]; ok {
		return data.Node
	}
	return nil
}

// CloseSession deletes session specific prepared statement data
func (e *Engine) CloseSession(ctx *sql.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.PreparedData, ctx.Session.ID())
}

func (e *Engine) analyzeQuery(ctx *sql.Context, query string, parsed sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	var (
		analyzed sql.Node
		err      error
	)

	if parsed == nil {
		parsed, err = parse.Parse(ctx, query)
		if err != nil {
			return nil, err
		}
	}

	err = e.readOnlyCheck(parsed)
	if err != nil {
		return nil, err
	}

	if len(bindings) > 0 {
		parsed, err = plan.ApplyBindings(parsed, bindings)
		if err != nil {
			return nil, err
		}
	}

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}

	return analyzed, nil
}

func (e *Engine) analyzePreparedQuery(ctx *sql.Context, query string, bindings map[string]sql.Expression) (sql.Node, error) {
	ctx.GetLogger().Tracef("optimizing prepared plan for query: %s", query)

	analyzed := e.preparedNode(ctx)
	analyzed, err := analyzer.DeepCopyNode(analyzed)
	if err != nil {
		return nil, err
	}

	if len(bindings) > 0 {
		analyzed, err = plan.ApplyBindings(analyzed, bindings)
		if err != nil {
			return nil, err
		}
	}
	ctx.GetLogger().Tracef("plan before re-opt: %s", analyzed.String())

	analyzed, _, err = e.Analyzer.AnalyzePrepared(ctx, analyzed, nil)
	if err != nil {
		return nil, err
	}

	ctx.GetLogger().Tracef("plan after re-opt: %s", analyzed.String())
	return analyzed, nil
}

// allNode2 returns whether all the nodes in the tree implement Node2.
func allNode2(n sql.Node) bool {
	allNode2 := true
	transform.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			table := n.Table
			if tw, ok := table.(sql.TableWrapper); ok {
				table = tw.Underlying()
			}
			if _, ok := table.(sql.Table2); !ok {
				allNode2 = false
				return false
			}
		}
		if _, ok := n.(sql.Node2); n != nil && !ok {
			allNode2 = false
			return false
		}
		return true
	})
	if !allNode2 {
		return allNode2
	}

	// All expressions in the tree must likewise be Expression2, and all types Type2, or we can't use rowFrame iteration
	// TODO: likely that some nodes rely on expressions but don't implement sql.Expressioner, or implement it incompletely
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		if e == nil {
			return false
		}
		if _, ok := e.(sql.Expression2); !ok {
			allNode2 = false
			return false
		}
		if _, ok := e.Type().(sql.Type2); !ok {
			allNode2 = false
			return false
		}
		return true
	})

	return allNode2
}

// rowFormatSelectorIter is a wrapping row iter that implements RowIterTypeSelector so that clients consuming rows from it
// know whether it's safe to iterate as RowIter or RowIter2.
type rowFormatSelectorIter struct {
	iter    sql.RowIter
	iter2   sql.RowIter2
	isNode2 bool
}

var _ sql.RowIterTypeSelector = rowFormatSelectorIter{}
var _ sql.RowIter = rowFormatSelectorIter{}
var _ sql.RowIter2 = rowFormatSelectorIter{}

func (t rowFormatSelectorIter) Next(context *sql.Context) (sql.Row, error) {
	return t.iter.Next(context)
}

func (t rowFormatSelectorIter) Close(context *sql.Context) error {
	if t.iter2 != nil {
		return t.iter2.Close(context)
	}
	return t.iter.Close(context)
}

func (t rowFormatSelectorIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	return t.iter2.Next2(ctx, frame)
}

func (t rowFormatSelectorIter) IsNode2() bool {
	return t.isNode2
}

const (
	enableIter2EnvVar = "ENABLE_ROW_ITER_2"
)

var enableRowIter2 bool

func init() {
	_, ok := os.LookupEnv(enableIter2EnvVar)
	if ok {
		enableRowIter2 = true
	}
}

func (e *Engine) beginTransaction(ctx *sql.Context, parsed sql.Node) (string, error) {
	// Before we begin a transaction, we need to know if the database being operated on is not the one
	// currently selected
	transactionDatabase := analyzer.GetTransactionDatabase(ctx, parsed)

	// TODO: this won't work with transactions that cross database boundaries, we need to detect that and error out
	beginNewTransaction := ctx.GetTransaction() == nil || plan.ReadCommitted(ctx)
	if beginNewTransaction {
		ctx.GetLogger().Tracef("beginning new transaction")
		if len(transactionDatabase) > 0 {
			database, err := e.Analyzer.Catalog.Database(ctx, transactionDatabase)
			// if the database doesn't exist, just don't start a transaction on it, let other layers complain
			if sql.ErrDatabaseNotFound.Is(err) || sql.ErrDatabaseAccessDeniedForUser.Is(err) {
				return "", nil
			} else if err != nil {
				return "", err
			}

			if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
				database = privilegedDatabase.Unwrap()
			}
			tdb, ok := database.(sql.TransactionDatabase)
			if ok {
				tx, err := tdb.StartTransaction(ctx, sql.ReadWrite)
				if err != nil {
					return "", err
				}

				ctx.SetTransaction(tx)
			}
		}
	}

	return transactionDatabase, nil
}

func (e *Engine) Close() error {
	for _, p := range e.ProcessList.Processes() {
		e.ProcessList.Kill(p.Connection)
	}
	return e.BackgroundThreads.Shutdown()
}

func (e *Engine) WithBackgroundThreads(b *sql.BackgroundThreads) *Engine {
	e.BackgroundThreads = b
	return e
}

// readOnlyCheck checks to see if the query is valid with the modification setting of the engine.
func (e *Engine) readOnlyCheck(node sql.Node) error {
	if plan.IsDDLNode(node) && e.IsReadOnly {
		return sql.ErrNotAuthorized.New()
	}
	switch node.(type) {
	case
		*plan.DeleteFrom, *plan.InsertInto, *plan.Update, *plan.LockTables, *plan.UnlockTables:
		if e.IsReadOnly {
			return sql.ErrNotAuthorized.New()
		}
	}
	return nil
}

// ResolveDefaults takes in a schema, along with each column's default value in a string form, and returns the schema
// with the default values parsed and resolved.
func ResolveDefaults(tableName string, schema []*ColumnWithRawDefault) (sql.Schema, error) {
	// todo: change this function or thread a context
	ctx := sql.NewEmptyContext()
	db := plan.NewDummyResolvedDB("temporary")
	e := NewDefault(memory.NewMemoryDBProvider(db))
	defer e.Close()

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
	createTable := plan.NewCreateTable(db, tableName, false, false, &plan.TableSpec{Schema: sql.NewPrimaryKeySchema(unresolvedSchema)})

	analyzed, err := e.Analyzer.Analyze(ctx, createTable, nil)
	if err != nil {
		return nil, err
	}

	analyzedQueryProcess, ok := analyzed.(*plan.QueryProcess)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown analyzed result type `%T`", analyzed)
	}

	analyzedCreateTable, ok := analyzedQueryProcess.Child().(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown query process child type `%T`", analyzedQueryProcess)
	}

	return analyzedCreateTable.CreateSchema.Schema, nil
}

// ColumnsFromCheckDefinition retrieves the Column Names referenced by a CheckDefinition
func ColumnsFromCheckDefinition(ctx *sql.Context, def *sql.CheckDefinition) ([]string, error) {
	// Evaluate the CheckDefinition to get evaluated Expression
	c, err := analyzer.ConvertCheckDefToConstraint(ctx, def)
	if err != nil {
		return nil, err
	}
	// Look for any column references in the evaluated Expression
	var cols []string
	sql.Inspect(c.Expr, func(expr sql.Expression) bool {
		if c, ok := expr.(*expression.UnresolvedColumn); ok {
			cols = append(cols, c.Name())
			return false
		}
		return true
	})
	return cols, nil
}
