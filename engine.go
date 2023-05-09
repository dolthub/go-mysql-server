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
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

const experimentalFlag = "GMS_EXPERIMENTAL"

var ExperimentalGMS bool

func init() {
	ExperimentalGMS = os.Getenv(experimentalFlag) != ""
}

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// IsReadOnly sets the engine to disallow modification queries.
	IsReadOnly     bool
	IsServerLocked bool
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

// PreparedDataCache manages all the prepared data for every session for every query for an engine
type PreparedDataCache struct {
	data map[uint32]map[string]sql.Node
	mu   *sync.Mutex
}

func NewPreparedDataCache() *PreparedDataCache {
	return &PreparedDataCache{
		data: make(map[uint32]map[string]sql.Node),
		mu:   &sync.Mutex{},
	}
}

// GetCachedStmt will retrieve the prepared sql.Node associated with the ctx.SessionId and query if it exists
// it will return nil, false if the query does not exist
func (p *PreparedDataCache) GetCachedStmt(sessId uint32, query string) (sql.Node, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sessData, ok := p.data[sessId]; ok {
		data, ok := sessData[query]
		return data, ok
	}
	return nil, false
}

// GetSessionData returns all the prepared queries for a particular session
func (p *PreparedDataCache) GetSessionData(sessId uint32) map[string]sql.Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data[sessId]
}

// DeleteSessionData clears a session along with all prepared queries for that session
func (p *PreparedDataCache) DeleteSessionData(sessId uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.data, sessId)
}

// CacheStmt saves the prepared node and associates a ctx.SessionId and query to it
func (p *PreparedDataCache) CacheStmt(sessId uint32, query string, node sql.Node) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.data[sessId]; !ok {
		p.data[sessId] = make(map[string]sql.Node)
	}
	p.data[sessId][query] = node
}

// UncacheStmt removes the prepared node associated with a ctx.SessionId and query to it
func (p *PreparedDataCache) UncacheStmt(sessId uint32, query string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.data[sessId]; !ok {
		return
	}
	delete(p.data[sessId], query)
}

// Engine is a SQL engine.
type Engine struct {
	Analyzer          *analyzer.Analyzer
	LS                *sql.LockSubsystem
	ProcessList       sql.ProcessList
	MemoryManager     *sql.MemoryManager
	BackgroundThreads *sql.BackgroundThreads
	IsReadOnly        bool
	IsServerLocked    bool
	PreparedDataCache *PreparedDataCache
	mu                *sync.Mutex
	Version           sql.AnalyzerVersion
}

type ColumnWithRawDefault struct {
	SqlColumn *sql.Column
	Default   string
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`. Should call Engine.Close() to finalize
// dependency lifecycles.
func New(a *analyzer.Analyzer, cfg *Config) *Engine {
	if cfg == nil {
		cfg = &Config{}
	}

	if cfg.IncludeRootAccount {
		a.Catalog.MySQLDb.AddRootAccount()
	}

	ls := sql.NewLockSubsystem()

	emptyCtx := sql.NewEmptyContext()
	a.Catalog.RegisterFunction(emptyCtx, sql.FunctionN{
		Name: "version",
		Fn:   function.NewVersion(cfg.VersionPostfix),
	})
	a.Catalog.RegisterFunction(emptyCtx, function.GetLockingFuncs(ls)...)

	version := sql.VersionStable
	if ExperimentalGMS {
		version = sql.VersionExperimental
	}
	return &Engine{
		Analyzer:          a,
		MemoryManager:     sql.NewMemoryManager(sql.ProcessMemory),
		ProcessList:       NewProcessList(),
		LS:                ls,
		BackgroundThreads: sql.NewBackgroundThreads(),
		IsReadOnly:        cfg.IsReadOnly,
		IsServerLocked:    cfg.IsServerLocked,
		PreparedDataCache: NewPreparedDataCache(),
		mu:                &sync.Mutex{},
		Version:           version,
	}
}

// NewDefault creates a new default Engine.
func NewDefault(pro sql.DatabaseProvider) *Engine {
	version := sql.VersionStable
	if ExperimentalGMS {
		version = sql.VersionExperimental
	}
	a := analyzer.NewDefault(pro, version)
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

	e.PreparedDataCache.CacheStmt(ctx.Session.ID(), query, node)
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
func (e *Engine) QueryNodeWithBindings(ctx *sql.Context, query string, parsed sql.Node, bindings map[string]sql.Expression) (sql.Schema, sql.RowIter, error) {
	var (
		analyzed sql.Node
		iter     sql.RowIter
		err      error
	)

	if ctx.Version == sql.VersionUnknown {
		ctx.Version = e.Version
	}

	if parsed == nil {
		switch ctx.Version {
		case sql.VersionExperimental:
			parsed, err = planbuilder.Parse(ctx, e.Analyzer.Catalog, query)
			if err != nil {
				ctx.Version = sql.VersionStable
				parsed, err = parse.Parse(ctx, query)
			}
		default:
			parsed, err = parse.Parse(ctx, query)
		}
		if err != nil {
			return nil, nil, err
		}
	}

	// Before we begin a transaction, we need to know if the database being operated on is not the one
	// currently selected
	transactionDatabase := analyzer.GetTransactionDatabase(ctx, parsed)

	// Give the integrator a chance to reject the session before proceeding
	err = ctx.Session.ValidateSession(ctx, transactionDatabase)
	if err != nil {
		return nil, nil, err
	}

	err = e.readOnlyCheck(parsed)
	if err != nil {
		return nil, nil, err
	}

	err = e.beginTransaction(ctx, transactionDatabase)
	if err != nil {
		return nil, nil, err
	}

	if p, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), query); ok {
		analyzed, err = e.analyzePreparedQuery(ctx, query, p, bindings)
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

	iter, err = e.Analyzer.ExecBuilder.Build(ctx, analyzed, nil)
	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			err = errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}

		return nil, nil, err
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

// CloseSession deletes session specific prepared statement data
func (e *Engine) CloseSession(connID uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.PreparedDataCache.DeleteSessionData(connID)
}

// Count number of BindVars in given tree
func countBindVars(node sql.Node) int {
	bindCnt := 0
	bindCntFunc := func(e sql.Expression) bool {
		if _, ok := e.(*expression.BindVar); ok {
			bindCnt++
		}
		return true
	}
	transform.InspectExpressions(node, bindCntFunc)

	// InsertInto.Source not a child of InsertInto, so also need to traverse those
	transform.Inspect(node, func(n sql.Node) bool {
		if in, ok := n.(*plan.InsertInto); ok {
			transform.InspectExpressions(in.Source, bindCntFunc)
			return false
		}
		return true
	})
	return bindCnt
}

func (e *Engine) analyzeQuery(ctx *sql.Context, query string, parsed sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	var (
		analyzed sql.Node
		err      error
	)

	// TODO: eventually, we should have this logic be in the RowIter() of the respective plans
	// along with a new rule that handles analysis
	switch n := parsed.(type) {
	case *plan.PrepareQuery:
		analyzedChild, err := e.Analyzer.PrepareQuery(ctx, n.Child, nil)
		if err != nil {
			return nil, err
		}
		e.PreparedDataCache.CacheStmt(ctx.Session.ID(), n.Name, analyzedChild)
		return parsed, nil
	case *plan.ExecuteQuery:
		// replace execute query node with the one prepared
		p, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), n.Name)
		if !ok {
			return nil, sql.ErrUnknownPreparedStatement.New(n.Name)
		}

		// number of BindVars provided must match number of BindVars expected
		if countBindVars(p) != len(n.BindVars) {
			return nil, sql.ErrInvalidArgument.New(n.Name)
		}
		parsed = p

		bindings = map[string]sql.Expression{}
		for i, binding := range n.BindVars {
			varName := fmt.Sprintf("v%d", i+1)
			bindings[varName] = binding
		}

		if len(bindings) > 0 {
			var usedBindings map[string]bool
			parsed, usedBindings, err = plan.ApplyBindings(parsed, bindings)
			if err != nil {
				return nil, err
			}
			for binding := range bindings {
				if !usedBindings[binding] && !plan.HasEmptyTable(analyzed) {
					return nil, fmt.Errorf("unused binding %s", binding)
				}
			}
		}

		analyzed, _, err = e.Analyzer.AnalyzePrepared(ctx, parsed, nil)
		if err != nil {
			return nil, err
		}
		return analyzed, nil
	case *plan.DeallocateQuery:
		if _, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), n.Name); !ok {
			return nil, sql.ErrUnknownPreparedStatement.New(n.Name)
		}
		e.PreparedDataCache.UncacheStmt(ctx.Session.ID(), n.Name)
		return parsed, nil
	}

	if len(bindings) > 0 {
		var usedBindings map[string]bool
		parsed, usedBindings, err = plan.ApplyBindings(parsed, bindings)
		if err != nil {
			return nil, err
		}
		for binding := range bindings {
			if !usedBindings[binding] && !plan.HasEmptyTable(analyzed) {
				return nil, fmt.Errorf("unused binding %s", binding)
			}
		}
	}

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}

	return analyzed, nil
}

func (e *Engine) analyzePreparedQuery(ctx *sql.Context, query string, analyzed sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	ctx.GetLogger().Tracef("optimizing prepared plan for query: %s", query)

	analyzed, err := analyzer.DeepCopyNode(analyzed)
	if err != nil {
		return nil, err
	}

	if len(bindings) > 0 {
		var usedBindings map[string]bool
		analyzed, usedBindings, err = plan.ApplyBindings(analyzed, bindings)
		if err != nil {
			return nil, err
		}
		for binding := range bindings {
			if !usedBindings[binding] && !plan.HasEmptyTable(analyzed) {
				return nil, fmt.Errorf("unused binding %s", binding)
			}
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

func (e *Engine) beginTransaction(ctx *sql.Context, transactionDatabase string) error {
	beginNewTransaction := ctx.GetTransaction() == nil || plan.ReadCommitted(ctx)
	if beginNewTransaction {
		ctx.GetLogger().Tracef("beginning new transaction")
		if len(transactionDatabase) > 0 {
			_, err := e.Analyzer.Catalog.Database(ctx, transactionDatabase)
			// if the database doesn't exist, just don't start a transaction on it, let other layers complain
			if sql.ErrDatabaseNotFound.Is(err) || sql.ErrDatabaseAccessDeniedForUser.Is(err) {
				ctx.GetLogger().Tracef("not starting transaction because of database not found for %s", transactionDatabase)
				return nil
			} else if err != nil {
				return err
			}

			ctx.SetTransactionDatabase(transactionDatabase)
		}

		ts, ok := ctx.Session.(sql.TransactionSession)
		if ok {
			tx, err := ts.StartTransaction(ctx, sql.ReadWrite)
			if err != nil {
				return err
			}

			ctx.SetTransaction(tx)
		}
	}

	return nil
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
	if plan.IsDDLNode(node) {
		if e.IsReadOnly {
			return sql.ErrReadOnly.New()
		} else if e.IsServerLocked {
			return sql.ErrDatabaseWriteLocked.New()
		}
	}
	switch node.(type) {
	case
			*plan.DeleteFrom, *plan.InsertInto, *plan.Update, *plan.LockTables, *plan.UnlockTables:
		if e.IsReadOnly {
			return sql.ErrReadOnly.New()
		} else if e.IsServerLocked {
			return sql.ErrDatabaseWriteLocked.New()
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
	e := NewDefault(memory.NewDBProvider(db))
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
