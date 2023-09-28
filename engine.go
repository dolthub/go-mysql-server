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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/pkg/errors"

	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	data map[uint32]map[string]sqlparser.Statement
	mu   *sync.Mutex
}

func NewPreparedDataCache() *PreparedDataCache {
	return &PreparedDataCache{
		data: make(map[uint32]map[string]sqlparser.Statement),
		mu:   &sync.Mutex{},
	}
}

// GetCachedStmt will retrieve the prepared sql.Node associated with the ctx.SessionId and query if it exists
// it will return nil, false if the query does not exist
func (p *PreparedDataCache) GetCachedStmt(sessId uint32, query string) (sqlparser.Statement, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sessData, ok := p.data[sessId]; ok {
		data, ok := sessData[query]
		return data, ok
	}
	return nil, false
}

// GetSessionData returns all the prepared queries for a particular session
func (p *PreparedDataCache) GetSessionData(sessId uint32) map[string]sqlparser.Statement {
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
func (p *PreparedDataCache) CacheStmt(sessId uint32, query string, stmt sqlparser.Statement) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.data[sessId]; !ok {
		p.data[sessId] = make(map[string]sqlparser.Statement)
	}
	p.data[sessId][query] = stmt
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
	ReadOnly          atomic.Bool
	IsServerLocked    bool
	PreparedDataCache *PreparedDataCache
	mu                *sync.Mutex
	Version           sql.AnalyzerVersion
	EventScheduler    *eventscheduler.EventScheduler
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

	ret := &Engine{
		Analyzer:          a,
		MemoryManager:     sql.NewMemoryManager(sql.ProcessMemory),
		ProcessList:       NewProcessList(),
		LS:                ls,
		BackgroundThreads: sql.NewBackgroundThreads(),
		IsServerLocked:    cfg.IsServerLocked,
		PreparedDataCache: NewPreparedDataCache(),
		mu:                &sync.Mutex{},
		EventScheduler:    nil,
	}
	ret.ReadOnly.Store(cfg.IsReadOnly)
	return ret
}

// NewDefault creates a new default Engine.
func NewDefault(pro sql.DatabaseProvider) *Engine {
	a := analyzer.NewDefaultWithVersion(pro)
	return New(a, nil)
}

// AnalyzeQuery analyzes a query and returns its sql.Node
func (e *Engine) AnalyzeQuery(
	ctx *sql.Context,
	query string,
) (sql.Node, error) {
	parsed, err := planbuilder.Parse(ctx, e.Analyzer.Catalog, query)
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
	sqlMode := sql.LoadSqlMode(ctx)

	stmt, _, err := sqlparser.ParseOneWithOptions(query, sqlMode.ParserOptions())
	if err != nil {
		return nil, err
	}

	binder := planbuilder.New(ctx, e.Analyzer.Catalog)
	node, err := binder.BindOnly(stmt, query)

	if err != nil {
		return nil, err
	}

	e.PreparedDataCache.CacheStmt(ctx.Session.ID(), query, stmt)
	return node, nil
}

// Query executes a query.
func (e *Engine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return e.QueryWithBindings(ctx, query, nil, nil)
}

func bindingsToExprs(bindings map[string]*querypb.BindVariable) (map[string]sql.Expression, error) {
	res := make(map[string]sql.Expression, len(bindings))
	for k, v := range bindings {
		v, err := sqltypes.NewValue(v.Type, v.Value)
		if err != nil {
			return nil, err
		}
		switch {
		case v.Type() == sqltypes.Year:
			v, _, err := types.Year.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, types.Year)
		case sqltypes.IsSigned(v.Type()):
			v, err := strconv.ParseInt(string(v.ToBytes()), 0, 64)
			if err != nil {
				return nil, err
			}
			t := types.Int64
			c, _, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case sqltypes.IsUnsigned(v.Type()):
			v, err := strconv.ParseUint(string(v.ToBytes()), 0, 64)
			if err != nil {
				return nil, err
			}
			t := types.Uint64
			c, _, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case sqltypes.IsFloat(v.Type()):
			v, err := strconv.ParseFloat(string(v.ToBytes()), 64)
			if err != nil {
				return nil, err
			}
			t := types.Float64
			c, _, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case v.Type() == sqltypes.Decimal:
			v, _, err := types.InternalDecimalType.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, types.InternalDecimalType)
		case v.Type() == sqltypes.Bit:
			t := types.MustCreateBitType(types.BitTypeMaxBits)
			v, _, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Null:
			res[k] = expression.NewLiteral(nil, types.Null)
		case v.Type() == sqltypes.Blob || v.Type() == sqltypes.VarBinary || v.Type() == sqltypes.Binary:
			t, err := types.CreateBinary(v.Type(), int64(len(v.ToBytes())))
			if err != nil {
				return nil, err
			}
			v, _, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Text || v.Type() == sqltypes.VarChar || v.Type() == sqltypes.Char:
			t, err := types.CreateStringWithDefaults(v.Type(), int64(len(v.ToBytes())))
			if err != nil {
				return nil, err
			}
			v, _, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Date || v.Type() == sqltypes.Datetime || v.Type() == sqltypes.Timestamp:
			precision := 6
			if v.Type() == sqltypes.Date {
				precision = 0
			}
			t, err := types.CreateDatetimeType(v.Type(), precision)
			if err != nil {
				return nil, err
			}
			v, _, err := t.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Time:
			t := types.Time
			v, _, err := t.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		default:
			return nil, sql.ErrUnsupportedFeature.New(v.Type().String())
		}
	}
	return res, nil
}

// QueryWithBindings executes the query given with the bindings provided. If parsed is non-nil, it will be used
// instead of parsing the query from text.
func (e *Engine) QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]*querypb.BindVariable) (sql.Schema, sql.RowIter, error) {
	var err error
	binder := planbuilder.New(ctx, e.Analyzer.Catalog)
	if prep, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), query); ok {
		parsed = prep
		binder.SetBindings(bindings)
	} else if len(bindings) > 0 {
		parsed = nil
		_, err := e.PrepareQuery(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		prep, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), query)
		if ok {
			parsed = prep
			binder.SetBindings(bindings)
		}
	}
	if err != nil {
		return nil, nil, err
	}

	// Give the integrator a chance to reject the session before proceeding
	err = ctx.Session.ValidateSession(ctx)
	if err != nil {
		return nil, nil, err
	}

	err = e.beginTransaction(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			return nil, nil, errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}
		return nil, nil, err
	}
	var bound sql.Node
	if parsed == nil {
		bound, err = binder.ParseOne(query)
		if err != nil {
			err2 := clearAutocommitTransaction(ctx)
			if err2 != nil {
				return nil, nil, errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
			}
			return nil, nil, err
		}
	} else {
		bound, err = binder.BindOnly(parsed, query)
		if err != nil {
			return nil, nil, err
		}
	}

	switch n := bound.(type) {
	case *plan.ExecuteQuery:
		prep, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), n.Name)
		if !ok {
			err := sql.ErrUnknownPreparedStatement.New(n.Name)
			return nil, nil, err
		}
		// todo validate expected and actual args -- not just count, by name
		//if prep.ArgCount() < 1 {
		//	return nil, nil, fmt.Errorf("invalid bind variable count: expected %d, found %d", prep.ArgCount(), len(bindings))
		//}
		for i, name := range n.BindVars {
			if bindings == nil {
				bindings = make(map[string]*querypb.BindVariable)
			}
			if strings.HasPrefix(name.String(), "@") {
				t, val, err := ctx.GetUserVariable(ctx, strings.TrimPrefix(name.String(), "@"))
				if err != nil {
					return nil, nil, err
				}
				if val != nil {
					val, _, err = t.Promote().Convert(val)
					if err != nil {
						return nil, nil, err
					}
				}
				bindings[fmt.Sprintf("v%d", i+1)], err = sqltypes.BuildBindVariable(val)
				if err != nil {
					return nil, nil, err
				}
			} else {
				bindings[fmt.Sprintf("v%d", i)] = sqltypes.StringBindVariable(name.String())
			}
		}
		binder.SetBindings(bindings)
		bound, err = binder.BindOnly(prep, query)
		if err != nil {
			err2 := clearAutocommitTransaction(ctx)
			if err2 != nil {
				return nil, nil, errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
			}

			return nil, nil, err
		}
	}

	// TODO: eventually, we should have this logic be in the RowIter() of the respective plans
	// along with a new rule that handles analysis
	var analyzed sql.Node
	switch n := bound.(type) {
	case *plan.PrepareQuery:
		sqlMode := sql.LoadSqlMode(ctx)

		// we have to name-resolve to check for structural errors, but we do
		// not to cache the name-bound query yet.
		//todo(max): improve name resolution so we can cache post name-binding.
		// this involves expression memoization, which currently screws up aggregation
		// and order by aliases
		prepStmt, _, err := sqlparser.ParseOneWithOptions(query, sqlMode.ParserOptions())
		if err != nil {
			return nil, nil, err
		}
		prepare, ok := prepStmt.(*sqlparser.Prepare)
		if !ok {
			return nil, nil, fmt.Errorf("expected PREPARE ast")
		}
		cacheStmt, _, err := sqlparser.ParseOneWithOptions(prepare.Expr, sqlMode.ParserOptions())
		if err != nil && strings.HasPrefix(prepare.Expr, "@") {
			val, err := expression.NewUserVar(strings.TrimPrefix(prepare.Expr, "@")).Eval(ctx, nil)
			if err != nil {
				return nil, nil, err
			}
			valStr, ok := val.(string)
			if !ok {
				return nil, nil, fmt.Errorf("invalid query for PREPARE: %s", val)
			}
			cacheStmt, _, err = sqlparser.ParseOneWithOptions(valStr, sqlMode.ParserOptions())
			if err != nil {
				return nil, nil, err
			}
		} else if err != nil {
			return nil, nil, err
		}
		e.PreparedDataCache.CacheStmt(ctx.Session.ID(), n.Name, cacheStmt)
		analyzed = bound
	case *plan.DeallocateQuery:
		if _, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), n.Name); !ok {
			return nil, nil, sql.ErrUnknownPreparedStatement.New(n.Name)
		}
		e.PreparedDataCache.UncacheStmt(ctx.Session.ID(), n.Name)
		analyzed = bound
	default:
		analyzed, err = e.analyzeQuery(ctx, query, bound)
		if err != nil {
			return nil, nil, err
		}
	}

	if bindCtx := binder.BindCtx(); bindCtx != nil {
		if unused := bindCtx.UnusedBindings(); len(unused) > 0 {
			return nil, nil, fmt.Errorf("invalid arguments. expected: %d, found: %d", len(bindCtx.Bindings)-len(unused), len(bindCtx.Bindings))
		}
	}

	err = e.readOnlyCheck(analyzed)
	if err != nil {
		return nil, nil, err
	}

	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			return nil, nil, errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}

		return nil, nil, err
	}

	iter, err := e.Analyzer.ExecBuilder.Build(ctx, analyzed, nil)
	if err != nil {
		err2 := clearAutocommitTransaction(ctx)
		if err2 != nil {
			return nil, nil, errors.Wrap(err, "unable to clear autocommit transaction: "+err2.Error())
		}

		return nil, nil, err
	}
	iter = rowexec.AddExpressionCloser(analyzed, iter)

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
	var bindVars map[string]bool
	bindCntFunc := func(e sql.Expression) bool {
		if bv, ok := e.(*expression.BindVar); ok {
			if bindVars == nil {
				bindVars = make(map[string]bool)
			}
			bindVars[bv.Name] = true
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
	return len(bindVars)
}

func (e *Engine) analyzeQuery(ctx *sql.Context, query string, parsed sql.Node) (sql.Node, error) {
	var (
		analyzed sql.Node
		err      error
	)

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}

	return analyzed, nil
}

func (e *Engine) beginTransaction(ctx *sql.Context) error {
	beginNewTransaction := ctx.GetTransaction() == nil || plan.ReadCommitted(ctx)
	if beginNewTransaction {
		ctx.GetLogger().Tracef("beginning new transaction")
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
	if e.EventScheduler != nil {
		e.EventScheduler.Close()
	}
	for _, p := range e.ProcessList.Processes() {
		e.ProcessList.Kill(p.Connection)
	}
	return e.BackgroundThreads.Shutdown()
}

func (e *Engine) WithBackgroundThreads(b *sql.BackgroundThreads) *Engine {
	e.BackgroundThreads = b
	return e
}

func (e *Engine) IsReadOnly() bool {
	return e.ReadOnly.Load()
}

// readOnlyCheck checks to see if the query is valid with the modification setting of the engine.
func (e *Engine) readOnlyCheck(node sql.Node) error {
	// Note: We only compute plan.IsReadOnly if the server is in one of
	// these two modes, since otherwise it is simply wasted work.
	if e.IsReadOnly() && !plan.IsReadOnly(node) {
		return sql.ErrReadOnly.New()
	}
	if e.IsServerLocked && !plan.IsReadOnly(node) {
		return sql.ErrDatabaseWriteLocked.New()
	}
	return nil
}

func (e *Engine) EnginePreparedDataCache() *PreparedDataCache {
	return e.PreparedDataCache
}

func (e *Engine) EngineAnalyzer() *analyzer.Analyzer {
	return e.Analyzer
}

// InitializeEventScheduler initializes the EventScheduler for the engine with the given sql.Context
// getter function, |ctxGetterFunc, the EventScheduler |status|, and the |period| for the event scheduler
// to check for events to execute. If |period| is less than 1, then it is ignored and the default period
// (30s currently) is used. This function also initializes the EventScheduler of the analyzer of this engine.
func (e *Engine) InitializeEventScheduler(ctxGetterFunc func() (*sql.Context, func() error, error), status eventscheduler.SchedulerStatus, period int) error {
	var err error
	e.EventScheduler, err = eventscheduler.InitEventScheduler(e.Analyzer, e.BackgroundThreads, ctxGetterFunc, status, e.executeEvent, period)
	if err != nil {
		return err
	}

	e.Analyzer.EventScheduler = e.EventScheduler
	return nil
}

// executeEvent executes an event with this Engine. The event is executed against the |dbName| database, and by the
// account identified by |username| and |address|. The entire CREATE EVENT statement is passed in as the |createEventStatement|
// parameter, but only the body of the event is executed. (The CREATE EVENT statement is passed in to support event
// bodies that contain multiple statements in a BEGIN/END block.) If any problems are encounterd, the error return
// value will be populated.
func (e *Engine) executeEvent(ctx *sql.Context, dbName, createEventStatement, username, address string) error {
	// the event must be executed against the correct database and with the definer's identity
	ctx.SetCurrentDatabase(dbName)
	ctx.Session.SetClient(sql.Client{User: username, Address: address})

	// Analyze the CREATE EVENT statement
	planTree, err := e.AnalyzeQuery(ctx, createEventStatement)
	if err != nil {
		return err
	}

	// and pull out the event body/definition
	createEventNode, err := findCreateEventNode(planTree)
	if err != nil {
		return err
	}
	definitionNode := createEventNode.DefinitionNode

	// Build an iterator to execute the event body
	iter, err := e.Analyzer.ExecBuilder.Build(ctx, definitionNode, nil)
	if err != nil {
		clearAutocommitErr := clearAutocommitTransaction(ctx)
		if clearAutocommitErr != nil {
			return clearAutocommitErr
		}
		return err
	}
	iter = rowexec.AddExpressionCloser(definitionNode, iter)

	// Drain the iterate to execute the event body/definition
	// NOTE: No row data is returned for an event; we just need to execute the statements
	_, err = sql.RowIterToRows(ctx, definitionNode.Schema(), iter)
	return err
}

// findCreateEventNode searches |planTree| for the first plan.CreateEvent node and
// returns it. If no matching node was found, the returned CreateEvent node will be
// nil and an error will be populated.
func findCreateEventNode(planTree sql.Node) (*plan.CreateEvent, error) {
	// Search through the node to find the first CREATE EVENT node, and then grab its body
	var targetNode sql.Node
	transform.Inspect(planTree, func(node sql.Node) bool {
		if cen, ok := node.(*plan.CreateEvent); ok {
			targetNode = cen
			return false
		}
		return true
	})

	if targetNode == nil {
		return nil, fmt.Errorf("unable to find create event node in plan tree: %v", planTree)
	}

	createEventNode, ok := targetNode.(*plan.CreateEvent)
	if !ok {
		return nil, fmt.Errorf("unable to find create event node in plan tree: %v", planTree)
	}

	return createEventNode, nil
}
