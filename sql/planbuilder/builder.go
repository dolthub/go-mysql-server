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

package planbuilder

import (
	"strings"
	"sync"

	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var BinderFactory = &sync.Pool{New: func() interface{} {
	return &Builder{f: &factory{}}
}}

type Builder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	parserOpts      ast.ParserOptions
	f               *factory
	currentDatabase sql.Database
	colId           columnId
	tabId           tableId
	multiDDL        bool
	viewCtx         *ViewContext
	procCtx         *ProcContext
	triggerCtx      *TriggerContext
	bindCtx         *BindvarContext
	insertActive    bool
	nesting         int
}

// BindvarContext holds bind variable replacement literals.
type BindvarContext struct {
	Bindings map[string]*querypb.BindVariable
	used     map[string]struct{}
	// resolveOnly indicates that we are resolving plan names,
	// but will not error for missing bindvar replacements.
	resolveOnly bool
}

func (bv *BindvarContext) GetSubstitute(s string) (*querypb.BindVariable, bool) {
	if bv.Bindings != nil {
		ret, ok := bv.Bindings[s]
		bv.used[s] = struct{}{}
		return ret, ok
	}
	return nil, false
}

func (bv *BindvarContext) UnusedBindings() []string {
	if len(bv.used) == len(bv.Bindings) {
		return nil
	}
	var unused []string
	for k, _ := range bv.Bindings {
		if _, ok := bv.used[k]; !ok {
			unused = append(unused, k)
		}
	}
	return unused
}

// ViewContext overwrites database root source of nested
// calls.
type ViewContext struct {
	AsOf   interface{}
	DbName string
}

type TriggerContext struct {
	Active           bool
	Call             bool
	UnresolvedTables []string
	ResolveErr       error
}

// ProcContext allows nested CALLs to use the same database for resolving
// procedure definitions without changing the underlying database roots.
type ProcContext struct {
	AsOf   interface{}
	DbName string
}

func New(ctx *sql.Context, cat sql.Catalog) *Builder {
	sqlMode := sql.LoadSqlMode(ctx)
	return &Builder{ctx: ctx, cat: cat, parserOpts: sqlMode.ParserOptions(), f: &factory{}}
}

func (b *Builder) Initialize(ctx *sql.Context, cat sql.Catalog, opts ast.ParserOptions) {
	b.ctx = ctx
	b.cat = cat
	b.f.ctx = ctx
	b.parserOpts = opts
}

func (b *Builder) SetDebug(val bool) {
	b.f.debug = val
}

func (b *Builder) SetBindings(bindings map[string]*querypb.BindVariable) {
	b.bindCtx = &BindvarContext{
		Bindings: bindings,
		used:     make(map[string]struct{}),
	}
}

func (b *Builder) SetParserOptions(opts ast.ParserOptions) {
	b.parserOpts = opts
}

func (b *Builder) BindCtx() *BindvarContext {
	return b.bindCtx
}

func (b *Builder) ViewCtx() *ViewContext {
	if b.viewCtx == nil {
		b.viewCtx = &ViewContext{}
	}
	return b.viewCtx
}

func (b *Builder) ProcCtx() *ProcContext {
	if b.procCtx == nil {
		b.procCtx = &ProcContext{}
	}
	return b.procCtx
}

func (b *Builder) TriggerCtx() *TriggerContext {
	if b.triggerCtx == nil {
		b.triggerCtx = &TriggerContext{}
	}
	return b.triggerCtx
}

func (b *Builder) newScope() *scope {
	return &scope{b: b}
}

func (b *Builder) Reset() {
	b.colId = 0
	b.bindCtx = nil
	b.currentDatabase = nil
	b.procCtx = nil
	b.multiDDL = false
	b.insertActive = false
	b.tabId = 0
	b.triggerCtx = nil
	b.viewCtx = nil
	b.nesting = 0
}

type parseErr struct {
	err error
}

func (b *Builder) handleErr(err error) {
	panic(parseErr{err})
}

func (b *Builder) build(inScope *scope, stmt ast.Statement, query string) (outScope *scope) {
	if inScope == nil {
		inScope = b.newScope()
	}
	switch n := stmt.(type) {
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(n)))
	case ast.SelectStatement:
		outScope = b.buildSelectStmt(inScope, n)
		if into := n.GetInto(); into != nil {
			b.buildInto(outScope, into)
		}
		return outScope
	case *ast.Analyze:
		return b.buildAnalyze(inScope, n, query)
	case *ast.CreateSpatialRefSys:
		return b.buildCreateSpatialRefSys(inScope, n)
	case *ast.Show:
		// When a query is empty it means it comes from a subquery, as we don't
		// have the query itself in a subquery. Hence, a SHOW could not be
		// parsed.
		if query == "" {
			b.handleErr(sql.ErrUnsupportedFeature.New("SHOW in subquery"))
		}
		return b.buildShow(inScope, n, query)
	case *ast.DDL:
		return b.buildDDL(inScope, query, n)
	case *ast.AlterTable:
		return b.buildAlterTable(inScope, query, n)
	case *ast.DBDDL:
		return b.buildDBDDL(inScope, n)
	case *ast.Explain:
		return b.buildExplain(inScope, n)
	case *ast.Insert:
		if n.With != nil {
			cteScope := b.buildWith(inScope, n.With)
			return b.buildInsert(cteScope, n)
		}
		return b.buildInsert(inScope, n)
	case *ast.Delete:
		if n.With != nil {
			cteScope := b.buildWith(inScope, n.With)
			return b.buildDelete(cteScope, n)
		}
		return b.buildDelete(inScope, n)
	case *ast.Update:
		if n.With != nil {
			cteScope := b.buildWith(inScope, n.With)
			return b.buildUpdate(cteScope, n)
		}
		return b.buildUpdate(inScope, n)
	case *ast.Load:
		return b.buildLoad(inScope, n)
	case *ast.Set:
		return b.buildSet(inScope, n)
	case *ast.Use:
		return b.buildUse(inScope, n)
	case *ast.Begin:
		outScope = inScope.push()
		transChar := sql.ReadWrite
		if n.TransactionCharacteristic == ast.TxReadOnly {
			transChar = sql.ReadOnly
		}

		outScope.node = plan.NewStartTransaction(transChar)
	case *ast.Commit:
		outScope = inScope.push()
		outScope.node = plan.NewCommit()
	case *ast.Rollback:
		outScope = inScope.push()
		outScope.node = plan.NewRollback()
	case *ast.Savepoint:
		outScope = inScope.push()
		outScope.node = plan.NewCreateSavepoint(n.Identifier)
	case *ast.RollbackSavepoint:
		outScope = inScope.push()
		outScope.node = plan.NewRollbackSavepoint(n.Identifier)
	case *ast.ReleaseSavepoint:
		outScope = inScope.push()
		outScope.node = plan.NewReleaseSavepoint(n.Identifier)
	case *ast.ChangeReplicationSource:
		return b.buildChangeReplicationSource(inScope, n)
	case *ast.ChangeReplicationFilter:
		return b.buildChangeReplicationFilter(inScope, n)
	case *ast.StartReplica:
		outScope = inScope.push()
		startRep := plan.NewStartReplica()
		if binCat, ok := b.cat.(binlogreplication.BinlogReplicaCatalog); ok && binCat.IsBinlogReplicaCatalog() {
			startRep.ReplicaController = binCat.GetBinlogReplicaController()
		}
		outScope.node = startRep
	case *ast.StopReplica:
		outScope = inScope.push()
		stopRep := plan.NewStopReplica()
		if binCat, ok := b.cat.(binlogreplication.BinlogReplicaCatalog); ok && binCat.IsBinlogReplicaCatalog() {
			stopRep.ReplicaController = binCat.GetBinlogReplicaController()
		}
		outScope.node = stopRep
	case *ast.ResetReplica:
		outScope = inScope.push()
		resetRep := plan.NewResetReplica(n.All)
		if binCat, ok := b.cat.(binlogreplication.BinlogReplicaCatalog); ok && binCat.IsBinlogReplicaCatalog() {
			resetRep.ReplicaController = binCat.GetBinlogReplicaController()
		}
		outScope.node = resetRep
	case *ast.BeginEndBlock:
		return b.buildBeginEndBlock(inScope, n)
	case *ast.IfStatement:
		return b.buildIfBlock(inScope, n)
	case *ast.CaseStatement:
		return b.buildCaseStatement(inScope, n)
	case *ast.Call:
		return b.buildCall(inScope, n)
	case *ast.Declare:
		return b.buildDeclare(inScope, n, query)
	case *ast.FetchCursor:
		return b.buildFetchCursor(inScope, n)
	case *ast.OpenCursor:
		return b.buildOpenCursor(inScope, n)
	case *ast.CloseCursor:
		return b.buildCloseCursor(inScope, n)
	case *ast.Loop:
		return b.buildLoop(inScope, n)
	case *ast.Repeat:
		return b.buildRepeat(inScope, n)
	case *ast.While:
		return b.buildWhile(inScope, n)
	case *ast.Leave:
		return b.buildLeave(inScope, n)
	case *ast.Iterate:
		return b.buildIterate(inScope, n)
	case *ast.Kill:
		return b.buildKill(inScope, n)
	case *ast.Signal:
		return b.buildSignal(inScope, n)
	case *ast.LockTables:
		return b.buildLockTables(inScope, n)
	case *ast.UnlockTables:
		return b.buildUnlockTables(inScope, n)
	case *ast.CreateUser:
		return b.buildCreateUser(inScope, n)
	case *ast.RenameUser:
		return b.buildRenameUser(inScope, n)
	case *ast.DropUser:
		return b.buildDropUser(inScope, n)
	case *ast.CreateRole:
		return b.buildCreateRole(inScope, n)
	case *ast.DropRole:
		return b.buildDropRole(inScope, n)
	case *ast.GrantPrivilege:
		return b.buildGrantPrivilege(inScope, n)
	case *ast.GrantRole:
		return b.buildGrantRole(inScope, n)
	case *ast.GrantProxy:
		return b.buildGrantProxy(inScope, n)
	case *ast.RevokePrivilege:
		return b.buildRevokePrivilege(inScope, n)
	case *ast.RevokeAllPrivileges:
		return b.buildRevokeAllPrivileges(inScope, n)
	case *ast.RevokeRole:
		return b.buildRevokeRole(inScope, n)
	case *ast.RevokeProxy:
		return b.buildRevokeProxy(inScope, n)
	case *ast.ShowGrants:
		return b.buildShowGrants(inScope, n)
	case *ast.ShowPrivileges:
		return b.buildShowPrivileges(inScope, n)
	case *ast.Flush:
		return b.buildFlush(inScope, n)
	case *ast.Prepare:
		return b.buildPrepare(inScope, n)
	case *ast.Execute:
		return b.buildExecute(inScope, n)
	case *ast.Deallocate:
		return b.buildDeallocate(inScope, n)
	}
	return
}

// buildVirtualTableScan returns a ProjectNode for a table that has virtual columns, projecting the values of any
// generated columns
func (b *Builder) buildVirtualTableScan(db string, tab sql.Table) *plan.VirtualColumnTable {
	tableScope := b.newScope()
	schema := tab.Schema()

	for _, c := range schema {
		tableScope.newColumn(scopeColumn{
			tableId:     sql.NewTableID(db, tab.Name()),
			col:         strings.ToLower(c.Name),
			originalCol: c.Name,
			typ:         c.Type,
			nullable:    c.Nullable,
		})
	}

	projections := make([]sql.Expression, len(schema))
	for i, c := range schema {
		if !c.Virtual {
			projections[i] = expression.NewGetFieldWithTable(i, c.Type, c.DatabaseSource, tab.Name(), c.Name, c.Nullable)
		} else {
			projections[i] = b.resolveColumnDefaultExpression(tableScope, c, c.Generated)
		}
	}

	// Unlike other kinds of nodes, the projection on this table wrapper is invisible to the analyzer, so we need to
	// get the column indexes correct here, they won't be fixed later like other kinds of expressions.
	for i, p := range projections {
		projections[i] = assignColumnIndexes(p, schema)
	}

	return plan.NewVirtualColumnTable(tab, projections)
}

// assignColumnIndexes fixes the column indexes in the expression to match the schema given
func assignColumnIndexes(e sql.Expression, schema sql.Schema) sql.Expression {
	e, _, _ = transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if gf, ok := e.(*expression.GetField); ok {
			idx := schema.IndexOfColName(gf.Name())
			return gf.WithIndex(idx), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	return e
}
