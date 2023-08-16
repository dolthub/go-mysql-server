package planbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type Builder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	currentDatabase sql.Database
	colId           columnId
	tabId           tableId
	multiDDL        bool
	viewCtx         *ViewContext
	procCtx         *ProcContext
	triggerCtx      *TriggerContext
	insertActive    bool
	nesting         int
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
	return &Builder{ctx: ctx, cat: cat}
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

func (b *Builder) reset() {
	b.colId = 0
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
		outScope.node = plan.NewStartReplica()
	case *ast.StopReplica:
		outScope = inScope.push()
		outScope.node = plan.NewStopReplica()
	case *ast.ResetReplica:
		outScope = inScope.push()
		outScope.node = plan.NewResetReplica(n.All)
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
