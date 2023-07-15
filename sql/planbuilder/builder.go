package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type PlanBuilder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	currentDatabase sql.Database
	colId           columnId
	tabId           tableId
	multiDDL        bool
	viewAsOf        interface{}
	viewDatabase    string
	nesting         int
}

func New(ctx *sql.Context, cat sql.Catalog) *PlanBuilder {
	return &PlanBuilder{ctx: ctx, cat: cat}
}

func (b *PlanBuilder) SetAsOf(asof interface{}) {
	b.viewAsOf = asof
}

func (b *PlanBuilder) AsOf() interface{} {
	return b.viewAsOf
}

func (b *PlanBuilder) newScope() *scope {
	return &scope{b: b}
}

func (b *PlanBuilder) reset() {
	b.colId = 0
}

type parseErr struct {
	err error
}

func (b *PlanBuilder) handleErr(err error) {
	panic(parseErr{err})
}

func (b *PlanBuilder) build(inScope *scope, stmt ast.Statement, query string) (outScope *scope) {
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
	case *ast.MultiAlterDDL:
		return b.buildMultiAlterDDL(inScope, query, n)
	case *ast.DBDDL:
		return b.buildDBDDL(inScope, n)
	case *ast.Explain:
		return b.buildExplain(inScope, n)
	case *ast.Insert:
		return b.buildInsert(inScope, n)
	case *ast.Delete:
		return b.buildDelete(inScope, n)
	case *ast.Update:
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
		outScope = inScope.push()
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
		outScope.node = plan.NewDropUser(n.IfExists, convertAccountName(n.AccountNames...))
	case *ast.CreateRole:
		outScope.node = plan.NewCreateRole(n.IfNotExists, convertAccountName(n.Roles...))
	case *ast.DropRole:
		outScope.node = plan.NewDropRole(n.IfExists, convertAccountName(n.Roles...))
	case *ast.GrantPrivilege:
		return b.buildGrantPrivilege(inScope, n)
	case *ast.GrantRole:
		outScope.node = plan.NewGrantRole(
			convertAccountName(n.Roles...),
			convertAccountName(n.To...),
			n.WithAdminOption,
		)
	case *ast.GrantProxy:
		outScope.node = plan.NewGrantProxy(
			convertAccountName(n.On)[0],
			convertAccountName(n.To...),
			n.WithGrantOption,
		)
	case *ast.RevokePrivilege:
		privs := convertPrivilege(n.Privileges...)
		objType := convertObjectType(n.ObjectType)
		level := convertPrivilegeLevel(n.PrivilegeLevel)
		users := convertAccountName(n.From...)
		revoker := b.ctx.Session.Client().User
		if strings.ToLower(level.Database) == sql.InformationSchemaDatabaseName {
			b.handleErr(sql.ErrDatabaseAccessDeniedForUser.New(revoker, level.Database))
		}
		outScope.node = &plan.Revoke{
			Privileges:     privs,
			ObjectType:     objType,
			PrivilegeLevel: level,
			Users:          users,
			MySQLDb:        sql.UnresolvedDatabase("mysql"),
		}
	case *ast.RevokeAllPrivileges:
		outScope.node = plan.NewRevokeAll(convertAccountName(n.From...))
	case *ast.RevokeRole:
		outScope.node = plan.NewRevokeRole(convertAccountName(n.Roles...), convertAccountName(n.From...))
	case *ast.RevokeProxy:
		outScope.node = plan.NewRevokeProxy(convertAccountName(n.On)[0], convertAccountName(n.From...))
	case *ast.ShowGrants:
		return b.buildShowGrants(inScope, n)
	case *ast.ShowPrivileges:
		outScope.node = plan.NewShowPrivileges()
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
