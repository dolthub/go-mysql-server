package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	oldparse "github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type PlanBuilder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	currentDatabase sql.Database
	colId           columnId
	tabId           tableId
	multiDDL        bool
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
		return b.buildSelectStmt(inScope, n)
		// todo: SELECT INTO
		//if into := n.GetInto(); into != nil {
		outScope = inScope.push()

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
	case *ast.AlterTable:
		return b.buildMultiAlterDDL(inScope, query, n)
	case *ast.DBDDL:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Explain:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Insert:
		return b.buildInsert(inScope, n)
	case *ast.Delete:
		return b.buildDelete(inScope, n)
	case *ast.Update:
		return b.buildUpdate(inScope, n)

	case *ast.Load:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
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
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.ChangeReplicationFilter:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
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
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.IfStatement:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CaseStatement:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Call:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Declare:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.FetchCursor:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.OpenCursor:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CloseCursor:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Loop:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Repeat:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.While:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Leave:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Iterate:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Kill:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Signal:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.LockTables:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.UnlockTables:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CreateUser:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.RenameUser:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.DropUser:
		outScope.node = plan.NewDropUser(n.IfExists, convertAccountName(n.AccountNames...))
	case *ast.CreateRole:
		outScope.node = plan.NewCreateRole(n.IfNotExists, convertAccountName(n.Roles...))
	case *ast.DropRole:
		outScope.node = plan.NewDropRole(n.IfExists, convertAccountName(n.Roles...))
	case *ast.GrantPrivilege:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
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
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.ShowPrivileges:
		outScope.node = plan.NewShowPrivileges()
	case *ast.Flush:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Prepare:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Execute:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Deallocate:
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	}
	return
}
