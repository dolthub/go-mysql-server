package optbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	oldparse "github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) reset() {
	b.colId = 0
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
		//if into := n.GetInto(); into != nil {
		outScope = inScope.push()
		//	node, err := oldparse.Parse(b.ctx, query

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
		//return convertDDL(ctx, query, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.MultiAlterDDL:
		//return convertMultiAlterDDL(ctx, query, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.DBDDL:
		//return convertDBDDL(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Explain:
		//return convertExplain(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Insert:
		//return convertInsert(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Delete:
		//return convertDelete(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Update:
		//return convertUpdate(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Load:
		//return convertLoad(ctx, n)
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
		transChar := sql.ReadWrite
		if n.TransactionCharacteristic == ast.TxReadOnly {
			transChar = sql.ReadOnly
		}

		outScope.node = plan.NewStartTransaction(transChar)
	case *ast.Commit:
		outScope.node = plan.NewCommit()
	case *ast.Rollback:
		outScope.node = plan.NewRollback()
	case *ast.Savepoint:
		outScope.node = plan.NewCreateSavepoint(n.Identifier)
	case *ast.RollbackSavepoint:
		outScope.node = plan.NewRollbackSavepoint(n.Identifier)
	case *ast.ReleaseSavepoint:
		outScope.node = plan.NewReleaseSavepoint(n.Identifier)
	case *ast.ChangeReplicationSource:
		//return convertChangeReplicationSource(n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.ChangeReplicationFilter:
		//return convertChangeReplicationFilter(n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.StartReplica:
		outScope.node = plan.NewStartReplica()
	case *ast.StopReplica:
		outScope.node = plan.NewStopReplica()
	case *ast.ResetReplica:
		outScope.node = plan.NewResetReplica(n.All)
	case *ast.BeginEndBlock:
		//return convertBeginEndBlock(ctx, n, query)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.IfStatement:
		//return convertIfBlock(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CaseStatement:
		//return convertCaseStatement(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Call:
		//return convertCall(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Declare:
		//return convertDeclare(ctx, n, query)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.FetchCursor:
		//return convertFetch(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.OpenCursor:
		//return convertOpen(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CloseCursor:
		//return convertClose(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Loop:
		//return convertLoop(ctx, n, query)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Repeat:
		//return convertRepeat(ctx, n, query)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.While:
		//return convertWhile(ctx, n, query)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Leave:
		//return convertLeave(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Iterate:
		//return convertIterate(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Kill:
		//return convertKill(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Signal:
		//return convertSignal(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.LockTables:
		//return convertLockTables(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.UnlockTables:
		//return convertUnlockTables(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.CreateUser:
		//return convertCreateUser(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.RenameUser:
		//return convertRenameUser(ctx, n)
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
		//return convertGrantPrivilege(ctx, n)
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
		//return convertShowGrants(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.ShowPrivileges:
		outScope.node = plan.NewShowPrivileges()
	case *ast.Flush:
		//return convertFlush(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Prepare:
		//return convertPrepare(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Execute:
		//return convertExecute(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	case *ast.Deallocate:
		//return convertDeallocate(ctx, n)
		outScope = inScope.push()
		node, err := oldparse.Parse(b.ctx, query)
		if err != nil {
			b.handleErr(err)
		}
		outScope.node = node
	}
	return
}

func intoToInto(ctx *sql.Context, into *ast.Into, node sql.Node) (sql.Node, error) {
	if into.Outfile != "" || into.Dumpfile != "" {
		return nil, sql.ErrUnsupportedSyntax.New("select into files is not supported yet")
	}

	vars := make([]sql.Expression, len(into.Variables))
	for i, val := range into.Variables {
		if strings.HasPrefix(val.String(), "@") {
			vars[i] = expression.NewUserVar(strings.TrimPrefix(val.String(), "@"))
		} else {
			vars[i] = expression.NewUnresolvedProcedureParam(val.String())
		}
	}
	return plan.NewInto(node, vars), nil
}
