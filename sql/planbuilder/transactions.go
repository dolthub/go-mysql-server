package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *PlanBuilder) buildbuildUse(inScope *scope, n *ast.Use) (outScope *scope) {
	name := n.DBName.String()
	ret := plan.NewUse(b.resolveDb(name))
	ret.Catalog = b.cat
	outScope = inScope.push()
	outScope.node = ret
	return
}

func (b *PlanBuilder) buildPrepare(inScope *scope, n *ast.Prepare) (outScope *scope) {
	outScope = inScope.push()
	expr := n.Expr
	if strings.HasPrefix(n.Expr, "@") {
		varName := strings.ToLower(strings.Trim(n.Expr, "@"))
		_, val, err := b.ctx.GetUserVariable(b.ctx, varName)
		if err != nil {
			b.handleErr(err)
		}
		strVal, _, err := types.LongText.Convert(val)
		if err != nil {
			b.handleErr(err)
		}
		if strVal == nil {
			expr = "NULL"
		} else {
			expr = strVal.(string)
		}
	}

	childStmt, err := ast.Parse(expr)
	if err != nil {
		b.handleErr(err)
	}

	childScope := b.build(inScope, childStmt, expr)

	outScope.node = plan.NewPrepareQuery(n.Name, childScope.node)
	return outScope
}

func (b *PlanBuilder) buildExecute(inScope *scope, n *ast.Execute) (outScope *scope) {
	outScope = inScope.push()
	exprs := make([]sql.Expression, len(n.VarList))
	for i, e := range n.VarList {
		if strings.HasPrefix(e, "@") {
			exprs[i] = expression.NewUserVar(strings.TrimPrefix(e, "@"))
		} else {
			exprs[i] = expression.NewUnresolvedProcedureParam(e)
		}
	}
	outScope.node = plan.NewExecuteQuery(n.Name, exprs...)
	return outScope
}

func (b *PlanBuilder) buildDeallocate(inScope *scope, n *ast.Deallocate) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewDeallocateQuery(n.Name)
	return outScope
}

func (b *PlanBuilder) buildUse(inScope *scope, n *ast.Use) (outScope *scope) {
	outScope = inScope.push()
	name := n.DBName.String()
	outScope.node = plan.NewUse(b.resolveDb(name))
	return outScope
}

func (b *PlanBuilder) buildLockTables(inScope *scope, s *ast.LockTables) (outScope *scope) {
	outScope = inScope.push()
	tables := make([]*plan.TableLock, len(s.Tables))

	for i, tbl := range s.Tables {
		tableScope := b.buildDataSource(inScope, tbl.Table)
		write := tbl.Lock == ast.LockWrite || tbl.Lock == ast.LockLowPriorityWrite

		// TODO: Allow for other types of locks (LOW PRIORITY WRITE & LOCAL READ)
		tables[i] = &plan.TableLock{Table: tableScope.node, Write: write}
	}

	outScope.node = plan.NewLockTables(tables)
	return outScope
}

func (b *PlanBuilder) buildUnlockTables(inScope *scope, s *ast.UnlockTables) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewUnlockTables()
	return outScope
}
