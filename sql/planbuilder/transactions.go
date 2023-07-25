package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) buildUse(inScope *scope, n *ast.Use) (outScope *scope) {
	name := n.DBName.String()
	ret := plan.NewUse(b.resolveDb(name))
	ret.Catalog = b.cat
	outScope = inScope.push()
	outScope.node = ret
	return
}

func (b *Builder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	names := make([]sql.DbTable, len(n.Tables))
	defaultDb := b.ctx.GetCurrentDatabase()
	for i, table := range n.Tables {
		dbName := table.Qualifier.String()
		if dbName == "" {
			if defaultDb == "" {
				err := sql.ErrNoDatabaseSelected.New()
				b.handleErr(err)
			}
			dbName = defaultDb
		}
		names[i] = sql.DbTable{Db: dbName, Table: strings.ToLower(table.Name.String())}
	}
	analyze := plan.NewAnalyze(names)

	stats, err := b.cat.Statistics(b.ctx)
	if err != nil {
		b.handleErr(err)
	}

	outScope.node = analyze.WithDb(defaultDb).WithStats(stats)
	return
}

func (b *Builder) buildPrepare(inScope *scope, n *ast.Prepare) (outScope *scope) {
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

func (b *Builder) buildExecute(inScope *scope, n *ast.Execute) (outScope *scope) {
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

func (b *Builder) buildDeallocate(inScope *scope, n *ast.Deallocate) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewDeallocateQuery(n.Name)
	return outScope
}

func (b *Builder) buildLockTables(inScope *scope, s *ast.LockTables) (outScope *scope) {
	outScope = inScope.push()
	tables := make([]*plan.TableLock, len(s.Tables))

	for i, tbl := range s.Tables {
		tableScope := b.buildDataSource(inScope, tbl.Table)
		write := tbl.Lock == ast.LockWrite || tbl.Lock == ast.LockLowPriorityWrite

		// TODO: Allow for other types of locks (LOW PRIORITY WRITE & LOCAL READ)
		tables[i] = &plan.TableLock{Table: tableScope.node, Write: write}
	}

	lockTables := plan.NewLockTables(tables)
	lockTables.Catalog = b.cat
	outScope.node = lockTables
	return outScope
}

func (b *Builder) buildUnlockTables(inScope *scope, s *ast.UnlockTables) (outScope *scope) {
	outScope = inScope.push()
	unlockTables := plan.NewUnlockTables()
	unlockTables.Catalog = b.cat
	outScope.node = unlockTables
	return outScope
}
