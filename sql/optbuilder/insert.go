package optbuilder

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildInsert(inScope *scope, i *sqlparser.Insert) (outScope *scope) {
	dbName := i.Table.Qualifier.String()
	tabName := i.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
		b.handleErr(err)
	}

	onDupExprs := b.assignmentExprsToExpressions(outScope, sqlparser.AssignmentExprs(i.OnDup))

	isReplace := i.Action == sqlparser.ReplaceStr

	src := b.insertRowsToNode(inScope, i.Rows)

	ignore := false
	// TODO: make this a bool in vitess
	if strings.Contains(strings.ToLower(i.Ignore), "ignore") {
		ignore = true
	}

	var columns []string
	{
		columns = columnsToStrings(i.Columns)
		// If no column names were specified in the query, go ahead and fill
		// them all in now that the destination is resolved.
		// TODO: setting the plan field directly is not great
		if len(columns) == 0 {
			schema := table.Schema()
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}
	}

	ins := plan.NewInsertInto(table.Database, table, src.node, isReplace, columns, onDupExprs, ignore)

	outScope.node = ins

	return
}

func (b *PlanBuilder) insertRowsToNode(inScope *scope, ir sqlparser.InsertRows) (outScope *scope) {
	switch v := ir.(type) {
	case sqlparser.SelectStatement:
		return b.buildSelectStmt(inScope, v)
	case sqlparser.Values:
		return b.buildValues(inScope, v)
	default:
		err := sql.ErrUnsupportedSyntax.New(sqlparser.String(ir))
		b.handleErr(err)
	}
	return
}

func (b *PlanBuilder) buildValues(inScope *scope, v sqlparser.Values) (outScope *scope) {
	// TODO add literals to outScope?
	exprTuples := make([][]sql.Expression, len(v))
	for i, vt := range v {
		exprs := make([]sql.Expression, len(vt))
		exprTuples[i] = exprs
		for j, e := range vt {
			exprs[j] = b.buildScalar(inScope, e)
		}
	}

	outScope = inScope.push()
	outScope.node = plan.NewValues(exprTuples)
	return
}

func (b *PlanBuilder) assignmentExprsToExpressions(inScope *scope, e sqlparser.AssignmentExprs) []sql.Expression {
	res := make([]sql.Expression, len(e))
	for i, updateExpr := range e {
		colName := b.buildScalar(inScope, updateExpr.Name)
		innerExpr := b.buildScalar(inScope, updateExpr.Expr)
		res[i] = expression.NewSetField(colName, innerExpr)
	}
	return res
}

func (b *PlanBuilder) Delete(inScope *scope, d *sqlparser.Delete) (outScope *scope) {
	outScope = b.buildFrom(inScope, d.TableExprs)
	b.buildWhere(outScope, d.Where)
	orderByScope := b.analyzeOrderBy(outScope, nil, d.OrderBy)
	b.buildOrderBy(outScope, orderByScope)
	limit := b.buildLimit(outScope, d.Limit)
	if limit != nil {
		outScope.node = plan.NewLimit(limit, outScope.node)
	}

	var targets []sql.Node
	if len(d.Targets) > 0 {
		targets = make([]sql.Node, len(d.Targets))
		for i, tableName := range d.Targets {
			tabName := tableName.Name.String()
			dbName := tableName.Qualifier.String()
			if dbName == "" {
				dbName = b.ctx.GetCurrentDatabase()
			}
			tableScope := b.buildTablescan(inScope, dbName, tabName, nil)
			targets[i] = tableScope.node
		}
	}

	del := plan.NewDeleteFrom(outScope.node, targets)
	outScope.node = del

	//if d.With != nil {
	//	node, err = ctesToWith(ctx, d.With, node)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	return
}

func (b *PlanBuilder) buildUpdate(inScope *scope, u *sqlparser.Update) (outScope *scope) {
	outScope = b.buildFrom(inScope, u.TableExprs)
	updateExprs := b.assignmentExprsToExpressions(outScope, u.Exprs)

	b.buildWhere(outScope, u.Where)

	orderByScope := b.analyzeOrderBy(outScope, nil, u.OrderBy)

	b.buildOrderBy(outScope, orderByScope)
	limit := b.buildLimit(outScope, u.Limit)
	if limit != nil {
		outScope.node = plan.NewLimit(limit, outScope.node)
	}

	// TODO comments
	// If the top level node can store comments and one was provided, store it.
	//if cn, ok := node.(sql.CommentedNode); ok && len(u.Comments) > 0 {
	//	node = cn.WithComment(string(u.Comments[0]))
	//}

	ignore := u.Ignore != ""

	update := plan.NewUpdate(outScope.node, ignore, updateExprs)
	outScope.node = update
	return
}
