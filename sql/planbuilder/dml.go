package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func (b *PlanBuilder) buildInsert(inScope *scope, i *ast.Insert) (outScope *scope) {
	if i.With != nil {
		inScope = b.buildWith(inScope, i.With)
	}
	dbName := i.Table.Qualifier.String()
	tabName := i.Table.Name.String()
	destScope := b.buildTablescan(inScope, dbName, tabName, nil)
	var db sql.Database
	var rt *plan.ResolvedTable
	switch n := destScope.node.(type) {
	case *plan.ResolvedTable:
		rt = n
		db = rt.Database
	case *plan.UnresolvedTable:
		db = n.Database()
	default:
		b.handleErr(fmt.Errorf("expected insert destination to be resolved or unresolved table"))
	}
	var triggerRefsUnknownTable bool
	if rt == nil {
		if b.buildingTrigger {
			triggerRefsUnknownTable = true
		} else {
			err := fmt.Errorf("expected resolved table: %s", tabName)
			b.handleErr(err)
		}
	}
	isReplace := i.Action == ast.ReplaceStr

	srcScope := b.insertRowsToNode(inScope, i.Rows)

	combinedScope := inScope.replace()
	for _, c := range destScope.cols {
		combinedScope.newColumn(c)
	}
	for _, c := range srcScope.cols {
		combinedScope.newColumn(c)
	}
	onDupExprs := b.assignmentExprsToExpressions(combinedScope, ast.AssignmentExprs(i.OnDup))

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
		if len(columns) == 0 && len(srcScope.cols) > 0 && !triggerRefsUnknownTable {
			schema := rt.Schema()
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}
	}

	ins := plan.NewInsertInto(db, destScope.node, srcScope.node, isReplace, columns, onDupExprs, ignore)

	if !triggerRefsUnknownTable {
		checks := b.loadChecksFromTable(destScope, rt.Table)
		ins.Checks = checks
	}

	outScope = destScope
	outScope.node = ins

	return
}

func (b *PlanBuilder) insertRowsToNode(inScope *scope, ir ast.InsertRows) (outScope *scope) {
	switch v := ir.(type) {
	case ast.SelectStatement:
		return b.buildSelectStmt(inScope, v)
	case ast.Values:
		return b.buildValues(inScope, v)
	default:
		err := sql.ErrUnsupportedSyntax.New(ast.String(ir))
		b.handleErr(err)
	}
	return
}

func (b *PlanBuilder) buildValues(inScope *scope, v ast.Values) (outScope *scope) {
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

func (b *PlanBuilder) assignmentExprsToExpressions(inScope *scope, e ast.AssignmentExprs) []sql.Expression {
	res := make([]sql.Expression, len(e))
	for i, updateExpr := range e {
		colName := b.buildScalar(inScope, updateExpr.Name)
		innerExpr := b.buildScalar(inScope, updateExpr.Expr)
		res[i] = expression.NewSetField(colName, innerExpr)
	}
	return res
}

func (b *PlanBuilder) buildDelete(inScope *scope, d *ast.Delete) (outScope *scope) {
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
	return
}

func (b *PlanBuilder) buildUpdate(inScope *scope, u *ast.Update) (outScope *scope) {
	outScope = b.buildFrom(inScope, u.TableExprs)

	var checks []*sql.CheckConstraint
	transform.Inspect(outScope.node, func(n sql.Node) bool {
		// todo maybe this should be later stage
		if rt, ok := n.(*plan.ResolvedTable); ok {
			checks = append(checks, b.loadChecksFromTable(inScope, rt.Table)...)
		}
		return true
	})

	updateExprs := b.assignmentExprsToExpressions(outScope, u.Exprs)

	b.buildWhere(outScope, u.Where)

	orderByScope := b.analyzeOrderBy(outScope, b.newScope(), u.OrderBy)

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
	update.Checks = checks
	outScope.node = update
	return
}

func (b *PlanBuilder) buildInto(inScope *scope, into *ast.Into) {
	if into.Outfile != "" || into.Dumpfile != "" {
		err := sql.ErrUnsupportedSyntax.New("select into files is not supported yet")
		b.handleErr(err)
	}

	vars := make([]sql.Expression, len(into.Variables))
	for i, val := range into.Variables {
		if strings.HasPrefix(val.String(), "@") {
			vars[i] = expression.NewUserVar(strings.TrimPrefix(val.String(), "@"))
		} else {
			vars[i] = expression.NewUnresolvedProcedureParam(val.String())
		}
	}
	inScope.node = plan.NewInto(inScope.node, vars)
}

func (b *PlanBuilder) loadChecksFromTable(inScope *scope, table sql.Table) []*sql.CheckConstraint {
	var loadedChecks []*sql.CheckConstraint
	if checkTable, ok := table.(sql.CheckTable); ok {
		checks, err := checkTable.GetChecks(b.ctx)
		if err != nil {
			b.handleErr(err)
		}
		for _, ch := range checks {
			constraint := b.buildCheckConstraint(inScope, &ch)
			loadedChecks = append(loadedChecks, constraint)
		}
	}
	return loadedChecks
}

func (b *PlanBuilder) buildCheckConstraint(inScope *scope, check *sql.CheckDefinition) *sql.CheckConstraint {
	parseStr := fmt.Sprintf("select %s", check.CheckExpression)
	parsed, err := ast.Parse(parseStr)
	if err != nil {
		b.handleErr(err)
	}

	selectStmt, ok := parsed.(*ast.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		err := sql.ErrInvalidCheckConstraint.New(check.CheckExpression)
		b.handleErr(err)
	}

	expr := selectStmt.SelectExprs[0]
	ae, ok := expr.(*ast.AliasedExpr)
	if !ok {
		err := sql.ErrInvalidCheckConstraint.New(check.CheckExpression)
		b.handleErr(err)
	}

	c := b.buildScalar(inScope, ae.Expr)

	return &sql.CheckConstraint{
		Name:     check.Name,
		Expr:     c,
		Enforced: check.Enforced,
	}
}
