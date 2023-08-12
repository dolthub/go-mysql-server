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

const OnDupValuesPrefix = "__new_ins"

func (b *Builder) buildInsert(inScope *scope, i *ast.Insert) (outScope *scope) {
	if i.With != nil {
		inScope = b.buildWith(inScope, i.With)
	}
	dbName := i.Table.Qualifier.String()
	tableName := i.Table.Name.String()
	destScope, ok := b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	var db sql.Database
	var rt *plan.ResolvedTable
	switch n := destScope.node.(type) {
	case *plan.ResolvedTable:
		rt = n
		db = rt.SqlDatabase
	case *plan.UnresolvedTable:
		db = n.Database()
	default:
		b.handleErr(fmt.Errorf("expected insert destination to be resolved or unresolved table"))
	}
	if rt == nil {
		if b.TriggerCtx().Active && !b.TriggerCtx().Call {
			b.TriggerCtx().UnresolvedTables = append(b.TriggerCtx().UnresolvedTables, tableName)
		} else {
			err := fmt.Errorf("expected resolved table: %s", tableName)
			b.handleErr(err)
		}
	}
	isReplace := i.Action == ast.ReplaceStr

	var columns []string
	{
		columns = columnsToStrings(i.Columns)
		// If no column names were specified in the query, go ahead and fill
		// them all in now that the destination is resolved.
		// TODO: setting the plan field directly is not great
		if len(columns) == 0 && len(destScope.cols) > 0 && rt != nil {
			schema := rt.Schema()
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}
	}

	srcScope := b.insertRowsToNode(inScope, i.Rows, columns, destScope.node.Schema())

	combinedScope := inScope.replace()
	for i, c := range destScope.cols {
		combinedScope.newColumn(c)
		if len(srcScope.cols) == len(destScope.cols) {
			combinedScope.newColumn(srcScope.cols[i])
		} else {
			// check for VALUES refs
			c.table = OnDupValuesPrefix
			combinedScope.newColumn(c)
		}
	}
	onDupExprs := b.buildOnDupUpdateExprs(combinedScope, destScope, ast.AssignmentExprs(i.OnDup))

	ignore := false
	// TODO: make this a bool in vitess
	if strings.Contains(strings.ToLower(i.Ignore), "ignore") {
		ignore = true
	}

	dest := destScope.node

	ins := plan.NewInsertInto(db, plan.NewInsertDestination(dest.Schema(), dest), srcScope.node, isReplace, columns, onDupExprs, ignore)

	if rt != nil {
		checks := b.loadChecksFromTable(destScope, rt.Table)
		ins.Checks = checks
	}

	outScope = destScope
	outScope.node = ins

	return
}

func (b *Builder) insertRowsToNode(inScope *scope, ir ast.InsertRows, columnNames []string, destSchema sql.Schema) (outScope *scope) {
	switch v := ir.(type) {
	case ast.SelectStatement:
		return b.buildSelectStmt(inScope, v)
	case ast.Values:
		outScope = b.buildInsertValues(inScope, v, columnNames, destSchema)

	default:
		err := sql.ErrUnsupportedSyntax.New(ast.String(ir))
		b.handleErr(err)
	}
	return
}

func (b *Builder) buildInsertValues(inScope *scope, v ast.Values, columnNames []string, destSchema sql.Schema) (outScope *scope) {
	columnDefaultValues := make([]*sql.ColumnDefaultValue, len(columnNames))
	for i, columnName := range columnNames {
		index := destSchema.IndexOfColName(columnName)
		if index == -1 {
			if !b.TriggerCtx().Call && len(b.TriggerCtx().UnresolvedTables) > 0 {
				continue
			}
			err := plan.ErrInsertIntoNonexistentColumn.New(columnName)
			b.handleErr(err)
		}
		columnDefaultValues[i] = destSchema[index].Default
	}

	exprTuples := make([][]sql.Expression, len(v))
	for i, vt := range v {
		exprs := make([]sql.Expression, len(columnNames))
		exprTuples[i] = exprs
		noExprs := len(vt) == 0
		for j := range columnNames {
			if noExprs {
				exprs[j] = expression.WrapExpression(columnDefaultValues[j])
				continue
			}
			e := vt[j]
			switch e := e.(type) {
			case *ast.Default:
				exprs[j] = expression.WrapExpression(columnDefaultValues[j])
			default:
				exprs[j] = b.buildScalar(inScope, e)
			}
		}
	}

	outScope = inScope.push()
	outScope.node = plan.NewValues(exprTuples)
	return
}

func (b *Builder) buildValues(inScope *scope, v ast.Values) (outScope *scope) {
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

func (b *Builder) assignmentExprsToExpressions(inScope *scope, e ast.AssignmentExprs) []sql.Expression {
	res := make([]sql.Expression, len(e))
	var startAggCnt int
	if inScope.groupBy != nil {
		startAggCnt = len(inScope.groupBy.aggs)
	}
	var startWinCnt int
	if inScope.windowFuncs != nil {
		startWinCnt = len(inScope.windowFuncs)
	}
	for i, updateExpr := range e {
		colName := b.buildScalar(inScope, updateExpr.Name)
		innerExpr := b.buildScalar(inScope, updateExpr.Expr)
		res[i] = expression.NewSetField(colName, innerExpr)
		if inScope.groupBy != nil {
			if len(inScope.groupBy.aggs) > startAggCnt {
				err := sql.ErrAggregationUnsupported.New(res[i])
				b.handleErr(err)
			}
		}
		if inScope.windowFuncs != nil {
			if len(inScope.windowFuncs) > startWinCnt {
				err := sql.ErrWindowUnsupported.New(res[i])
				b.handleErr(err)
			}
		}
	}
	return res
}

func (b *Builder) buildOnDupUpdateExprs(combinedScope, destScope *scope, e ast.AssignmentExprs) []sql.Expression {
	b.insertActive = true
	defer func() {
		b.insertActive = false
	}()
	res := make([]sql.Expression, len(e))
	// todo(max): prevent aggregations in separate semantic walk step
	var startAggCnt int
	if combinedScope.groupBy != nil {
		startAggCnt = len(combinedScope.groupBy.aggs)
	}
	var startWinCnt int
	if combinedScope.windowFuncs != nil {
		startWinCnt = len(combinedScope.windowFuncs)
	}
	for i, updateExpr := range e {
		colName := b.buildOnDupLeft(destScope, updateExpr.Name)
		innerExpr := b.buildScalar(combinedScope, updateExpr.Expr)

		res[i] = expression.NewSetField(colName, innerExpr)
		if combinedScope.groupBy != nil {
			if len(combinedScope.groupBy.aggs) > startAggCnt {
				err := sql.ErrAggregationUnsupported.New(res[i])
				b.handleErr(err)
			}
		}
		if combinedScope.windowFuncs != nil {
			if len(combinedScope.windowFuncs) > startWinCnt {
				err := sql.ErrWindowUnsupported.New(res[i])
				b.handleErr(err)
			}
		}
	}
	return res
}

func (b *Builder) buildOnDupLeft(inScope *scope, e ast.Expr) sql.Expression {
	// expect col reference only
	switch e := e.(type) {
	case *ast.ColName:
		c, ok := inScope.resolveColumn(strings.ToLower(e.Qualifier.Name.String()), strings.ToLower(e.Name.String()), true)
		if !ok {
			b.handleErr(sql.ErrColumnNotFound.New(e))
		}
		return c.scalarGf()
	default:
		err := fmt.Errorf("invalid update target; expected column reference, found: %T", e)
		b.handleErr(err)
	}
	return nil
}

func (b *Builder) buildDelete(inScope *scope, d *ast.Delete) (outScope *scope) {
	outScope = b.buildFrom(inScope, d.TableExprs)
	b.buildWhere(outScope, d.Where)
	orderByScope := b.analyzeOrderBy(outScope, outScope, d.OrderBy)
	b.buildOrderBy(outScope, orderByScope)
	offset := b.buildOffset(outScope, d.Limit)
	if offset != nil {
		outScope.node = plan.NewOffset(offset, outScope.node)
	}
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
			var target sql.Node
			if _, ok := outScope.tables[tabName]; ok {
				transform.InspectUp(outScope.node, func(n sql.Node) bool {
					switch n := n.(type) {
					case sql.NameableNode:
						if strings.EqualFold(n.Name(), tabName) {
							target = n
							return true
						}
					default:
					}
					return false
				})
			} else {
				tableScope, ok := b.buildTablescan(inScope, dbName, tabName, nil)
				if !ok {
					b.handleErr(sql.ErrTableNotFound.New(tabName))
				}
				target = tableScope.node
			}
			targets[i] = target
		}
	}

	del := plan.NewDeleteFrom(outScope.node, targets)
	outScope.node = del
	return
}

func (b *Builder) buildUpdate(inScope *scope, u *ast.Update) (outScope *scope) {
	outScope = b.buildFrom(inScope, u.TableExprs)

	updateExprs := b.assignmentExprsToExpressions(outScope, u.Exprs)

	b.buildWhere(outScope, u.Where)

	orderByScope := b.analyzeOrderBy(outScope, b.newScope(), u.OrderBy)

	b.buildOrderBy(outScope, orderByScope)
	offset := b.buildOffset(outScope, u.Limit)
	if offset != nil {
		outScope.node = plan.NewOffset(offset, outScope.node)
	}

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

	var checks []*sql.CheckConstraint
	if join, ok := outScope.node.(*plan.JoinNode); ok {
		source := plan.NewUpdateSource(
			join,
			ignore,
			updateExprs,
		)
		updaters, err := rowUpdatersByTable(b.ctx, source, join)
		if err != nil {
			b.handleErr(err)
		}
		updateJoin := plan.NewUpdateJoin(updaters, source)
		update.Child = updateJoin
		transform.Inspect(update, func(n sql.Node) bool {
			// todo maybe this should be later stage
			switch n := n.(type) {
			case sql.NameableNode:
				if _, ok := updaters[n.Name()]; ok {
					rt := getResolvedTable(n)
					tableScope := inScope.push()
					for _, c := range rt.Schema() {
						tableScope.addColumn(scopeColumn{
							db:       rt.SqlDatabase.Name(),
							table:    strings.ToLower(n.Name()),
							col:      strings.ToLower(c.Name),
							typ:      c.Type,
							nullable: c.Nullable,
						})
					}
					checks = append(checks, b.loadChecksFromTable(tableScope, rt.Table)...)
				}
			default:
			}
			return true
		})
	} else {
		transform.Inspect(update, func(n sql.Node) bool {
			// todo maybe this should be later stage
			if rt, ok := n.(*plan.ResolvedTable); ok {
				checks = append(checks, b.loadChecksFromTable(outScope, rt.Table)...)
			}
			return true
		})
	}
	update.Checks = checks
	outScope.node = update
	return
}

// rowUpdatersByTable maps a set of tables to their RowUpdater objects.
func rowUpdatersByTable(ctx *sql.Context, node sql.Node, ij sql.Node) (map[string]sql.RowUpdater, error) {
	namesOfTableToBeUpdated := getTablesToBeUpdated(node)
	resolvedTables := getTablesByName(ij)

	rowUpdatersByTable := make(map[string]sql.RowUpdater)
	for tableToBeUpdated, _ := range namesOfTableToBeUpdated {
		resolvedTable, ok := resolvedTables[tableToBeUpdated]
		if !ok {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}

		var table = resolvedTable.UnderlyingTable()

		// If there is no UpdatableTable for a table being updated, error out
		updatable, ok := table.(sql.UpdatableTable)
		if !ok && updatable == nil {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}

		keyless := sql.IsKeyless(updatable.Schema())
		if keyless {
			return nil, sql.ErrUnsupportedFeature.New("error: keyless tables unsupported for UPDATE JOIN")
		}

		rowUpdatersByTable[tableToBeUpdated] = updatable.Updater(ctx)
	}

	return rowUpdatersByTable, nil
}

// getTablesByName takes a node and returns all found resolved tables in a map.
func getTablesByName(node sql.Node) map[string]*plan.ResolvedTable {
	ret := make(map[string]*plan.ResolvedTable)

	transform.Inspect(node, func(node sql.Node) bool {
		switch n := node.(type) {
		case *plan.ResolvedTable:
			ret[n.Table.Name()] = n
		case *plan.IndexedTableAccess:
			rt, ok := n.TableNode.(*plan.ResolvedTable)
			if ok {
				ret[rt.Name()] = rt
			}
		case *plan.TableAlias:
			rt := getResolvedTable(n)
			if rt != nil {
				ret[n.Name()] = rt
			}
		default:
		}
		return true
	})

	return ret
}

// Finds first TableNode node that is a descendant of the node given
func getResolvedTable(node sql.Node) *plan.ResolvedTable {
	var table *plan.ResolvedTable
	transform.Inspect(node, func(node sql.Node) bool {
		// plan.Inspect will get called on all children of a node even if one of the children's calls returns false. We
		// only want the first TableNode match.
		if table != nil {
			return false
		}

		switch n := node.(type) {
		case *plan.ResolvedTable:
			if !plan.IsDualTable(n) {
				table = n
				return false
			}
		case *plan.IndexedTableAccess:
			rt, ok := n.TableNode.(*plan.ResolvedTable)
			if ok {
				table = rt
				return false
			}
		}
		return true
	})
	return table
}

// getTablesToBeUpdated takes a node and looks for the tables to modified by a SetField.
func getTablesToBeUpdated(node sql.Node) map[string]struct{} {
	ret := make(map[string]struct{})

	transform.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.SetField:
			gf := e.Left.(*expression.GetField)
			ret[gf.Table()] = struct{}{}
			return false
		}

		return true
	})

	return ret
}

func (b *Builder) buildInto(inScope *scope, into *ast.Into) {
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

func (b *Builder) loadChecksFromTable(inScope *scope, table sql.Table) []*sql.CheckConstraint {
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

func (b *Builder) buildCheckConstraint(inScope *scope, check *sql.CheckDefinition) *sql.CheckConstraint {
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
