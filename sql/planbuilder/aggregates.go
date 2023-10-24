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
	"fmt"
	"sort"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ ast.Expr = (*aggregateInfo)(nil)

type groupBy struct {
	inCols   []scopeColumn
	outScope *scope
	aggs     map[string]scopeColumn
	grouping map[string]bool
}

func (g *groupBy) addInCol(c scopeColumn) {
	g.inCols = append(g.inCols, c)
}

func (g *groupBy) addOutCol(c scopeColumn) columnId {
	return g.outScope.newColumn(c)
}

func (g *groupBy) hasAggs() bool {
	return len(g.aggs) > 0
}

func (g *groupBy) aggregations() []scopeColumn {
	aggregations := make([]scopeColumn, 0, len(g.aggs))
	for _, agg := range g.aggs {
		aggregations = append(aggregations, agg)
	}
	sort.Slice(aggregations, func(i, j int) bool {
		return aggregations[i].scalar.String() < aggregations[j].scalar.String()
	})
	return aggregations
}

func (g *groupBy) addAggStr(c scopeColumn) {
	if g.aggs == nil {
		g.aggs = make(map[string]scopeColumn)
	}
	g.aggs[strings.ToLower(c.scalar.String())] = c
}

func (g *groupBy) getAggRef(name string) sql.Expression {
	if g.aggs == nil {
		return nil
	}
	ret, _ := g.aggs[name]
	if ret.empty() {
		return nil
	}
	return ret.scalarGf()
}

type aggregateInfo struct {
	ast.Expr
}

func (b *Builder) needsAggregation(fromScope *scope, sel *ast.Select) bool {
	return len(sel.GroupBy) > 0 ||
		(fromScope.groupBy != nil && fromScope.groupBy.hasAggs())
}

func (b *Builder) buildGroupingCols(fromScope, projScope *scope, groupby ast.GroupBy, selects ast.SelectExprs) []sql.Expression {
	// grouping col will either be:
	// 1) alias into targets
	// 2) a column reference
	// 3) an index into selects
	// 4) a simple non-aggregate expression
	groupings := make([]sql.Expression, 0)
	if fromScope.groupBy == nil {
		fromScope.initGroupBy()
	}
	g := fromScope.groupBy
	for _, e := range groupby {
		var col scopeColumn
		switch e := e.(type) {
		case *ast.ColName:
			var ok bool
			// GROUP BY binds to column references before projections.
			dbName := strings.ToLower(e.Qualifier.Qualifier.String())
			tblName := strings.ToLower(e.Qualifier.Name.String())
			colName := strings.ToLower(e.Name.String())
			col, ok = fromScope.resolveColumn(dbName, tblName, colName, true)
			if !ok {
				col, ok = projScope.resolveColumn(dbName, tblName, colName, true)
			}

			if !ok {
				b.handleErr(sql.ErrColumnNotFound.New(e.Name.String()))
			}
		case *ast.SQLVal:
			// literal -> index into targets
			replace := b.normalizeValArg(e)
			val, ok := replace.(*ast.SQLVal)
			if !ok {
				// ast.NullVal
				continue
			}
			if val.Type == ast.IntVal {
				lit := b.convertInt(string(val.Val), 10)
				idx, _, err := types.Int64.Convert(lit.Value())
				if err != nil {
					b.handleErr(err)
				}
				intIdx, ok := idx.(int64)
				if !ok {
					b.handleErr(fmt.Errorf("expected integer order by literal"))
				}
				if intIdx < 1 {
					b.handleErr(fmt.Errorf("expected positive integer order by literal"))
				}
				col = projScope.cols[intIdx-1]
			}
		default:
			expr := b.buildScalar(fromScope, e)
			col = scopeColumn{
				tableId:  sql.TableID{},
				col:      expr.String(),
				typ:      nil,
				scalar:   expr,
				nullable: expr.IsNullable(),
			}
		}
		if col.scalar == nil {
			gf := expression.NewGetFieldWithTable(0, col.typ, col.tableId.DatabaseName, col.tableId.TableName, col.col, col.nullable)
			id, ok := fromScope.getExpr(gf.String(), true)
			if !ok {
				err := sql.ErrColumnNotFound.New(gf.String())
				b.handleErr(err)
			}
			col.scalar = gf.WithIndex(int(id))
		}
		g.addInCol(col)
		groupings = append(groupings, col.scalar)
	}

	return groupings
}

func (b *Builder) buildAggregation(fromScope, projScope *scope, groupingCols []sql.Expression) *scope {
	// GROUP_BY consists of:
	// - input arguments projection
	// - grouping cols projection
	// - aggregate expressions
	// - output projection
	if fromScope.groupBy == nil {
		fromScope.initGroupBy()
	}

	group := fromScope.groupBy
	outScope := group.outScope
	// select columns:
	//  - aggs
	//  - extra columns needed by having, order by, select
	var selectExprs []sql.Expression
	var selectGfs []sql.Expression
	selectStr := make(map[string]bool)
	for _, e := range group.aggregations() {
		if !selectStr[strings.ToLower(e.String())] {
			selectExprs = append(selectExprs, e.scalar)
			selectGfs = append(selectGfs, e.scalarGf())
			selectStr[strings.ToLower(e.String())] = true
		}
	}
	var aliases []sql.Expression
	for _, col := range projScope.cols {
		// eval aliases in project scope
		switch e := col.scalar.(type) {
		case *expression.Alias:
			if !e.Unreferencable() {
				aliases = append(aliases, e.WithId(sql.ColumnId(col.id)))
			}
		default:
		}

		// projection dependencies -> table cols needed above
		transform.InspectExpr(col.scalar, func(e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				colName := strings.ToLower(e.String())
				if !selectStr[colName] {
					selectExprs = append(selectExprs, e)
					selectGfs = append(selectGfs, e)
					selectStr[colName] = true
				}
			default:
			}
			return false
		})
	}
	for _, e := range fromScope.extraCols {
		// accessory cols used by ORDER_BY, HAVING
		if !selectStr[e.String()] {
			selectExprs = append(selectExprs, e.scalarGf())
			selectGfs = append(selectGfs, e.scalarGf())

			selectStr[e.String()] = true
		}
	}
	gb := plan.NewGroupBy(selectExprs, groupingCols, fromScope.node)
	outScope.node = gb

	if len(aliases) > 0 {
		outScope.node = plan.NewProject(append(selectGfs, aliases...), outScope.node)
	}
	return outScope
}

func isAggregateFunc(name string) bool {
	switch name {
	case "avg", "bit_and", "bit_or", "bit_xor", "count",
		"group_concat", "json_arrayagg", "json_objectagg",
		"max", "min", "std", "stddev_pop", "stddev_samp",
		"stddev", "sum", "var_pop", "var_samp", "variance",
		"first", "last", "any_value":
		return true
	default:
		return false
	}
}

// buildAggregateFunc tags aggregate functions in the correct scope
// and makes the aggregate available for reference by other clauses.
func (b *Builder) buildAggregateFunc(inScope *scope, name string, e *ast.FuncExpr) sql.Expression {
	if len(inScope.windowFuncs) > 0 {
		err := sql.ErrNonAggregatedColumnWithoutGroupBy.New()
		b.handleErr(err)
	}

	if inScope.groupBy == nil {
		inScope.initGroupBy()
	}
	gb := inScope.groupBy

	if name == "count" {
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			var agg sql.Aggregation
			if e.Distinct {
				agg = aggregation.NewCountDistinct(expression.NewLiteral(1, types.Int64))
			} else {
				agg = aggregation.NewCount(expression.NewLiteral(1, types.Int64))
			}
			aggName := strings.ToLower(agg.String())
			gf := gb.getAggRef(aggName)
			if gf != nil {
				// if we've already computed use reference here
				return gf
			}

			col := scopeColumn{col: strings.ToLower(agg.String()), scalar: agg, typ: agg.Type(), nullable: agg.IsNullable()}
			id := gb.outScope.newColumn(col)
			col.id = id
			gb.addAggStr(col)
			return col.scalarGf()
		}
	}

	if name == "jsonarray" {
		// TODO we don't have any tests for this
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			var agg sql.Aggregation
			agg = aggregation.NewJsonArray(expression.NewLiteral(expression.NewStar(), types.Int64))
			//if e.Distinct {
			//	agg = plan.NewDistinct(expression.NewLiteral(1, types.Int64))
			//}
			aggName := strings.ToLower(agg.String())
			gf := gb.getAggRef(aggName)
			if gf != nil {
				// if we've already computed use reference here
				return gf
			}

			col := scopeColumn{col: strings.ToLower(agg.String()), scalar: agg, typ: agg.Type(), nullable: agg.IsNullable()}
			id := gb.outScope.newColumn(col)
			col.id = id
			gb.addAggStr(col)
			return col.scalarGf()
		}
	}

	var args []sql.Expression
	for _, arg := range e.Exprs {
		e := b.selectExprToExpression(inScope, arg)
		switch e := e.(type) {
		case *expression.GetField:
			args = append(args, e)
			col := scopeColumn{tableId: e.TableID(), col: e.Name(), scalar: e, typ: e.Type(), nullable: e.IsNullable()}
			gb.addInCol(col)
		case *expression.Star:
			err := sql.ErrStarUnsupported.New()
			b.handleErr(err)
		case *plan.Subquery:
			args = append(args, e)
			col := scopeColumn{col: e.QueryString, scalar: e, typ: e.Type()}
			gb.addInCol(col)
		default:
			args = append(args, e)
			col := scopeColumn{col: e.String(), scalar: e, typ: e.Type()}
			gb.addInCol(col)
		}
	}

	var agg sql.Expression
	if e.Distinct && name == "count" {
		agg = aggregation.NewCountDistinct(args...)
	} else {

		// NOTE: Not all aggregate functions support DISTINCT. Fortunately, the vitess parser will throw
		// errors for when DISTINCT is used on aggregate functions that don't support DISTINCT.
		if e.Distinct {
			if len(e.Exprs) != 1 {
				err := sql.ErrUnsupportedSyntax.New("more than one expression with distinct")
				b.handleErr(err)
			}

			args[0] = expression.NewDistinctExpression(args[0])
		}

		f, err := b.cat.Function(b.ctx, name)
		if err != nil {
			b.handleErr(err)
		}

		agg, err = f.NewInstance(args)
		if err != nil {
			b.handleErr(err)
		}
	}

	aggType := agg.Type()
	if name == "avg" || name == "sum" {
		aggType = types.Float64
	}

	aggName := strings.ToLower(plan.AliasSubqueryString(agg))
	if id, ok := gb.outScope.getExpr(aggName, true); ok {
		// if we've already computed use reference here
		gf := expression.NewGetFieldWithTable(int(id), aggType, "", "", aggName, agg.IsNullable())
		return gf
	}

	col := scopeColumn{col: aggName, scalar: agg, typ: aggType, nullable: agg.IsNullable()}
	id := gb.outScope.newColumn(col)
	col.id = id
	gb.addAggStr(col)
	return col.scalarGf()
}

func (b *Builder) buildGroupConcat(inScope *scope, e *ast.GroupConcatExpr) sql.Expression {
	if inScope.groupBy == nil {
		inScope.initGroupBy()
	}
	gb := inScope.groupBy

	args := make([]sql.Expression, len(e.Exprs))
	for i, a := range e.Exprs {
		args[i] = b.selectExprToExpression(inScope, a)
	}

	separatorS := ","
	if !e.Separator.DefaultSeparator {
		separatorS = e.Separator.SeparatorString
	}

	orderByScope := b.analyzeOrderBy(inScope, inScope, e.OrderBy)
	var sortFields sql.SortFields
	for _, c := range orderByScope.cols {
		so := sql.Ascending
		if c.descending {
			so = sql.Descending
		}
		scalar := c.scalar
		if scalar == nil {
			scalar = c.scalarGf()
		}
		sf := sql.SortField{
			Column: scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}

	//TODO: this should be acquired at runtime, not at parse time, so fix this
	gcml, err := b.ctx.GetSessionVariable(b.ctx, "group_concat_max_len")
	if err != nil {
		b.handleErr(err)
	}
	groupConcatMaxLen := gcml.(uint64)

	// todo store ref to aggregate
	agg := aggregation.NewGroupConcat(e.Distinct, sortFields, separatorS, args, int(groupConcatMaxLen))
	aggName := strings.ToLower(plan.AliasSubqueryString(agg))
	col := scopeColumn{col: aggName, scalar: agg, typ: agg.Type(), nullable: agg.IsNullable()}

	id := gb.outScope.newColumn(col)
	gb.addAggStr(col)
	col.id = id
	return col.scalarGf()
}

func isWindowFunc(name string) bool {
	switch name {
	case "first", "last", "count", "sum", "any_value",
		"avg", "max", "min", "count_distinct", "json_arrayagg",
		"row_number", "percent_rank", "lead", "lag",
		"first_value", "last_value",
		"rank", "dense_rank":
		return true
	default:
		return false
	}
}

func (b *Builder) buildWindowFunc(inScope *scope, name string, e *ast.FuncExpr, over *ast.WindowDef) sql.Expression {
	if inScope.groupBy != nil {
		err := sql.ErrNonAggregatedColumnWithoutGroupBy.New()
		b.handleErr(err)
	}

	// internal expressions can be complex, but window can't be more than alias
	var args []sql.Expression
	for _, arg := range e.Exprs {
		e := b.selectExprToExpression(inScope, arg)
		args = append(args, e)
	}

	var win sql.Expression
	if name == "count" {
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			win = aggregation.NewCount(expression.NewLiteral(1, types.Int64))
		}
	}
	if win == nil {
		f, err := b.cat.Function(b.ctx, name)
		if err != nil {
			b.handleErr(err)
		}

		win, err = f.NewInstance(args)
		if err != nil {
			b.handleErr(err)
		}
	}

	def := b.buildWindowDef(inScope, over)
	switch w := win.(type) {
	case sql.WindowAdaptableExpression:
		win = w.WithWindow(def)
	}

	col := scopeColumn{col: strings.ToLower(win.String()), scalar: win, typ: win.Type(), nullable: win.IsNullable()}
	id := inScope.newColumn(col)
	col.id = id
	inScope.windowFuncs = append(inScope.windowFuncs, col)
	return col.scalarGf()
}

func (b *Builder) buildWindow(fromScope, projScope *scope) *scope {
	if len(fromScope.windowFuncs) == 0 {
		return fromScope
	}
	// passthrough dependency cols plus window funcs
	var selectExprs []sql.Expression
	var selectGfs []sql.Expression
	selectStr := make(map[string]bool)
	for _, col := range fromScope.windowFuncs {
		e := col.scalar
		if !selectStr[strings.ToLower(e.String())] {
			switch e.(type) {
			case sql.WindowAdaptableExpression:
				selectStr[strings.ToLower(e.String())] = true
				selectExprs = append(selectExprs, e)
				selectGfs = append(selectGfs, col.scalarGf())
			default:
				err := fmt.Errorf("expected window function to be sql.WindowAggregation")
				b.handleErr(err)
			}
		}
	}
	var aliases []sql.Expression
	for _, col := range projScope.cols {
		// eval aliases in project scope
		switch e := col.scalar.(type) {
		case *expression.Alias:
			if !e.Unreferencable() {
				aliases = append(aliases, e.WithId(sql.ColumnId(col.id)))
			}
		default:
		}

		// projection dependencies -> table cols needed above
		transform.InspectExpr(col.scalar, func(e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				colName := strings.ToLower(e.String())
				if !selectStr[colName] {
					selectExprs = append(selectExprs, e)
					selectGfs = append(selectGfs, e)
					selectStr[colName] = true
				}
			default:
			}
			return false
		})
	}
	for _, e := range fromScope.extraCols {
		// accessory cols used by ORDER_BY, HAVING
		if !selectStr[e.String()] {
			selectExprs = append(selectExprs, e.scalarGf())
			selectGfs = append(selectGfs, e.scalarGf())
			selectStr[e.String()] = true
		}
	}

	outScope := fromScope
	window := plan.NewWindow(selectExprs, fromScope.node)
	fromScope.node = window

	if len(aliases) > 0 {
		outScope.node = plan.NewProject(append(selectGfs, aliases...), outScope.node)
	}

	return outScope
}

func (b *Builder) buildNamedWindows(fromScope *scope, window ast.Window) {
	// topo sort first
	adj := make(map[string]*ast.WindowDef)
	for _, w := range window {
		adj[w.Name.Lowered()] = w
	}

	var topo []*ast.WindowDef
	var seen map[string]bool
	var dfs func(string)
	dfs = func(name string) {
		if ok, _ := seen[name]; ok {
			b.handleErr(sql.ErrCircularWindowInheritance.New())
		}
		seen[name] = true
		cur := adj[name]
		if ref := cur.NameRef.Lowered(); ref != "" {
			dfs(ref)
		}
		topo = append(topo, cur)
	}
	for _, w := range adj {
		seen = make(map[string]bool)
		dfs(w.Name.Lowered())
	}

	fromScope.windowDefs = make(map[string]*sql.WindowDefinition)
	for _, w := range topo {
		fromScope.windowDefs[w.Name.Lowered()] = b.buildWindowDef(fromScope, w)
	}
	return
}

func (b *Builder) buildWindowDef(fromScope *scope, def *ast.WindowDef) *sql.WindowDefinition {
	if def == nil {
		return nil
	}

	var sortFields sql.SortFields
	for _, c := range def.OrderBy {
		// resolve col in fromScope
		e := b.buildScalar(fromScope, c.Expr)
		so := sql.Ascending
		if c.Direction == ast.DescScr {
			so = sql.Descending
		}
		sf := sql.SortField{
			Column: e,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}

	partitions := make([]sql.Expression, len(def.PartitionBy))
	for i, expr := range def.PartitionBy {
		partitions[i] = b.buildScalar(fromScope, expr)
	}

	frame := b.NewFrame(fromScope, def.Frame)

	// According to MySQL documentation at https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
	// "If OVER() is empty, the window consists of all query rows and the window function computes a result using all rows."
	if def.OrderBy == nil && frame == nil {
		frame = plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame()
	}

	windowDef := sql.NewWindowDefinition(partitions, sortFields, frame, def.NameRef.Lowered(), def.Name.Lowered())
	if ref, ok := fromScope.windowDefs[def.NameRef.Lowered()]; ok {
		// this is only safe if windows are built in topo order
		windowDef = b.mergeWindowDefs(windowDef, ref)
		// collapse dependencies if any reference this window
		fromScope.windowDefs[windowDef.Name] = windowDef
	}
	return windowDef
}

// mergeWindowDefs combines the attributes of two window definitions or returns
// an error if the two are incompatible. [def] should have a reference to
// [ref] through [def.Ref], and the return value drops the reference to indicate
// the two were properly combined.
func (b *Builder) mergeWindowDefs(def, ref *sql.WindowDefinition) *sql.WindowDefinition {
	if ref.Ref != "" {
		panic("unreachable; cannot merge unresolved window definition")
	}

	var orderBy sql.SortFields
	switch {
	case len(def.OrderBy) > 0 && len(ref.OrderBy) > 0:
		err := sql.ErrInvalidWindowInheritance.New("", "", "both contain order by clause")
		b.handleErr(err)
	case len(def.OrderBy) > 0:
		orderBy = def.OrderBy
	case len(ref.OrderBy) > 0:
		orderBy = ref.OrderBy
	default:
	}

	var partitionBy []sql.Expression
	switch {
	case len(def.PartitionBy) > 0 && len(ref.PartitionBy) > 0:
		err := sql.ErrInvalidWindowInheritance.New("", "", "both contain partition by clause")
		b.handleErr(err)
	case len(def.PartitionBy) > 0:
		partitionBy = def.PartitionBy
	case len(ref.PartitionBy) > 0:
		partitionBy = ref.PartitionBy
	default:
		partitionBy = []sql.Expression{}
	}

	var frame sql.WindowFrame
	switch {
	case def.Frame != nil && ref.Frame != nil:
		_, isDefDefaultFrame := def.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)
		_, isRefDefaultFrame := ref.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)

		// if both frames are set and one is RowsUnboundedPrecedingToUnboundedFollowingFrame (default),
		// we should use the other frame
		if isDefDefaultFrame {
			frame = ref.Frame
		} else if isRefDefaultFrame {
			frame = def.Frame
		} else {
			// if both frames have identical string representations, use either one
			df := def.Frame.String()
			rf := ref.Frame.String()
			if df != rf {
				err := sql.ErrInvalidWindowInheritance.New("", "", "both contain different frame clauses")
				b.handleErr(err)
			}
			frame = def.Frame
		}
	case def.Frame != nil:
		frame = def.Frame
	case ref.Frame != nil:
		frame = ref.Frame
	default:
	}

	return sql.NewWindowDefinition(partitionBy, orderBy, frame, "", def.Name)
}

func (b *Builder) analyzeHaving(fromScope, projScope *scope, having *ast.Where) {
	// build having filter expr
	// aggregates added to fromScope.groupBy
	// can see projScope outputs
	if having == nil {
		return
	}

	ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery:
			return false, nil
		case *ast.FuncExpr:
			name := n.Name.Lowered()
			if isAggregateFunc(name) {
				// record aggregate
				_ = b.buildAggregateFunc(fromScope, name, n)
			} else if isWindowFunc(name) {
				_ = b.buildWindowFunc(fromScope, name, n, (*ast.WindowDef)(n.Over))
			}
		case *ast.ColName:
			// add to extra cols
			dbName := strings.ToLower(n.Qualifier.Qualifier.String())
			tblName := strings.ToLower(n.Qualifier.Name.String())
			colName := strings.ToLower(n.Name.String())
			c, ok := projScope.resolveColumn(dbName, tblName, colName, false)
			if ok {
				// references projection alias
				break
			}
			c, ok = fromScope.resolveColumn(dbName, tblName, colName, true)
			if !ok {
				err := sql.ErrColumnNotFound.New(n.Name)
				b.handleErr(err)
			}
			c.scalar = expression.NewGetFieldWithTable(int(c.id), c.typ, c.tableId.DatabaseName, c.tableId.TableName, c.col, c.nullable)
			fromScope.addExtraColumn(c)
		}
		return true, nil
	}, having.Expr)
}

func (b *Builder) buildInnerProj(fromScope, projScope *scope) *scope {
	outScope := fromScope
	proj := make([]sql.Expression, len(fromScope.cols))
	for i, c := range fromScope.cols {
		proj[i] = c.scalarGf()
	}

	// eval aliases in project scope
	for _, col := range projScope.cols {
		switch e := col.scalar.(type) {
		case *expression.Alias:
			if !e.Unreferencable() {
				proj = append(proj, e.WithId(sql.ColumnId(col.id)))
			}
		}
	}

	if len(proj) > 0 {
		outScope.node = plan.NewProject(proj, outScope.node)
	}

	return outScope
}

func (b *Builder) buildHaving(fromScope, projScope, outScope *scope, having *ast.Where) {
	// expressions in having can be from aggOut or projScop
	if having == nil {
		return
	}
	if fromScope.groupBy == nil {
		fromScope.initGroupBy()
	}
	havingScope := fromScope.push()
	for _, c := range projScope.cols {
		if c.tableId.IsEmpty() {
			havingScope.newColumn(c)
		}
	}
	havingScope.groupBy = fromScope.groupBy
	h := b.buildScalar(havingScope, having.Expr)
	outScope.node = plan.NewHaving(h, outScope.node)
	return
}
