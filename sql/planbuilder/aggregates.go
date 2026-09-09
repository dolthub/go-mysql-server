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
	outScope *scope
	aggs     map[string]scopeColumn
	grouping map[string]bool
	inCols   []scopeColumn
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

// aggregateScope returns the closest query source referenced by an aggregate's arguments.
func (s *scope) aggregateScope(ctx *sql.Context, args []sql.Expression) *scope {
	var columnIds sql.ColSet
	for _, arg := range args {
		sql.Inspect(ctx, arg, func(ctx *sql.Context, expr sql.Expression) bool {
			if _, ok := expr.(*plan.Subquery); ok {
				return false
			}
			gf, ok := expr.(*expression.GetField)
			if !ok {
				return true
			}
			columnIds.Add(gf.Id())
			return false
		})
	}
	if columnIds.Empty() {
		return s.querySource
	}
	for candidate := s.querySource; candidate != nil; candidate = candidate.outerQuery {
		for _, col := range candidate.cols {
			if columnIds.Contains(sql.ColumnId(col.id)) {
				return candidate
			}
		}
	}
	return s.querySource
}

// isCorrelatedColumn reports whether the column is supplied by an outer query to the active subquery.
func (s *scope) isCorrelatedColumn(id sql.ColumnId) bool {
	for candidate := s.querySource; candidate != nil; candidate = candidate.outerQuery {
		if _, found := candidate.queryColumn(id); found {
			return candidate != s.querySource
		}
	}
	return false
}

// queryColumn returns a source column by ID, including join scopes whose colset is populated later.
func (s *scope) queryColumn(id sql.ColumnId) (scopeColumn, bool) {
	for _, col := range s.cols {
		if sql.ColumnId(col.id) == id {
			return col, true
		}
	}
	return scopeColumn{}, false
}

// aggregateArgScope exposes only the namespaces used to resolve aggregate arguments at each query level.
func (s *scope) aggregateArgScope() *scope {
	source := s.querySource
	if source == nil {
		return nil
	}
	if source.aggregateArgs != nil {
		return source.aggregateArgs
	}
	visibleCtes := make(map[string]*scope)
	for lexical := source; lexical != nil; lexical = lexical.parent {
		for name, cte := range lexical.ctes {
			if _, found := visibleCtes[name]; !found {
				visibleCtes[name] = cte
			}
		}
	}
	argScope := &scope{
		colset:           source.colset,
		exprs:            source.exprs,
		tables:           source.tables,
		oldTables:        source.oldTables,
		selectAliases:    source.selectAliases,
		redirectCol:      source.redirectCol,
		ctes:             visibleCtes,
		b:                source.b,
		proc:             source.proc,
		activeSubquery:   source.querySubquery,
		outerQuery:       source.outerQuery,
		querySubquery:    source.querySubquery,
		insertTableAlias: source.insertTableAlias,
		cols:             source.cols,
		schemaName:       source.schemaName,
	}
	if source.outerQuery != nil {
		argScope.parent = source.outerQuery.aggregateArgScope()
	} else {
		argScope.parent = source.parent
	}
	argScope.querySource = argScope
	source.aggregateArgs = argScope
	return argScope
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
	fromScope.initGroupBy()

	g := fromScope.groupBy
	for _, e := range groupby {
		var col scopeColumn
		switch e := e.(type) {
		case *ast.ColName:
			var ok bool
			// GROUP BY binds to column references before projections.
			dbName := strings.ToLower(e.Qualifier.DbQualifier.String())
			tblName := strings.ToLower(e.Qualifier.Name.String())
			colName := strings.ToLower(e.Name.String())
			col, ok = fromScope.resolveColumn(dbName, tblName, colName, true, false)
			if !ok {
				col, ok = projScope.resolveColumn(dbName, tblName, colName, true, true)
			}

			if !ok {
				b.handleErr(sql.ErrColumnNotFound.New(e.Name.String()))
			}
		case *ast.SQLVal:
			// literal -> index into targets
			v, ok := b.normalizeIntVal(e)
			if !ok {
				b.handleErr(fmt.Errorf("expected integer order by literal"))
			}
			idx, _, err := types.Int64.Convert(b.ctx, v)
			if err != nil {
				b.handleErr(err)
			}
			intIdx, ok := idx.(int64)
			if !ok {
				b.handleErr(fmt.Errorf("expected integer order by literal"))
			}
			if intIdx < 1 {
				// TODO: this actually works in MySQL
				b.handleErr(fmt.Errorf("expected positive integer order by literal"))
			}
			if int(intIdx) > len(selects) {
				b.handleErr(fmt.Errorf("column ordinal out of range: %d", intIdx))
			}
			col = projScope.cols[intIdx-1]
		default:
			expr := b.buildScalar(fromScope, e)
			col = scopeColumn{
				col:      expr.String(),
				typ:      nil,
				scalar:   expr,
				nullable: expr.IsNullable(b.ctx),
			}
		}
		if col.scalar == nil {
			gf := expression.NewGetFieldWithTable(int(col.id), int(col.tableId), col.typ, col.db, col.table, col.col, col.nullable)
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

func (b *Builder) buildNameConst(fromScope *scope, f *ast.FuncExpr) sql.Expression {
	if len(f.Exprs) != 2 {
		b.handleErr(fmt.Errorf("incorrect parameter count in the call to native function NAME_CONST"))
	}
	alias := b.selectExprToExpression(fromScope, f.Exprs[0])
	aLit, ok := alias.(*expression.Literal)
	if !ok {
		b.handleErr(fmt.Errorf("incorrect arguments to: NAME_CONST"))
	}
	value := b.selectExprToExpression(fromScope, f.Exprs[1])
	vLit, ok := value.(*expression.Literal)
	if !ok {
		b.handleErr(fmt.Errorf("incorrect arguments to: NAME_CONST"))
	}
	var aliasStr string
	if types.IsText(aLit.Type(b.ctx)) {
		aliasStr = strings.Trim(aLit.String(), "'")
	} else {
		aliasStr = aLit.String()
	}
	return expression.NewAlias(b.ctx, aliasStr, vLit)
}

func (b *Builder) buildAggregation(fromScope, projScope *scope, groupingCols []sql.Expression, having *ast.Where) *scope {
	b.qFlags.Set(sql.QFlagAggregation)

	// GROUP_BY consists of:
	// - input arguments projection
	// - grouping cols projection
	// - aggregate expressions
	// - output projection
	fromScope.initGroupBy()

	group := fromScope.groupBy
	outScope := group.outScope
	// Select dependencies include aggregations and table columns needed for projections, having, and sort (order by)
	var selectDeps []sql.Expression
	var selectGfs []sql.Expression
	selectStr := make(map[string]bool)
	aliasDeps := make(map[string]bool)
	windowCols := make(map[columnId]struct{}, len(fromScope.windowFuncs))
	for _, col := range fromScope.windowFuncs {
		windowCols[col.id] = struct{}{}
	}
	var inspectSelectDeps func(sql.Expression, bool) bool
	inspectSelectDeps = func(expr sql.Expression, inAlias bool) (hasWindowDep bool) {
		transform.InspectExpr(b.ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				if _, ok := windowCols[columnId(e.Id())]; ok {
					hasWindowDep = true
					return true
				}
				colName := strings.ToLower(e.String())
				if !selectStr[colName] {
					selectDeps = append(selectDeps, e)
					selectGfs = append(selectGfs, e)
					selectStr[colName] = true
				}

				if isAliasDep, ok := aliasDeps[colName]; !ok && inAlias {
					aliasDeps[colName] = true
				} else if isAliasDep && !inAlias {
					aliasDeps[colName] = false
				}
			case *plan.Subquery:
				e.Correlated().ForEach(func(colId sql.ColumnId) {
					if correlated, found := projScope.parent.getCol(colId); found {
						hasWindowDep = inspectSelectDeps(correlated.scalarGf(), inAlias) || hasWindowDep
					}
				})
			}
			return false
		})
		return hasWindowDep
	}
	for _, e := range group.aggregations() {
		if !selectStr[strings.ToLower(e.String())] {
			selectDeps = append(selectDeps, e.scalar)
			selectGfs = append(selectGfs, e.scalarGf())
			selectStr[strings.ToLower(e.String())] = true
		}
	}
	for _, col := range fromScope.windowFuncs {
		inspectSelectDeps(col.scalar, false)
	}
	var aliases []sql.Expression
	for _, col := range projScope.cols {
		inAlias := false
		// eval aliases in project scope
		switch e := col.scalar.(type) {
		case *expression.Alias:
			if !e.Unreferencable() {
				inAlias = true
			}
		default:
		}

		hasWindowDep := inspectSelectDeps(col.scalar, inAlias)
		if inAlias && !hasWindowDep {
			aliases = append(aliases, col.scalar.(*expression.Alias).WithId(sql.ColumnId(col.id)).(*expression.Alias))
		}
	}
	for _, e := range fromScope.extraCols {
		// accessory cols used by ORDER_BY, HAVING
		if !selectStr[e.String()] {
			selectDeps = append(selectDeps, e.scalarGf())
			selectGfs = append(selectGfs, e.scalarGf())

			selectStr[e.String()] = true
		}
	}
	gb := plan.NewGroupBy(selectDeps, groupingCols, fromScope.node)
	outScope.node = gb

	if len(aliases) > 0 {
		outScope.node = plan.NewProject(b.ctx, append(selectGfs, aliases...), outScope.node).WithAliasDeps(aliasDeps)
	}

	b.buildHaving(fromScope, projScope, outScope, having)
	if len(fromScope.windowFuncs) > 0 {
		outScope.windowFuncs = fromScope.windowFuncs
		outScope = b.buildWindow(outScope, projScope)
	}
	return outScope
}

// IsAggregateFunc is a hacky "extension point" to allow for other dialects to declare additional aggregate functions
var IsAggregateFunc = IsMySQLAggregateFuncName

func IsMySQLAggregateFuncName(ctx *sql.Context, name string) (bool, error) {
	switch name {
	case "avg", "bit_and", "bit_or", "bit_xor", "count",
		"group_concat", "json_arrayagg", "json_objectagg",
		"max", "min", "std", "stddev_pop", "stddev_samp",
		"stddev", "sum", "var_pop", "var_samp", "variance",
		"first", "last", "any_value":
		return true, nil
	default:
		return false, nil
	}
}

// buildAggregateFunc tags aggregate functions in the correct scope
// and makes the aggregate available for reference by other clauses.
func (b *Builder) buildAggregateFunc(inScope *scope, name string, e *ast.FuncExpr) sql.Expression {
	if strings.EqualFold(name, "count") {
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			inScope.initGroupBy()
			return b.buildCountStarAggregate(e, inScope.groupBy)
		}
	}

	if strings.EqualFold(name, "jsonarray") {
		// TODO we don't have any tests for this
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			inScope.initGroupBy()
			return b.buildJsonArrayStarAggregate(inScope.groupBy)
		}
	}

	if strings.EqualFold(name, "any_value") {
		b.qFlags.Set(sql.QFlagAnyAgg)
	}

	argScope := inScope.aggregateArgScope()
	if argScope == nil {
		argScope = inScope
	}
	correlations := inScope.aggregateCorrelationSnapshot()
	args := b.buildAggFunctionArgs(argScope, e)
	var gb *groupBy
	aggScope := inScope.aggregateScope(b.ctx, args)
	if aggScope == nil {
		aggScope = inScope
	}
	aggScope.initGroupBy()
	gb = aggScope.groupBy
	b.addOuterAggregateArgDeps(aggScope, args)
	b.addAggFunctionArgs(gb, args)
	agg := b.newAggregation(e, name, args)

	if name == "count" {
		b.qFlags.Set(sql.QFlagCount)
	}

	aggType := agg.Type(b.ctx)

	aggName := strings.ToLower(plan.AliasSubqueryString(b.ctx, agg))
	if gf := gb.getAggRef(aggName); gf != nil {
		// if we've already computed use reference here
		b.recordOuterAggregateArgDeps(inScope, aggScope, correlations, sql.ColumnId(gf.(*expression.GetField).Id()))
		return gf
	}
	col := scopeColumn{col: aggName, scalar: agg, typ: aggType, nullable: agg.IsNullable(b.ctx)}
	id := gb.outScope.newColumn(col)

	agg = agg.WithId(sql.ColumnId(id)).(sql.Aggregation)
	gb.outScope.cols[len(gb.outScope.cols)-1].scalar = agg
	col.scalar = agg

	col.id = id
	gb.addAggStr(col)
	b.recordOuterAggregateArgDeps(inScope, aggScope, correlations, sql.ColumnId(id))
	return col.scalarGf()
}

// newAggregation creates a new aggregation function instance from the arguments given
func (b *Builder) newAggregation(e *ast.FuncExpr, name string, args []sql.Expression) sql.Aggregation {
	var agg sql.Aggregation
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

		f, ok := b.cat.Function(b.ctx, e.Qualifier.String(), name)
		if !ok {
			// todo(max): similar names in registry?
			err := sql.ErrFunctionNotFound.New(name)
			b.handleErr(err)
		}

		newInst, err := f.NewInstance(b.ctx, args)
		if err != nil {
			b.handleErr(err)
		}

		agg, ok = newInst.(sql.Aggregation)
		if !ok {
			err := fmt.Errorf("expected function to be aggregation: %s", f.FunctionName())
			b.handleErr(err)
		}
	}
	return agg
}

// buildAggFunctionArgs builds the arguments for an aggregate function
func (b *Builder) buildAggFunctionArgs(inScope *scope, e *ast.FuncExpr) []sql.Expression {
	var args []sql.Expression
	for _, arg := range e.Exprs {
		windowCount := len(inScope.windowFuncs)
		e := b.selectExprToExpression(inScope, arg)
		if len(inScope.windowFuncs) > windowCount {
			b.handleErr(sql.ErrNonAggregatedColumnWithoutGroupBy.New())
		}
		// if GetField is an alias, alias must be masking a column
		if gf, ok := e.(*expression.GetField); ok && gf.TableId() == 0 && !inScope.isCorrelatedColumn(gf.Id()) {
			e = b.selectExprToExpression(inScope.parent, arg)
		}
		switch e := e.(type) {
		case *expression.GetField:
			if e.TableId() == 0 && !inScope.isCorrelatedColumn(e.Id()) {
				b.handleErr(fmt.Errorf("failed to resolve aggregate column argument: %s", e))
			}
			args = append(args, e)
		case *expression.Star:
			err := sql.ErrStarUnsupported.New()
			b.handleErr(err)
		case *plan.Subquery:
			args = append(args, e)
		default:
			args = append(args, e)
		}
	}
	return args
}

// addAggFunctionArgs records aggregate inputs in the scope that owns the aggregate.
func (b *Builder) addAggFunctionArgs(gb *groupBy, args []sql.Expression) {
	for _, arg := range args {
		var col scopeColumn
		switch arg := arg.(type) {
		case *expression.GetField:
			col = scopeColumn{tableId: arg.TableID(), db: arg.Database(), table: arg.Table(), col: arg.Name(), scalar: arg, typ: arg.Type(b.ctx), nullable: arg.IsNullable(b.ctx)}
		case *plan.Subquery:
			col = scopeColumn{col: arg.QueryString, scalar: arg, typ: arg.Type(b.ctx)}
		default:
			col = scopeColumn{col: arg.String(), scalar: arg, typ: arg.Type(b.ctx)}
		}
		gb.addInCol(col)
	}
}

// addOuterAggregateArgDeps projects outer arguments needed to evaluate an aggregate owned by a nested query.
func (b *Builder) addOuterAggregateArgDeps(aggScope *scope, args []sql.Expression) {
	for _, arg := range args {
		sql.Inspect(b.ctx, arg, func(ctx *sql.Context, expr sql.Expression) bool {
			if _, ok := expr.(*plan.Subquery); ok {
				return false
			}
			gf, ok := expr.(*expression.GetField)
			if !ok {
				return true
			}
			for outer := aggScope.outerQuery; outer != nil; outer = outer.outerQuery {
				col, found := outer.queryColumn(gf.Id())
				if !found {
					continue
				}
				col.scalar = col.scalarGf()
				outer.addExtraColumn(col)
				break
			}
			return false
		})
	}
}

// aggregateCorrelationSnapshot preserves correlations established before resolving an aggregate's arguments.
func (s *scope) aggregateCorrelationSnapshot() sql.ColSet {
	if s.querySubquery == nil {
		return sql.ColSet{}
	}
	return s.querySubquery.correlated.Copy()
}

// recordOuterAggregateArgDeps replaces correlations introduced by outer aggregate arguments with the result column.
func (b *Builder) recordOuterAggregateArgDeps(inScope, aggScope *scope, before sql.ColSet, aggId sql.ColumnId) {
	if inScope.querySource == aggScope || inScope.querySubquery == nil {
		return
	}
	inScope.querySubquery.correlated = before
	inScope.querySubquery.correlated.Add(aggId)
}

// buildJsonArrayStarAggregate builds a JSON_ARRAY(*) aggregate function
func (b *Builder) buildJsonArrayStarAggregate(gb *groupBy) sql.Expression {
	var agg sql.Aggregation
	agg = aggregation.NewJsonArray(expression.NewLiteral(expression.NewStar(), types.Int64))
	b.qFlags.Set(sql.QFlagStar)

	// if e.Distinct {
	//	agg = plan.NewDistinct(expression.NewLiteral(1, types.Int64))
	// }
	aggName := strings.ToLower(agg.String())
	gf := gb.getAggRef(aggName)
	if gf != nil {
		// if we've already computed use reference here
		return gf
	}

	col := scopeColumn{col: strings.ToLower(agg.String()), scalar: agg, typ: agg.Type(b.ctx), nullable: agg.IsNullable(b.ctx)}
	id := gb.outScope.newColumn(col)

	agg = agg.WithId(sql.ColumnId(id)).(*aggregation.JsonArray)
	gb.outScope.cols[len(gb.outScope.cols)-1].scalar = agg
	col.scalar = agg

	col.id = id
	gb.addAggStr(col)
	return col.scalarGf()
}

// buildCountStarAggregate builds a COUNT(*) aggregate function
func (b *Builder) buildCountStarAggregate(e *ast.FuncExpr, gb *groupBy) sql.Expression {
	var agg sql.Aggregation
	if e.Distinct {
		agg = aggregation.NewCountDistinct(expression.NewLiteral(1, types.Int64))
	} else {
		agg = aggregation.NewCount(expression.NewLiteral(1, types.Int64))
	}
	b.qFlags.Set(sql.QFlagCountStar)
	aggName := strings.ToLower(agg.String())
	gf := gb.getAggRef(aggName)
	if gf != nil {
		// if we've already computed use reference here
		return gf
	}

	col := scopeColumn{col: strings.ToLower(agg.String()), scalar: agg, typ: agg.Type(b.ctx), nullable: agg.IsNullable(b.ctx)}
	id := gb.outScope.newColumn(col)
	col.id = id

	agg = agg.WithId(sql.ColumnId(id)).(sql.Aggregation)
	gb.outScope.cols[len(gb.outScope.cols)-1].scalar = agg
	col.scalar = agg

	gb.addAggStr(col)
	return col.scalarGf()
}

// buildGroupConcat builds a GROUP_CONCAT aggregate function
func (b *Builder) buildGroupConcat(inScope *scope, e *ast.GroupConcatExpr) sql.Expression {
	argScope := inScope.aggregateArgScope()
	if argScope == nil {
		argScope = inScope
	}
	correlations := inScope.aggregateCorrelationSnapshot()

	args := make([]sql.Expression, len(e.Exprs))
	for i, a := range e.Exprs {
		args[i] = b.selectExprToExpression(argScope, a)
	}

	separatorS := ","
	if !e.Separator.DefaultSeparator {
		separatorS = e.Separator.SeparatorString
	}

	orderByScope := b.analyzeOrderBy(argScope, argScope, e.OrderBy)
	sortConditions := b.buildSortConditions(orderByScope, transform.SameTree)
	deps := append(sortConditions.ToExpressions(), args...)
	aggScope := inScope.aggregateScope(b.ctx, deps)
	if aggScope == nil {
		aggScope = inScope
	}
	aggScope.initGroupBy()
	gb := aggScope.groupBy
	b.addOuterAggregateArgDeps(aggScope, deps)
	b.addAggFunctionArgs(gb, deps)

	// TODO: this should be acquired at runtime, not at parse time, so fix this
	gcml, err := b.ctx.GetSessionVariable(b.ctx, "group_concat_max_len")
	if err != nil {
		b.handleErr(err)
	}
	groupConcatMaxLen := gcml.(uint64)

	agg := aggregation.NewGroupConcat(e.Distinct, sortConditions, separatorS, args, int(groupConcatMaxLen))
	aggName := strings.ToLower(plan.AliasSubqueryString(b.ctx, agg))
	if gf := gb.getAggRef(aggName); gf != nil {
		b.recordOuterAggregateArgDeps(inScope, aggScope, correlations, sql.ColumnId(gf.(*expression.GetField).Id()))
		return gf
	}
	col := scopeColumn{col: aggName, scalar: agg, typ: agg.Type(b.ctx), nullable: agg.IsNullable(b.ctx)}

	id := gb.outScope.newColumn(col)

	agg = agg.WithId(sql.ColumnId(id)).(*aggregation.GroupConcat)
	gb.outScope.cols[len(gb.outScope.cols)-1].scalar = agg
	col.scalar = agg

	gb.addAggStr(col)
	col.id = id
	b.recordOuterAggregateArgDeps(inScope, aggScope, correlations, sql.ColumnId(id))
	return col.scalarGf()
}

// IsWindowFunc is a hacky "extension point" to allow for other dialects to declare additional window functions
var IsWindowFunc = IsMySQLWindowFuncName

func IsMySQLWindowFuncName(ctx *sql.Context, name string) (bool, error) {
	switch name {
	case "first", "last", "count", "sum", "any_value", "bit_and", "bit_or", "bit_xor",
		"avg", "max", "min", "count_distinct", "json_arrayagg",
		"row_number", "percent_rank", "lead", "lag",
		"first_value", "last_value",
		"rank", "dense_rank",
		"ntile",
		"std", "stddev", "stddev_pop", "stddev_samp",
		"variance", "var_pop", "var_samp":
		return true, nil
	default:
		return false, nil
	}
}

func (b *Builder) buildWindowFunc(inScope *scope, name string, e *ast.FuncExpr, over *ast.WindowDef) sql.Expression {
	// internal expressions can be complex, but window can't be more than alias
	var args []sql.Expression
	for _, arg := range e.Exprs {
		e := b.selectExprToExpression(inScope, arg)
		args = append(args, e)
	}

	var win sql.WindowAdaptableExpression
	if name == "count" {
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			win = aggregation.NewCount(expression.NewLiteral(1, types.Int64))
			b.qFlags.Set(sql.QFlagCountStar)
		}
	}
	if win == nil {
		f, ok := b.cat.Function(b.ctx, e.Qualifier.String(), name)
		if !ok {
			// todo(max): similar names in registry?
			err := sql.ErrFunctionNotFound.New(name)
			b.handleErr(err)
		}

		newInst, err := f.NewInstance(b.ctx, args)
		if err != nil {
			b.handleErr(err)
		}
		b.validateDistinctWindow(e, name, newInst)

		win, ok = newInst.(sql.WindowAdaptableExpression)
		if !ok {
			err := fmt.Errorf("function is not a window adaptable exprssion: %s", f.FunctionName())
			b.handleErr(err)
		}
	}

	def := b.buildWindowDef(inScope, over)
	switch w := win.(type) {
	case sql.WindowAdaptableExpression:
		win = w.WithWindow(b.ctx, def)
	}

	col := scopeColumn{col: strings.ToLower(win.String()), scalar: win, typ: win.Type(b.ctx), nullable: win.IsNullable(b.ctx)}
	id := inScope.newColumn(col)
	col.id = id
	win = win.WithId(sql.ColumnId(id)).(sql.WindowAdaptableExpression)
	inScope.cols[len(inScope.cols)-1].scalar = win
	col.scalar = win
	inScope.windowFuncs = append(inScope.windowFuncs, col)
	return col.scalarGf()
}

// validateDistinctWindow lets resolved function implementations reject unsupported DISTINCT window calls.
func (b *Builder) validateDistinctWindow(e *ast.FuncExpr, name string, expr sql.Expression) {
	if !e.Distinct {
		return
	}
	if validator, ok := expr.(sql.DistinctWindowFunctionValidator); ok {
		if err := validator.ValidateDistinctWindow(e.Qualifier.String(), name); err != nil {
			b.handleErr(err)
		}
	}
}

func (b *Builder) buildWindow(fromScope, projScope *scope) *scope {
	if len(fromScope.windowFuncs) == 0 {
		return fromScope
	}
	// passthrough dependency cols plus window funcs
	var selectExprs []sql.Expression
	var selectGfs []sql.Expression
	selectStr := make(map[string]bool)
	windowStr := make(map[string]bool)
	for _, col := range fromScope.windowFuncs {
		e := col.scalar
		if !windowStr[e.String()] || expressionIsNonDeterministic(b.ctx, e) {
			switch e.(type) {
			case sql.WindowAdaptableExpression:
				windowStr[e.String()] = true
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
				aliases = append(aliases, e.WithId(sql.ColumnId(col.id)).(*expression.Alias))
			}
		default:
		}

		// projection dependencies -> table cols needed above
		var findSelectDeps func(*sql.Context, sql.Expression) bool
		findSelectDeps = func(ctx *sql.Context, e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				colName := strings.ToLower(e.String())
				if !selectStr[colName] {
					selectExprs = append(selectExprs, e)
					selectGfs = append(selectGfs, e)
					selectStr[colName] = true
				}
			case *plan.Subquery:
				e.Correlated().ForEach(func(colId sql.ColumnId) {
					if correlated, found := projScope.parent.getCol(colId); found {
						findSelectDeps(ctx, correlated.scalarGf())
					}
				})
			default:
			}
			return false
		}
		transform.InspectExpr(b.ctx, col.scalar, findSelectDeps)
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
		outScope.node = plan.NewProject(b.ctx, append(selectGfs, aliases...), outScope.node)
	}

	return outScope
}

// expressionIsNonDeterministic reports whether an expression tree contains a nondeterministic expression.
func expressionIsNonDeterministic(ctx *sql.Context, expr sql.Expression) bool {
	return transform.InspectExpr(ctx, expr, func(_ *sql.Context, child sql.Expression) bool {
		nondeterministic, ok := child.(sql.NonDeterministicExpression)
		return ok && nondeterministic.IsNonDeterministic()
	})
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
		cur, ok := adj[name]
		if !ok {
			b.handleErr(sql.ErrUnknownWindowName.New(name))
		}
		seen[name] = true
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

	// TODO: We might be able to reuse b.buildSortConditions with some refactoring
	sortConditions := make(sql.SortConditions, len(def.OrderBy))
	for i, c := range def.OrderBy {
		// resolve col in fromScope
		e := b.buildScalar(fromScope, c.Expr)
		so := sql.Ascending
		if c.Direction == ast.DescScr {
			so = sql.Descending
		}
		sortConditions[i] = sql.SortCondition{
			Expr:         e,
			Order:        so,
			NullOrdering: nullOrdering(sortNullsLast(c, so == sql.Descending)),
		}
	}

	partitions := make([]sql.Expression, len(def.PartitionBy))
	for i, expr := range def.PartitionBy {
		partitions[i] = b.buildScalar(fromScope, expr)
	}

	frame := b.NewFrame(fromScope, def.Frame)

	windowDef := sql.NewWindowDefinition(partitions, sortConditions, frame, def.NameRef.Lowered(), def.Name.Lowered())
	if nameRef := def.NameRef.Lowered(); nameRef != "" {
		ref, ok := fromScope.windowDefs[nameRef]
		if !ok {
			b.handleErr(sql.ErrUnknownWindowName.New(nameRef))
		}
		// this is only safe if windows are built in topo order
		windowDef = b.mergeWindowDefs(windowDef, ref)
		// collapse dependencies if any reference this window
		fromScope.windowDefs[windowDef.Name] = windowDef
	}

	// According to MySQL documentation at https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
	// "If OVER() is empty, the window consists of all query rows and the window function computes a result using all rows."
	// This must be evaluated after merging with any referenced named window (OVER w), since the
	// referenced window's ORDER BY determines the correct default frame, not this def's own (possibly empty) ORDER BY.
	if len(windowDef.OrderBy) == 0 && windowDef.Frame == nil {
		windowDef.Frame = plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame()
	}

	return windowDef
}

// windowDisplayName returns a human-readable label for a window definition, for use in error
// messages. Anonymous windows (an inline OVER (w ...) clause rather than a named WINDOW w AS (...))
// have no name of their own, so they fall back to a generic label.
func windowDisplayName(def *sql.WindowDefinition) string {
	if def.Name != "" {
		return def.Name
	}
	return "OVER clause"
}

// mergeWindowDefs combines the attributes of two window definitions or returns
// an error if the two are incompatible. [def] should have a reference to
// [ref] through [def.Ref], and the return value drops the reference to indicate
// the two were properly combined.
func (b *Builder) mergeWindowDefs(def, ref *sql.WindowDefinition) *sql.WindowDefinition {
	if ref.Ref != "" {
		panic("unreachable; cannot merge unresolved window definition")
	}

	var orderBy sql.SortConditions
	switch {
	case len(def.OrderBy) > 0 && len(ref.OrderBy) > 0:
		err := sql.ErrInvalidWindowInheritance.New(windowDisplayName(def), def.Ref, "both contain order by clause")
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
		err := sql.ErrInvalidWindowInheritance.New(windowDisplayName(def), def.Ref, "both contain partition by clause")
		b.handleErr(err)
	case len(def.PartitionBy) > 0:
		partitionBy = def.PartitionBy
	case len(ref.PartitionBy) > 0:
		partitionBy = ref.PartitionBy
	default:
		partitionBy = []sql.Expression{}
	}

	_, isDefDefaultFrame := def.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)
	_, isRefDefaultFrame := ref.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)

	var frame sql.WindowFrame
	switch {
	case def.Frame != nil && ref.Frame != nil:
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
				err := sql.ErrInvalidWindowInheritance.New(windowDisplayName(def), def.Ref, "both contain different frame clauses")
				b.handleErr(err)
			}
			frame = def.Frame
		}
	case isDefDefaultFrame, isRefDefaultFrame:
		// The default frame was only correct in the context of the (partial) window definition
		// that computed it, which may not have known about an ORDER BY contributed by the other
		// side of this merge. Leave frame unset so the caller re-derives the correct default from
		// the fully merged ORDER BY, rather than blindly propagating a stale sentinel value.
	case def.Frame != nil:
		frame = def.Frame
	case ref.Frame != nil:
		frame = ref.Frame
	default:
	}

	return sql.NewWindowDefinition(partitionBy, orderBy, frame, "", def.Name)
}

func (b *Builder) analyzeHaving(fromScope, projScope *scope, having *ast.Where) {
	// Resolve the HAVING expression early so its aggregates are registered with
	// the query scopes that own their resolved arguments. buildHaving caches the
	// expression for reuse after the aggregation node has been constructed.
	b.buildHaving(fromScope, projScope, nil, having)
}

// addHavingDeps preserves source columns needed by HAVING expressions and their aggregates.
func (b *Builder) addHavingDeps(fromScope *scope, having sql.Expression) {
	addField := func(gf *expression.GetField) {
		col, found := fromScope.queryColumn(gf.Id())
		if !found {
			return
		}
		col.scalar = expression.NewGetFieldWithTable(int(col.id), 0, col.typ, col.db, col.table, col.col, col.nullable)
		fromScope.addExtraColumn(col)
	}
	addAggregateFields := func(id sql.ColumnId) bool {
		for _, col := range fromScope.groupBy.aggregations() {
			if sql.ColumnId(col.id) != id {
				continue
			}
			sql.Inspect(b.ctx, col.scalar, func(ctx *sql.Context, expr sql.Expression) bool {
				if gf, ok := expr.(*expression.GetField); ok {
					addField(gf)
					return false
				}
				return true
			})
			return true
		}
		return false
	}
	sql.Inspect(b.ctx, having, func(ctx *sql.Context, expr sql.Expression) bool {
		if _, ok := expr.(*plan.Subquery); ok {
			return false
		}
		if gf, ok := expr.(*expression.GetField); ok {
			if gf.TableId() == 0 && addAggregateFields(gf.Id()) {
				return false
			}
			if _, found := fromScope.queryColumn(gf.Id()); found {
				addField(gf)
			}
			return false
		}
		return true
	})
}

func (b *Builder) buildInnerProj(fromScope, projScope *scope) *scope {
	outScope := fromScope
	var proj []sql.Expression

	// eval aliases in project scope
	for i, col := range projScope.cols {
		switch e := col.scalar.(type) {
		case *expression.Alias:
			if !e.Unreferencable() {
				proj = append(proj, e.WithId(sql.ColumnId(col.id)).(*expression.Alias))
				if exprReturnsRowIter(b.ctx, e) {
					// This projection expands expressions that return a RowIter (set-returning functions)
					// into multiple rows. The final projection must reference the expanded column rather
					// than re-evaluate the expression, which would multiply the rows again.
					projScope.cols[i].scalar = col.scalarGf()
				}
			}
		}
	}

	aliasCnt := len(proj)

	if len(proj) == 0 && !(len(fromScope.cols) == 1 && fromScope.cols[0].id == 0) {
		// remove redundant projection unless it is the single dual table column
		return outScope
	}

	for _, c := range fromScope.cols {
		proj = append(proj, c.scalarGf())
	}

	// todo: fulltext indexes depend on match alias first
	proj = append(proj[aliasCnt:], proj[:aliasCnt]...)

	if len(proj) > 0 {
		outScope.node = plan.NewProject(b.ctx, proj, outScope.node)
	}

	return outScope
}

func (b *Builder) buildHaving(fromScope, projScope, outScope *scope, having *ast.Where) {
	if having == nil {
		return
	}
	// HAVING is resolved during analysis, before the aggregation node exists.
	// Reuse that expression when this later call attaches the filter to the plan.
	if fromScope.having != nil {
		if outScope != nil {
			outScope.node = plan.NewHaving(fromScope.having, outScope.node)
		}
		return
	}
	fromScope.initGroupBy()

	// HAVING can reference grouped inputs, aggregate results, and SELECT aliases.
	// Build a namespace with that precedence while retaining the enclosing query
	// chain needed by correlated subqueries.
	havingScope := b.newScope()
	havingScope.querySource = fromScope
	havingScope.outerQuery = fromScope.outerQuery
	havingScope.querySubquery = fromScope.querySubquery
	if fromScope.parent != nil {
		havingScope.parent = fromScope.parent
		havingScope.parent.selectAliases = fromScope.selectAliases
	}

	// Include columns referenced by GROUP BY and aggregate inputs.
	for _, c := range fromScope.groupBy.inCols {
		if !havingScope.colset.Contains(sql.ColumnId(c.id)) {
			havingScope.addColumn(c)
		}
	}
	// Include source columns referenced inside registered aggregate expressions.
	for _, c := range fromScope.groupBy.aggregations() {
		transform.InspectExpr(b.ctx, c.scalar, func(ctx *sql.Context, e sql.Expression) bool {
			gf, ok := e.(*expression.GetField)
			if !ok {
				return false
			}
			col, found := fromScope.resolveColumn(gf.Database(), gf.Table(), gf.Name(), false, false)
			if found && !havingScope.colset.Contains(sql.ColumnId(col.id)) {
				havingScope.addColumn(col)
			}
			return false
		})
	}
	// Add SELECT outputs after source columns so an identically named alias does
	// not mask a source column. Plain column aliases also retain their unaliased
	// source name, which MySQL permits in HAVING.
	for _, c := range projScope.cols {
		if !havingScope.colset.Contains(sql.ColumnId(c.id)) {
			havingScope.addColumn(c)
		}
		alias, isAlias := c.scalar.(*expression.Alias)
		if !isAlias {
			continue
		}
		gf, isGetField := alias.Child.(*expression.GetField)
		if !isGetField {
			continue
		}
		col, found := fromScope.resolveColumn(gf.Database(), gf.Table(), gf.Name(), false, false)
		if found && !havingScope.colset.Contains(sql.ColumnId(col.id)) {
			havingScope.addColumn(col)
		}
	}

	havingScope.groupBy = fromScope.groupBy
	fromScope.having = b.buildScalar(havingScope, having.Expr)
	b.addHavingDeps(fromScope, fromScope.having)
	if outScope != nil {
		outScope.node = plan.NewHaving(fromScope.having, outScope.node)
	}
}

// exprReturnsRowIter returns whether any expression in the tree rooted at |e| returns a RowIter rather than a
// scalar value (sql.RowIterExpression), i.e. a set-returning function.
func exprReturnsRowIter(ctx *sql.Context, e sql.Expression) bool {
	return transform.InspectExpr(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
		rie, ok := e.(sql.RowIterExpression)
		return ok && rie.ReturnsRowIter()
	})
}
