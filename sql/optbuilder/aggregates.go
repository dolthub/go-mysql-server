package optbuilder

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strings"
)

var _ ast.Expr = (*aggregateInfo)(nil)

type groupBy struct {
	inCols   []scopeColumn
	outScope *scope
	aggs     map[string]sql.Expression
	grouping map[string]bool
}

func (g *groupBy) addInCol(c scopeColumn) {
	g.inCols = append(g.inCols, c)
}

func (g *groupBy) addOutCol(c scopeColumn) {
	g.outScope.addColumn(c)
}

func (g *groupBy) hasAggs() bool {
	return len(g.aggs) > 0
}

func (g *groupBy) aggregations() []sql.Expression {
	aggregations := make([]sql.Expression, 0, len(g.aggs))
	for _, agg := range g.aggs {
		aggregations = append(aggregations, agg)
	}
	return aggregations
}

func (g *groupBy) addAggStr(e sql.Expression) {
	if g.aggs == nil {
		g.aggs = make(map[string]sql.Expression)
	}
	g.aggs[strings.ToLower(e.String())] = e
}

func (g *groupBy) getAgg(name string) sql.Expression {
	if g.aggs == nil {
		return nil
	}
	ret, _ := g.aggs[name]
	return ret
}

type aggregateInfo struct {
	ast.Expr
}

func (b *PlanBuilder) needsAggregation(fromScope *scope, sel *ast.Select) bool {
	return len(sel.GroupBy) > 0 ||
		sel.Having != nil ||
		(fromScope.groupBy != nil && fromScope.groupBy.hasAggs())
}

func (b *PlanBuilder) buildGroupingCols(fromScope, projScope *scope, groupby ast.GroupBy, selects ast.SelectExprs) []sql.Expression {
	// grouping col will either be:
	// 1) alias into targets
	// 2) a column reference
	// 3) an index into selects
	// 4) a simple non-aggregate expression
	var groupings []sql.Expression
	g := fromScope.groupBy
	for _, e := range groupby {
		var col scopeColumn
		switch e := e.(type) {
		case *ast.ColName:
			// col in fromScope first
			name := strings.ToLower(e.Name.String())
			for _, c := range fromScope.cols {
				// match in-scope only
				if c.col == name {
					col = c
					break
				}
			}
			if col.table == "" {
				break
			}
			// fallback to alias in targets
			for _, c := range projScope.cols {
				// match alias in projection scope
				if c.col == name {
					col = c
					break
				}
			}
			if col.col == "" {
				b.handleErr(sql.ErrColumnNotFound.New(e.Name.String()))
			}
		case *ast.SQLVal:
			// literal -> index into targets
			if e.Type == ast.IntVal {
				lit := b.convertInt(string(e.Val), 10)
				idx, _, err := types.Int64.Convert(lit.Value())
				if err != nil {
					b.handleErr(err)
				}
				intIdx, ok := idx.(int64)
				if !ok {
					b.handleErr(fmt.Errorf("expected integer order by literal"))
				}
				col = projScope.cols[intIdx]
			}
		default:
			expr := b.buildScalar(fromScope, e)
			col = scopeColumn{
				table:    "",
				col:      expr.String(),
				typ:      nil,
				scalar:   expr,
				nullable: expr.IsNullable(),
			}
		}
		if _, ok := g.grouping[col.col]; ok {
			continue
		}
		g.addInCol(col)
		//g.addOutCol(col)
		scalar := col.scalar
		if col.scalar == nil {
			id := b.exprs[col.table+"."+col.col]
			scalar = expression.NewGetFieldWithTable(int(id), col.typ, col.table, col.col, col.nullable)
		}
		groupings = append(groupings, scalar)
	}

	return groupings
}

func (b *PlanBuilder) buildAggregation(fromScope, projScope *scope, having sql.Expression, groupingCols []sql.Expression) *scope {
	// GROUP_BY consists of:
	// - input arguments projection
	// - grouping cols projection
	// - aggregate expressions
	// - output projection
	// - HAVING filter
	if fromScope.groupBy == nil {
		fromScope.initGroupBy()
	}

	group := fromScope.groupBy
	outScope := group.outScope
	// select columns:
	//  - aggs
	//  - extra columns needed by having, order by, select
	var selectExprs []sql.Expression
	selectStr := make(map[string]bool)
	for _, e := range group.inCols {
		// arguments for aggregation selection
		if !selectStr[e.col] {
			selectExprs = append(selectExprs, e.scalar)
			selectStr[e.col] = true
		}
	}
	for _, e := range group.aggregations() {
		// aggregation functions
		if !selectStr[e.String()] {
			selectExprs = append(selectExprs, e)
			selectStr[e.String()] = true
		}
	}
	for _, e := range projScope.cols {
		// projection dependencies
		if e.table != "" && !selectStr[e.col] {
			selectExprs = append(selectExprs, e.scalar)
			selectStr[e.col] = true
		}
	}
	for _, e := range fromScope.extraCols {
		// accessory cols used by ORDER_BY, HAVING
		if !selectStr[e.col] {
			selectExprs = append(selectExprs, e.scalar)
			selectStr[e.col] = true
		}
	}
	gb := plan.NewGroupBy(selectExprs, groupingCols, fromScope.node)
	outScope.node = gb

	if having != nil {
		outScope.node = plan.NewHaving(having, outScope.node)
	}
	return outScope
}

func (b *PlanBuilder) buildAggregateFunc(inScope *scope, name string, e *ast.FuncExpr) sql.Expression {
	if inScope.groupBy == nil {
		inScope.initGroupBy()
	}
	gb := inScope.groupBy

	if name == "count" {
		if _, ok := e.Exprs[0].(*ast.StarExpr); ok {
			agg := aggregation.NewCount(expression.NewLiteral(1, types.Int64))
			gf := gb.getAgg(strings.ToLower(agg.String()))
			if gf != nil {
				// TODO check agg scope output, see if we've already computed
				// if so use reference here
				return gf
			}

			col := scopeColumn{col: agg.String(), scalar: agg, typ: agg.Type(), nullable: agg.IsNullable()}
			b.newColumn(gb.outScope, col)
			//gb.addOutCol(col)
			gb.addAggStr(agg)
			id := b.exprs[agg.String()]
			return expression.NewGetFieldWithTable(int(id), agg.Type(), "", agg.String(), agg.IsNullable())
		}
	}

	var args []sql.Expression
	outerLen := inScope.outerScopeLen()
	for _, arg := range e.Exprs {
		e := b.selectExprToExpression(inScope, arg)
		switch e := e.(type) {
		case *expression.GetField:
			gf := expression.NewGetFieldWithTable(outerLen+e.Index(), e.Type(), e.Table(), e.Name(), e.IsNullable())
			args = append(args, gf)
			col := scopeColumn{table: gf.Table(), col: gf.Name(), scalar: gf, typ: gf.Type(), nullable: gf.IsNullable()}
			gb.addInCol(col)
		case *expression.Star:
			panic("todo custom handle count(*)")
		default:
			args = append(args, e)
			col := scopeColumn{col: e.String(), scalar: e, typ: e.Type()}
			gb.addInCol(col)
		}
	}

	f, err := b.cat.Function(b.ctx, name)
	if err != nil {
		b.handleErr(err)
	}

	agg, err := f.NewInstance(args)
	if err != nil {
		b.handleErr(err)
	}

	gf := gb.getAgg(strings.ToLower(agg.String()))
	if gf != nil {
		// TODO check agg scope output, see if we've already computed
		// if so use reference here
		return gf
	}

	col := scopeColumn{col: agg.String(), scalar: agg, typ: agg.Type(), nullable: agg.IsNullable()}
	//gb.addOutCol(col)
	b.newColumn(gb.outScope, col)
	gb.addAggStr(agg)

	//TODO we need to return a reference here, so that top-level
	// projection references the group by output.
	id := b.exprs[agg.String()]
	return expression.NewGetFieldWithTable(int(id), agg.Type(), "", agg.String(), agg.IsNullable())
}

func isAggregateFunc(name string) bool {
	switch name {
	case "avg", "bit_and", "bit_or", "bit_xor", "count",
		"group_concat", "json_arrayagg", "json_objectagg",
		"max", "min", "std", "stddev_pop", "stddev_samp",
		"stddev", "sum", "var_pop", "var_samp", "variance":
		return true
	default:
		return false
	}
}

func isWindowFunc(name string) bool {
	switch name {
	case "first", "last", "count", "sum", "any_value",
		"avg", "max", "min", "count_distinct", "json_arrayagg",
		"row_number", "percent_rank", "lag", "first_value":
		return true
	default:
		return false
	}
}

func (b *PlanBuilder) analyzeHaving(fromScope *scope, having *ast.Where) {
	// build having filter expr
	// aggregates added to fromScope.groupBy
	// can see projScope outputs
	if having == nil {
		return
	}

	ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.FuncExpr:
			name := n.Name.Lowered()
			if isAggregateFunc(name) {
				// record aggregate
				_ = b.buildAggregateFunc(fromScope, name, n)
			} else if isWindowFunc(name) {
				panic("todo window funcs")
			}
		}
		return true, nil
	}, having)
}

func (b *PlanBuilder) buildHaving(fromScope, projScope *scope, having *ast.Where) sql.Expression {
	// expressions in having can be from aggOut or projScop
	if fromScope.groupBy == nil {
		fromScope.initGroupBy()
	}
	havingScope := fromScope.push()
	for _, c := range projScope.cols {
		if c.table == "" {
			havingScope.addColumn(c)
		}
	}
	return b.buildScalar(havingScope, having.Expr)
}
