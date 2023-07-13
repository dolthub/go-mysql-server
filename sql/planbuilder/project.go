package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) analyzeProjectionList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	b.analyzeSelectList(inScope, outScope, selectExprs)
}

func (b *PlanBuilder) analyzeSelectList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	// todo ideally we would not create new expressions here.
	// we want to in-place identify aggregations, expand stars.
	// use inScope to construct projections for projScope

	tempScope := inScope.push()
	// need to transfer aggregation state from out -> in
	var exprs []sql.Expression
	for _, se := range selectExprs {
		pe := b.selectExprToExpression(tempScope, se)
		switch e := pe.(type) {
		case *expression.GetField:
			gf := expression.NewGetFieldWithTable(e.Index(), e.Type(), strings.ToLower(e.Table()), strings.ToLower(e.Name()), e.IsNullable())
			exprs = append(exprs, gf)
			id, ok := tempScope.getExpr(gf.String())
			if !ok {
				err := sql.ErrColumnNotFound.New(gf.String())
				b.handleErr(err)
			}
			gf = gf.WithIndex(int(id)).(*expression.GetField)
			outScope.addColumn(scopeColumn{table: gf.Table(), col: gf.Name(), scalar: gf, typ: gf.Type(), nullable: gf.IsNullable(), id: id})
		case *expression.Star:
			tableName := strings.ToLower(e.Table)
			for _, c := range inScope.cols {
				if c.table == tableName || tableName == "" {
					gf := expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
					exprs = append(exprs, gf)
					id, ok := inScope.getExpr(gf.String())
					if !ok {
						err := sql.ErrColumnNotFound.New(gf.String())
						b.handleErr(err)
					}
					outScope.addColumn(scopeColumn{table: c.table, col: c.col, scalar: gf, typ: gf.Type(), nullable: gf.IsNullable(), id: id})
				}
			}
		case *expression.Alias:
			var col scopeColumn
			if gf, ok := e.Child.(*expression.GetField); ok {
				id, ok := tempScope.getExpr(gf.String())
				if !ok {
					err := sql.ErrColumnNotFound.New(gf.String())
					b.handleErr(err)
				}
				col = scopeColumn{id: id, table: "", col: e.Name(), scalar: e, typ: gf.Type(), nullable: gf.IsNullable()}
			} else if sq, ok := e.Child.(*plan.Subquery); ok {
				col = scopeColumn{col: e.Name(), scalar: e, typ: sq.Type(), nullable: sq.IsNullable()}
			} else {
				col = scopeColumn{col: e.Name(), scalar: e, typ: e.Type(), nullable: e.IsNullable()}
			}
			if e.Unreferencable() {
				outScope.addColumn(col)
			} else {
				id := outScope.newColumn(col)
				col.id = id
				tempScope.addColumn(col)
			}
			exprs = append(exprs, e)
		default:
			exprs = append(exprs, pe)
			col := scopeColumn{col: pe.String(), scalar: pe, typ: pe.Type()}
			outScope.newColumn(col)
		}
	}

	// move aggregates, windows from tempScope->inScope
	if tempScope.groupBy != nil {
		if inScope.groupBy == nil {
			inScope.initGroupBy()
		}
		for _, col := range tempScope.groupBy.aggregations() {
			inScope.groupBy.addAggStr(col)
		}
		for _, col := range tempScope.groupBy.inCols {
			inScope.groupBy.addInCol(col)
		}
	}
	inScope.windowFuncs = append(inScope.windowFuncs, tempScope.windowFuncs...)
}

func (b *PlanBuilder) selectExprToExpression(inScope *scope, se ast.SelectExpr) sql.Expression {
	switch e := se.(type) {
	case *ast.StarExpr:
		if e.TableName.IsEmpty() {
			return expression.NewStar()
		}
		return expression.NewQualifiedStar(strings.ToLower(e.TableName.Name.String()))
	case *ast.AliasedExpr:
		expr := b.buildScalar(inScope, e.Expr)

		if !e.As.IsEmpty() {
			return expression.NewAlias(e.As.String(), expr)
		}

		if selectExprNeedsAlias(e, expr) {
			return expression.NewAlias(e.InputExpression, expr).AsUnreferencable()
		}

		return expr
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
	}
	return nil
}

func (b *PlanBuilder) buildProjection(inScope, outScope *scope) {
	projections := make([]sql.Expression, len(outScope.cols))
	for i, sc := range outScope.cols {
		scalar := sc.scalar
		if a, ok := sc.scalar.(*expression.Alias); ok && !a.Unreferencable() {
			// replace alias with its reference
			scalar = sc.scalarGf()
		}
		projections[i] = scalar
	}
	proj := plan.NewProject(projections, inScope.node)
	if _, ok := inScope.node.(*plan.SubqueryAlias); ok && proj.Schema().Equals(proj.Child.Schema()) {
		// pruneColumns can get overly aggressive
		outScope.node = inScope.node
	} else {
		outScope.node = proj
	}
}

func selectExprNeedsAlias(e *ast.AliasedExpr, expr sql.Expression) bool {
	if len(e.InputExpression) == 0 {
		return false
	}

	// We want to avoid unnecessary wrapping of aliases, but not at the cost of blowing up parse time. So we examine
	// the expression tree to see if is likely to need an alias without first serializing the expression being
	// examined, which can be very expensive in memory.
	complex := false
	sql.Inspect(expr, func(expr sql.Expression) bool {
		switch expr.(type) {
		case *plan.Subquery, *expression.UnresolvedFunction, *expression.Case, *expression.InTuple, *plan.InSubquery, *expression.HashInTuple:
			complex = true
			return false
		default:
			return true
		}
	})

	return complex || e.InputExpression != expr.String()
}
