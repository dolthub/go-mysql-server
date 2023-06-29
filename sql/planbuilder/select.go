package planbuilder

import (
	"fmt"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildSelectStmt(inScope *scope, s ast.SelectStatement) (outScope *scope) {
	switch s := s.(type) {
	case *ast.Select:
		if s.With != nil {
			cteScope := b.buildWith(inScope, s.With)
			return b.buildSelect(cteScope, s)
		}
		return b.buildSelect(inScope, s)
	case *ast.Union:
		if s.With != nil {
			cteScope := b.buildWith(inScope, s.With)
			return b.buildUnion(cteScope, s)
		}
		return b.buildUnion(inScope, s)
	case *ast.ParenSelect:
		return b.buildSelectStmt(inScope, s.Select)
	default:
		b.handleErr(fmt.Errorf("unknown select statement %T", s))
	}
	return
}

func (b *PlanBuilder) buildSelect(inScope *scope, s *ast.Select) (outScope *scope) {
	fromScope := b.buildFrom(inScope, s.From)
	if cn, ok := fromScope.node.(sql.CommentedNode); ok && len(s.Comments) > 0 {
		fromScope.node = cn.WithComment(string(s.Comments[0]))
	}

	// window defs
	// unique names, definitions available
	b.buildNamedWindows(fromScope, s.Window)

	b.buildWhere(fromScope, s.Where)
	projScope := fromScope.replace()

	// create SELECT list
	// aggregates in select list added to fromScope.groupBy.outCols
	// args to aggregates added to fromScope.groupBy.inCols
	// select gets ref of agg output
	b.analyzeProjectionList(fromScope, projScope, s.SelectExprs)

	// find aggregations in order by
	orderByScope := b.analyzeOrderBy(fromScope, projScope, s.OrderBy)

	// find aggregations in having
	b.analyzeHaving(fromScope, projScope, s.Having)

	needsAgg := b.needsAggregation(fromScope, s)
	if needsAgg {
		groupingCols := b.buildGroupingCols(fromScope, projScope, s.GroupBy, s.SelectExprs)
		having := b.buildHaving(fromScope, projScope, s.Having)
		// make PROJECT -> HAVING -> GROUP_BY
		outScope = b.buildAggregation(fromScope, projScope, having, groupingCols)
	} else if fromScope.windowFuncs != nil {
		having := b.buildHaving(fromScope, projScope, s.Having)
		outScope = b.buildWindow(fromScope, projScope, having)
	} else {
		having := b.buildHaving(fromScope, projScope, s.Having)
		outScope = b.buildInnerProj(fromScope, projScope, having)
	}

	b.buildOrderBy(outScope, orderByScope)

	offset := b.buildOffset(outScope, s.Limit)
	if offset != nil {
		outScope.node = plan.NewOffset(offset, outScope.node)
	}
	limit := b.buildLimit(outScope, s.Limit)
	if limit != nil {
		outScope.node = plan.NewLimit(limit, outScope.node)
	}
	b.buildProjection(outScope, projScope)
	outScope = projScope
	b.buildDistinct(outScope, s.Distinct)
	return
}

func (b *PlanBuilder) buildLimit(inScope *scope, limit *ast.Limit) sql.Expression {
	if limit != nil {
		return b.buildScalar(inScope, limit.Rowcount)
	}
	return nil
}

func (b *PlanBuilder) buildOffset(inScope *scope, limit *ast.Limit) sql.Expression {
	if limit != nil && limit.Offset != nil {
		return b.buildScalar(inScope, limit.Offset)
	}
	return nil
}

func (b *PlanBuilder) buildDistinct(inScope *scope, distinct string) {
	if distinct != "" {
		inScope.node = plan.NewDistinct(inScope.node)
	}
}

func (b *PlanBuilder) currentDb() sql.Database {
	if b.currentDatabase == nil {
		database, err := b.cat.Database(b.ctx, b.ctx.GetCurrentDatabase())
		if err != nil {
			b.handleErr(err)
		}

		if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
			database = privilegedDatabase.Unwrap()
		}
		b.currentDatabase = database
	}
	return b.currentDatabase
}

func (b *PlanBuilder) renameSource(scope *scope, table string, cols []string) {
	if table != "" {
		scope.setTableAlias(table)
	}
	if len(cols) > 0 {
		scope.setColAlias(cols)
	}
	for i, c := range scope.cols {
		c.scalar = nil
		scope.cols[i] = c
	}
}
