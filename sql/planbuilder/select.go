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
	// General order of binding:
	// 1) Get definitions in FROM.
	// 2) Build WHERE, which can only reference FROM columns.
	// 3) Bookkeep aggregation/window function usage in higher-scopes
	//    (GROUP BY, WINDOW, HAVING, SELECT, ORDER BY).
	// 4) Construct either i) aggregation, ii) window, or iii) projection over
	//    FROM clause providing expressions used in (2) (including aliases).
	// 5) Build top-level scopes, replacing aggregation and aliases with
	//    projections from (4).
	// 6) Finish with final target projections.
	fromScope := b.buildFrom(inScope, s.From)
	if cn, ok := fromScope.node.(sql.CommentedNode); ok && len(s.Comments) > 0 {
		fromScope.node = cn.WithComment(string(s.Comments[0]))
	}

	// Resolve and fold named window definitions
	b.buildNamedWindows(fromScope, s.Window)

	b.buildWhere(fromScope, s.Where)
	projScope := fromScope.replace()

	// Aggregates in select list added to fromScope.groupBy.outCols.
	// Args to aggregates added to fromScope.groupBy.inCols.
	b.analyzeProjectionList(fromScope, projScope, s.SelectExprs)

	// Find aggregations in order by
	orderByScope := b.analyzeOrderBy(fromScope, projScope, s.OrderBy)

	// Find aggregations in having
	b.analyzeHaving(fromScope, projScope, s.Having)

	// At this point we've recorded dependencies for higher-level scopes,
	// so we can build the FROM clause
	if b.needsAggregation(fromScope, s) {
		groupingCols := b.buildGroupingCols(fromScope, projScope, s.GroupBy, s.SelectExprs)
		outScope = b.buildAggregation(fromScope, projScope, groupingCols)
	} else if fromScope.windowFuncs != nil {
		outScope = b.buildWindow(fromScope, projScope)
	} else {
		outScope = b.buildInnerProj(fromScope, projScope)
	}

	// At this point, we've combined table relations, performed aggregations,
	// and projected aliases used in higher-level clauses. Aliases and agg
	// expressions in higher level scopes will be replaced with GetField
	// references.

	b.buildHaving(fromScope, projScope, outScope, s.Having)
	b.buildOrderBy(outScope, orderByScope)

	offset := b.buildOffset(outScope, s.Limit)
	if offset != nil {
		outScope.node = plan.NewOffset(offset, outScope.node)
	}
	limit := b.buildLimit(outScope, s.Limit)
	if limit != nil {
		outScope.node = plan.NewLimit(limit, outScope.node)
	}

	// Last level projection restricts outputs to target projections.
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
		rowCount := b.buildScalar(inScope, limit.Offset)
		// Check if offset starts at 0, if so, we can just remove the offset node.
		// Only cast to int8, as a larger int type just means a non-zero offset.
		if val, err := rowCount.Eval(b.ctx, nil); err == nil {
			if v, ok := val.(int8); ok && v == 0 {
				return nil
			}
		}
		return rowCount
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
