package planbuilder

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// factory functions are supposed to apply all optimizations to an expression
// that are always costing/simplification wins. Each function will be a series
// of optimizations local to this specific node. Eventually there should be a
// top level optimizer with a switch for every type.

func buildProject(p *plan.Project) (sql.Node, error) {
	{
		// todo generalize this
		if sqa, _ := p.Child.(*plan.SubqueryAlias); sqa != nil && p.Schema().Equals(sqa.Schema()) {
			return sqa, nil
		}
	}

	{
		// project->project=>project
		if p2, _ := p.Child.(*plan.Project); p2 != nil {
			if !containsSubqueryExpr(p.Projections) && !containsSubqueryExpr(p2.Projections) {
				// it is important to bisect subquery expression alias inputs
				// into a separate projection with current exec impl
				adjGraph := make(map[sql.ColumnId]sql.Expression, 0)
				for _, e := range p2.Projections {
					// inner projections track/collapse alias refs
					_, err := aliasTrackAndReplace(adjGraph, e)
					if err != nil {
						return nil, err
					}
				}

				var newP []sql.Expression
				for _, e := range p.Projections {
					//outer projections are the ones we want, with aliases replaced
					newE, err := aliasTrackAndReplace(adjGraph, e)
					if err != nil {
						return nil, err
					}
					newP = append(newP, newE)
				}
				return plan.NewProject(newP, p2.Child), nil
			}
		}
	}
	return p, nil
}

func containsSubqueryExpr(exprs []sql.Expression) bool {
	for _, e := range exprs {
		subqFound := transform.InspectExpr(e, func(e sql.Expression) bool {
			_, ok := e.(*plan.Subquery)
			return ok
		})
		if subqFound {
			return true
		}
	}
	return false
}

func aliasTrackAndReplace(adj map[sql.ColumnId]sql.Expression, e sql.Expression) (sql.Expression, error) {
	var id sql.ColumnId
	switch e := e.(type) {
	case *expression.Alias:
		id = e.Id()
	case *expression.GetField:
		id = sql.ColumnId(e.Index())
	default:
	}
	newE, _, err := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			if a, _ := adj[sql.ColumnId(e.Index())]; a != nil {
				return a, transform.NewTree, nil
			}
		default:
		}
		return e, transform.SameTree, nil
	})
	if err != nil {
		return nil, err
	}
	if id > 0 {
		adj[id] = newE
	}
	return newE, nil
}
