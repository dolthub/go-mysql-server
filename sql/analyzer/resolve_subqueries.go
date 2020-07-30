package analyzer

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			a.Log("found subquery %q with child of type %T", n.Name(), n.Child)
			child, err := a.Analyze(ctx, n.Child, scope)
			if err != nil {
				return nil, err
			}

			return n.WithChildren(child)
		default:
			return n, nil
		}
	})
}

func resolveSubqueryExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*plan.Subquery)
		if !ok || s.Resolved() {
			return e, nil
		}

		subqueryCtx := ctx.NewSubContext(ctx.Context)
		subScope := scope.newScope(n)

		analyzed, err := a.Analyze(subqueryCtx, s.Query, subScope)
		if err != nil {
			// We ignore certain errors, deferring them to later analysis passes. Specifically, if the subquery isn't
			// resolved or a column can't be found in the scope node, wait until a later pass.
			// TODO: we won't be able to give the right error message in all cases when we do this, although we attempt to
			//  recover the actual error in the validation step.
			if ErrValidationResolved.Is(err) || sql.ErrTableColumnNotFound.Is(err) {
				// keep the work we have and defer remainder of analysis of this subquery until a later pass
				return s.WithQuery(analyzed), nil
			}
			return nil, err
		}

		if qp, ok := analyzed.(*plan.QueryProcess); ok {
			analyzed = qp.Child
		}

		return s.WithQuery(analyzed), nil
	})
}

// pullUpMissingSubqueryColumns examines subqueries to see which columns from outer scopes are missing in the scope
// node, and pulls them up. An additional higher-level projection is added as necessary.
func pullUpMissingSubqueryColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		e, ok := n.(sql.Expressioner)
		if !ok {
			return n, nil
		}

		var deferredColumns []*deferredColumn
		var exprs []sql.Expression
		for _, e := range e.Expressions() {
			// var subquery sql.Expression
			// if a, ok := e.(*expression.Alias); ok {
			//
			// }
			// TODO: handle aliases
			s, ok := e.(*plan.Subquery)
			if !ok {
				exprs = append(exprs, e)
				continue
			}

			// wrap any unaliased subqueries in an alias so that they can be identified by parent nodes during further analysis
			exprs = append(exprs, expression.NewAlias(s.QueryString, s))
			deferredColumns = append(deferredColumns, findDeferredColumns(s.Query)...)
		}

		if len(deferredColumns) > 0 {
			switch n.(type) {
			case *plan.Project:
				var err error
				n, err = replaceExpressions(n, exprs)
				if err != nil {
					return nil, err
				}
			}

			return addDeferredColumns(n, deferredColumns)
		}

		return n, nil
	})
}

func replaceExpressions(n sql.Node, exprs []sql.Expression) (sql.Node, error) {
	switch nn := n.(type) {
	case sql.Expressioner:
		return nn.WithExpressions(exprs...)
	default:
		panic(fmt.Sprintf("Cannot replace expressions for node %T", n))
	}
}

// addDeferredColumns adds the given deferred columns to necessary nodes in the tree given, as well as a top-level
// projection to restore the original schema.
func addDeferredColumns(n sql.Node, columns []*deferredColumn) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Project:
			// TODO: error check to make sure that these expressions aren't already included here
			nn := plan.NewProject(append(n.Expressions(), deferredColumnsToUnresolvedColumns(columns)...), n.Child)
			return plan.NewProject(projectionsToGetFields(n.Projections), nn), nil
		default:
			return n, nil
		}
	})
}

func projectionsToGetFields(projections []sql.Expression) []sql.Expression {
	getFields := make([]sql.Expression, len(projections))
	for i, projection := range projections {
		if projection.Resolved() {
			getFields[i] = expression.NewGetField(i, projection.Type(), getName(projection), projection.IsNullable())
		} else {
			getFields[i] = expression.NewGetIndexedField(i, getName(projection))
		}
	}
	return getFields
}

func getName(e sql.Expression) string {
	if n, ok := e.(sql.Nameable); ok {
		return n.Name()
	}
	if _, ok := e.(*plan.Subquery); ok {
		return "subquery"
	}
	return e.String()
}

func deferredColumnsToUnresolvedColumns(dcs []*deferredColumn) []sql.Expression {
	ucs := make([]sql.Expression, len(dcs))
	for i, dc := range dcs {
		ucs[i] = expression.NewUnresolvedQualifiedColumn(dc.Table(), dc.Name())
	}
	return ucs
}

func findDeferredColumns(n sql.Node) []*deferredColumn {
	var cols []*deferredColumn
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if dc, ok := e.(*deferredColumn); ok {
			cols = append(cols, dc)
		}
		return true
	})

	return cols
}
