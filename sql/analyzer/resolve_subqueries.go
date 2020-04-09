package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.Finish()

	a.Log("resolving subqueries")
	n, err := plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			a.Log("found subquery %q with child of type %T", n.Name(), n.Child)
			child, err := a.Analyze(ctx, n.Child)
			if err != nil {
				return nil, err
			}

			return n.WithChildren(child)
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*expression.Subquery)
		if !ok || s.Resolved() {
			return e, nil
		}

		q, err := a.Analyze(ctx, s.Query)
		if err != nil {
			return nil, err
		}

		if qp, ok := q.(*plan.QueryProcess); ok {
			q = qp.Child
		}

		return s.WithQuery(q), nil
	})
}
