package analyzer

import (
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
	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*expression.Subquery)
		if !ok || s.Resolved() {
			return e, nil
		}

		subqueryCtx := ctx.NewSubContext(ctx.Context)
		subScope := scope.newScope(n)

		analyzed, err := a.Analyze(subqueryCtx, s.Query, subScope)
		if err != nil {
			if sql.ErrTableNotFound.Is(err) || sql.ErrTableColumnNotFound.Is(err) || sql.ErrAmbiguousColumnName.Is(err) || ErrValidationResolved.Is(err) {
				// defer analysis of this subquery until a later pass of analysis
				return e, nil
			}
			return nil, err
		}

		if qp, ok := analyzed.(*plan.QueryProcess); ok {
			analyzed = qp.Child
		}

		return s.WithQuery(analyzed), nil
	})
}