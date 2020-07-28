package analyzer

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
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
				return s.WithQuery(analyzed).WithScopeLen(subScope.SchemaLength()), nil
			}
			return nil, err
		}

		if qp, ok := analyzed.(*plan.QueryProcess); ok {
			analyzed = qp.Child
		}

		return s.WithQuery(analyzed).WithScopeLen(subScope.SchemaLength()), nil
	})
}

// pullUpMissingSubqueryColumns examines subqueries to see which columns from outer scopes are missing in the scope
// node, and pulls them up. An additional higher-level projection is added as necessary.
func pullUpMissingSubqueryColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*plan.Subquery)
		if !ok || s.Resolved() {
			return e, nil
		}

		subqueryCtx := ctx.NewSubContext(ctx.Context)
		subScope := scope.newScope(n)

		analyzed, err := a.Analyze(subqueryCtx, s.Query, subScope)
		if err != nil {
			if ErrValidationResolved.Is(err) {
				// keep the work we have and defer remainder of analysis of this subquery until a later pass
				return s.WithQuery(analyzed).WithScopeLen(subScope.SchemaLength()), nil
			}
			return nil, err
		}

		if qp, ok := analyzed.(*plan.QueryProcess); ok {
			analyzed = qp.Child
		}

		return s.WithQuery(analyzed).WithScopeLen(subScope.SchemaLength()), nil
	})
}
