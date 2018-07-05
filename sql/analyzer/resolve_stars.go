package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func resolveStar(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_star")
	defer span.Finish()

	a.Log("resolving star, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			if !n.Child.Resolved() {
				return n, nil
			}

			expressions, err := expandStars(n.Projections, n.Child.Schema())
			if err != nil {
				return nil, err
			}

			return plan.NewProject(expressions, n.Child), nil
		case *plan.GroupBy:
			if !n.Child.Resolved() {
				return n, nil
			}

			aggregate, err := expandStars(n.Aggregate, n.Child.Schema())
			if err != nil {
				return nil, err
			}

			return plan.NewGroupBy(aggregate, n.Grouping, n.Child), nil
		default:
			return n, nil
		}
	})
}

func expandStars(exprs []sql.Expression, schema sql.Schema) ([]sql.Expression, error) {
	var expressions []sql.Expression
	for _, e := range exprs {
		if s, ok := e.(*expression.Star); ok {
			var exprs []sql.Expression
			for i, col := range schema {
				if s.Table == "" || s.Table == col.Source {
					exprs = append(exprs, expression.NewGetFieldWithTable(
						i, col.Type, col.Source, col.Name, col.Nullable,
					))
				}
			}

			if len(exprs) == 0 && s.Table != "" {
				return nil, sql.ErrTableNotFound.New(s.Table)
			}

			expressions = append(expressions, exprs...)
		} else {
			expressions = append(expressions, e)
		}
	}

	return expressions, nil
}
