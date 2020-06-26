package analyzer

import (
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func resolveStar(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_star")
	defer span.Finish()

	tableAliases, err := getTableAliases(n)
	if err != nil {
		return nil, err
	}

	a.Log("resolving star, node of type: %T", n)
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			if !n.Child.Resolved() {
				return n, nil
			}

			expressions, err := expandStars(a, n.Projections, n.Child.Schema(), tableAliases)
			if err != nil {
				return nil, err
			}

			return plan.NewProject(expressions, n.Child), nil
		case *plan.GroupBy:
			if !n.Child.Resolved() {
				return n, nil
			}

			aggregate, err := expandStars(a, n.Aggregate, n.Child.Schema(), tableAliases)
			if err != nil {
				return nil, err
			}

			return plan.NewGroupBy(aggregate, n.Grouping, n.Child), nil
		default:
			return n, nil
		}
	})
}

func expandStars(a *Analyzer, exprs []sql.Expression, schema sql.Schema, tableAliases TableAliases) ([]sql.Expression, error) {
	var expressions []sql.Expression
	for _, e := range exprs {
		if s, ok := e.(*expression.Star); ok {
			var exprs []sql.Expression
			for i, col := range schema {
				lowerSource := strings.ToLower(col.Source)
				lowerTable := strings.ToLower(s.Table)
				if s.Table == "" || lowerTable == lowerSource ||
					(tableAliases[lowerSource] != nil && strings.ToLower(tableAliases[lowerSource].Name()) == lowerTable) {
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

	a.Log("resolved * to expressions %s", expressions)
	return expressions, nil
}
