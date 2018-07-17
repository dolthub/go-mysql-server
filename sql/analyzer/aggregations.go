package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func reorderAggregations(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("reorder_aggregations")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	a.Log("reorder aggregations, node of type: %T", n)

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.GroupBy:
			if !hasHiddenAggregations(n.Aggregate...) {
				return n, nil
			}

			a.Log("fixing aggregations of node of type: %T", n)

			return fixAggregations(n.Aggregate, n.Grouping, n.Child)
		default:
			return n, nil
		}
	})
}

func fixAggregations(projection, grouping []sql.Expression, child sql.Node) (sql.Node, error) {
	var aggregate = make([]sql.Expression, 0, len(projection))
	var newProjection = make([]sql.Expression, len(projection))

	for i, p := range projection {
		var transformed bool
		e, err := p.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			agg, ok := e.(sql.Aggregation)
			if !ok {
				return e, nil
			}

			transformed = true
			aggregate = append(aggregate, agg)
			return expression.NewGetField(
				len(aggregate)-1, agg.Type(), agg.String(), agg.IsNullable(),
			), nil
		})
		if err != nil {
			return nil, err
		}

		if !transformed {
			aggregate = append(aggregate, e)
			name, source := getNameAndSource(e)
			newProjection[i] = expression.NewGetFieldWithTable(
				len(aggregate)-1, e.Type(), source, name, e.IsNullable(),
			)
		} else {
			newProjection[i] = e
		}
	}

	return plan.NewProject(
		newProjection,
		plan.NewGroupBy(aggregate, grouping, child),
	), nil
}

func getNameAndSource(e sql.Expression) (name, source string) {
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	} else {
		name = e.String()
	}

	if t, ok := e.(sql.Tableable); ok {
		source = t.Table()
	}

	return
}

// hasHiddenAggregations reports whether any of the given expressions has a
// hidden aggregation. That is, an aggregation that is not at the root of the
// expression.
func hasHiddenAggregations(exprs ...sql.Expression) bool {
	for _, e := range exprs {
		if containsHiddenAggregation(e) {
			return true
		}
	}
	return false
}

func containsHiddenAggregation(e sql.Expression) bool {
	_, ok := e.(sql.Aggregation)
	if ok {
		return false
	}

	return containsAggregation(e)
}

func containsAggregation(e sql.Expression) bool {
	var hasAgg bool
	expression.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(sql.Aggregation); ok {
			hasAgg = true
			return false
		}
		return true
	})
	return hasAgg
}
