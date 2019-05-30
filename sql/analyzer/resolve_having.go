package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/expression/function/aggregation"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func resolveHaving(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		having, ok := node.(*plan.Having)
		if !ok {
			return node, nil
		}

		if !having.Resolved() {
			return node, nil
		}

		// If there are no aggregations there is no need to check anything else
		// and we can just leave the node as it is.
		if !hasAggregations(having.Cond) {
			return node, nil
		}

		groupBy, ok := having.Child.(*plan.GroupBy)
		if !ok {
			return nil, errHavingNeedsGroupBy.New()
		}

		var aggregate = make([]sql.Expression, len(groupBy.Aggregate))
		copy(aggregate, groupBy.Aggregate)

		// We need to find all the aggregations in the having that are already present in
		// the group by and replace them with a GetField. If the aggregation is not
		// present, we need to move it to the GroupBy and reference it with a GetField.
		cond, err := having.Cond.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			agg, ok := e.(sql.Aggregation)
			if !ok {
				return e, nil
			}

			for i, expr := range aggregate {
				if aggregationEquals(agg, expr) {
					var name string
					if n, ok := expr.(sql.Nameable); ok {
						name = n.Name()
					} else {
						name = expr.String()
					}

					return expression.NewGetField(
						i,
						expr.Type(),
						name,
						expr.IsNullable(),
					), nil
				}
			}

			aggregate = append(aggregate, agg)
			return expression.NewGetField(
				len(aggregate)-1,
				agg.Type(),
				agg.String(),
				agg.IsNullable(),
			), nil
		})
		if err != nil {
			return nil, err
		}

		var result sql.Node = plan.NewHaving(
			cond,
			plan.NewGroupBy(aggregate, groupBy.Grouping, groupBy.Child),
		)

		// If any aggregation was sent to the GroupBy aggregate, we will need
		// to wrap the new Having in a project that will get rid of all those
		// extra columns we added.
		if len(aggregate) != len(groupBy.Aggregate) {
			var projection = make([]sql.Expression, len(groupBy.Aggregate))
			for i, e := range groupBy.Aggregate {
				var table, name string
				if t, ok := e.(sql.Tableable); ok {
					table = t.Table()
				}

				if n, ok := e.(sql.Nameable); ok {
					name = n.Name()
				} else {
					name = e.String()
				}

				projection[i] = expression.NewGetFieldWithTable(
					i,
					e.Type(),
					table,
					name,
					e.IsNullable(),
				)
			}
			result = plan.NewProject(projection, result)
		}

		return result, nil
	})
}

func aggregationEquals(a, b sql.Expression) bool {
	// First unwrap aliases
	if alias, ok := b.(*expression.Alias); ok {
		b = alias.Child
	} else if alias, ok := a.(*expression.Alias); ok {
		a = alias.Child
	}

	switch a := a.(type) {
	case *aggregation.Count:
		// it doesn't matter what's inside a Count, the result will be
		// the same.
		_, ok := b.(*aggregation.Count)
		return ok
	case *aggregation.Sum,
		*aggregation.Avg,
		*aggregation.Min,
		*aggregation.Max:
		return reflect.DeepEqual(a, b)
	default:
		return false
	}
}

var errHavingNeedsGroupBy = errors.NewKind("found HAVING clause with no GROUP BY")

func hasAggregations(expr sql.Expression) bool {
	var has bool
	expression.Inspect(expr, func(e sql.Expression) bool {
		_, ok := e.(sql.Aggregation)
		if ok {
			has = true
			return false
		}
		return true
	})
	return has
}
