// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// flattenGroupByAggregations flattens any complex expressions in a GroupBy and adds a projection on top of the result.
// The child terms of any complex expressions get pushed down to become selected expressions in the GroupBy, and then a
// new project node re-applies the original expression to the new schema of the GroupBy.
// e.g. GroupBy(sum(a) + sum(b)) becomes project(sum(a) + sum(b), GroupBy(sum(a), sum(b)).
func flattenGroupByAggregations(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("flatten_group_by_aggregations")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		// TODO: add windows here too
		case *plan.GroupBy:
			if !hasHiddenAggregations(n.SelectedExprs...) {
				return n, nil
			}

			return flattenedGroupBy(n.SelectedExprs, n.GroupByExprs, n.Child)
		default:
			return n, nil
		}
	})
}

func flattenedGroupBy(projection, grouping []sql.Expression, child sql.Node) (sql.Node, error) {
	var aggregate = make([]sql.Expression, 0, len(projection))
	var newProjection = make([]sql.Expression, len(projection))

	for i, p := range projection {
		var transformed bool
		e, err := expression.TransformUp(p, func(e sql.Expression) (sql.Expression, error) {
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

// hasHiddenAggregations returns whether any of the given expressions has a hidden aggregation. That is, an aggregation
// that is not at the root of the expression.
func hasHiddenAggregations(exprs ...sql.Expression) bool {
	for _, e := range exprs {
		if containsHiddenAggregation(e) {
			return true
		}
	}
	return false
}

// containsHiddenAggregation returns whether the given expressions has a hidden aggregation. That is, an aggregation
// that is not at the root of the expression.
func containsHiddenAggregation(e sql.Expression) bool {
	_, ok := e.(sql.Aggregation)
	if ok {
		return false
	}

	return containsAggregation(e)
}

// containsAggregation returns whether the expression given contains any sql.Aggregation terms.
func containsAggregation(e sql.Expression) bool {
	var hasAgg bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(sql.Aggregation); ok {
			hasAgg = true
			return false
		}
		return true
	})
	return hasAgg
}
