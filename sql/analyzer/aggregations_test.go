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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestFlattenAggregationExprs(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
		{Name: "c", Type: sql.Int64, Source: "foo"},
	})
	rule := getRule("flatten_aggregation_exprs")

	tests := []struct {
		name     string
		node     sql.Node
		expected sql.Node
	}{
		{
			name: "addition",
			node: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewArithmetic(
						aggregation.NewSum(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
						expression.NewLiteral(int64(1), sql.Int64),
						"+",
					),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),

			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewArithmetic(
						expression.NewGetField(0, sql.Float64, "SUM(foo.a)", false),
						expression.NewLiteral(int64(1), sql.Int64),
						"+",
					),
				},
				plan.NewGroupBy(
					[]sql.Expression{
						aggregation.NewSum(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "addition with alias",
			node: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias("x",
						expression.NewArithmetic(
							aggregation.NewSum(
								ctx,
								expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
							),
							expression.NewLiteral(int64(1), sql.Int64),
							"+",
						)),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("x",
						expression.NewArithmetic(
							expression.NewGetField(0, sql.Float64, "SUM(foo.a)", false),
							expression.NewLiteral(int64(1), sql.Int64),
							"+",
						)),
				},
				plan.NewGroupBy(
					[]sql.Expression{
						aggregation.NewSum(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "multiple aggregates in one expression",
			node: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewArithmetic(
						aggregation.NewSum(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
						aggregation.NewCount(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
						"/",
					),
					expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewArithmetic(
						expression.NewGetField(0, sql.Float64, "SUM(foo.a)", false),
						expression.NewGetField(1, sql.Int64, "COUNT(foo.a)", false),
						"/",
					),
					expression.NewGetFieldWithTable(2, sql.Int64, "foo", "b", false),
				},
				plan.NewGroupBy(
					[]sql.Expression{
						aggregation.NewSum(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
						aggregation.NewCount(
							ctx,
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						),
						expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), test.node, nil)
			require.NoError(err)
			require.Equal(test.expected, result)
		})
	}
}
