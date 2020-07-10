// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestReorderProjection(t *testing.T) {
	f := getRule("reorder_projection")

	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int64},
		{Name: "s", Source: "mytable", Type: sql.Text},
	})

	testCases := []struct {
		name     string
		project  sql.Node
		expected sql.Node
	}{
		{
			name: "sort",
			project: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
					expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
					expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
				},
				plan.NewSort(
					[]plan.SortField{
						{Column: expression.NewUnresolvedColumn("foo")},
					},
					plan.NewFilter(
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewUnresolvedColumn("bar"),
						),
						plan.NewResolvedTable(table),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
					expression.NewGetField(3, sql.Int64, "foo", false),
					expression.NewGetField(2, sql.Int64, "bar", false),
				},
				plan.NewSort(
					[]plan.SortField{{Column: expression.NewGetField(3, sql.Int64, "foo", false)}},
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false),
							expression.NewGetField(2, sql.Int64, "bar", false),
							expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
						},
						plan.NewFilter(
							expression.NewEquals(
								expression.NewLiteral(1, sql.Int64),
								expression.NewGetField(2, sql.Int64, "bar", false),
							),
							plan.NewProject(
								[]sql.Expression{
									expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
									expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false),
									expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
								},
								plan.NewResolvedTable(table),
							),
						),
					),
				),
			),
		},
		{
			name: "use alias twice",
			project: plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewUnresolvedColumn("foo"),
						),
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewUnresolvedColumn("foo"),
						),
					),
					plan.NewResolvedTable(table),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(2, sql.Int64, "foo", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewGetField(2, sql.Int64, "foo", false),
						),
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewGetField(2, sql.Int64, "foo", false),
						),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false),
							expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
						},
						plan.NewResolvedTable(table),
					),
				),
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), tt.project, nil)
			require.NoError(err)

			require.Equal(tt.expected, result)
		})
	}
}
