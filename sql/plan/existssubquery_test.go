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

package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestExistsSubquery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	}), nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	emptyTable := memory.NewTable("empty", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "t", Source: "empty", Type: sql.Int64},
	}), nil)

	project := func(expr sql.Expression, tbl *memory.Table) sql.Node {
		return plan.NewProject([]sql.Expression{
			expr,
		}, plan.NewResolvedTable(tbl, nil, nil))
	}

	testCases := []struct {
		name     string
		subquery sql.Node
		row      sql.Row
		result   interface{}
	}{
		{
			"Null returns as true",
			project(
				expression.NewGetField(1, sql.Text, "foo", false), table,
			),
			sql.NewRow(nil),
			true,
		},
		{
			"Non NULL evaluates as true",
			project(
				expression.NewGetField(1, sql.Text, "foo", false), table,
			),
			sql.NewRow("four"),
			true,
		},
		{
			"Empty Set Passes",
			project(
				expression.NewGetField(1, sql.Text, "foo", false), emptyTable,
			),
			sql.NewRow(),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := plan.NewExistsSubquery(
				plan.NewSubquery(tt.subquery, ""),
			).Eval(sql.NewEmptyContext(), tt.row)
			require.NoError(err)
			require.Equal(tt.result, result)

			// Test Not Exists
			result, err = expression.NewNot(plan.NewExistsSubquery(
				plan.NewSubquery(tt.subquery, ""),
			)).Eval(sql.NewEmptyContext(), tt.row)

			require.NoError(err)
			require.Equal(tt.result, !result.(bool))
		})
	}
}
