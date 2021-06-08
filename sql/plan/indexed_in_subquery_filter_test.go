// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestIndexedInSubqueryFilter(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	})

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	rows, err := sql.NodeToRows(ctx, plan.NewIndexedInSubqueryFilter(
		plan.NewSubquery(
			plan.NewProject([]sql.Expression{
				expression.NewGetField(1, sql.Text, "t", true),
			}, plan.NewResolvedTable(table, nil, nil)),
			"select t from foo",
		),
		plan.EmptyTable,
		1,
		expression.NewGetField(0, sql.Int32, "id", false),
		false),
	)
	require.NoError(t, err)
	require.Len(t, rows, 0)

	rows, err = sql.NodeToRows(ctx, plan.NewIndexedInSubqueryFilter(
		plan.NewSubquery(plan.EmptyTable, "select from dual"),
		plan.NewProject([]sql.Expression{
			expression.NewGetField(1, sql.Text, "t", true),
		}, plan.NewResolvedTable(table, nil, nil)),
		1,
		expression.NewGetField(0, sql.Int32, "id", false),
		false),
	)
	require.NoError(t, err)
	require.Len(t, rows, 0)

	rows, err = sql.NodeToRows(ctx, plan.NewIndexedInSubqueryFilter(
		plan.NewSubquery(
			plan.NewProject([]sql.Expression{
				expression.NewGetField(1, sql.Text, "t", true),
			}, plan.NewResolvedTable(table, nil, nil)),
			"select t from foo",
		),
		plan.EmptyTable,
		1,
		expression.NewGetField(0, sql.Int32, "id", false),
		true),
	)
	require.Error(t, err)

	rows, err = sql.NodeToRows(ctx, plan.NewIndexedInSubqueryFilter(
		plan.NewSubquery(
			plan.NewProject([]sql.Expression{
				expression.NewGetField(1, sql.Text, "t", true),
			}, plan.NewResolvedTable(table, nil, nil)),
			"select t from foo",
		),
		plan.NewProject([]sql.Expression{
			expression.NewGetField(0, sql.Text, "t", true),
		}, plan.NewResolvedTable(table, nil, nil)),
		1,
		expression.NewGetField(0, sql.Text, "t", false),
		false),
	)
	require.NoError(t, err)
	require.Equal(t, rows, []sql.Row{
		sql.Row{"one"},
		sql.Row{"two"},
		sql.Row{"three"},
		sql.Row{"one"},
		sql.Row{"two"},
		sql.Row{"three"},
		sql.Row{"one"},
		sql.Row{"two"},
		sql.Row{"three"},
	})

	c, err := function.NewConcat(sql.NewEmptyContext(), expression.NewGetField(0, sql.Text, "t", true), expression.NewLiteral("_some_stuff", sql.Text))
	require.NoError(t, err)
	rows, err = sql.NodeToRows(ctx, plan.NewIndexedInSubqueryFilter(
		plan.NewSubquery(
			plan.NewProject([]sql.Expression{
				expression.NewGetField(1, sql.Text, "t", true),
			}, plan.NewResolvedTable(table, nil, nil)),
			"select t from foo",
		),
		plan.NewProject([]sql.Expression{
			c,
		}, plan.NewResolvedTable(table, nil, nil)),
		1,
		expression.NewGetField(0, sql.Text, "t", false),
		false),
	)
	require.NoError(t, err)
	require.Len(t, rows, 0)
}
