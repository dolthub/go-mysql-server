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

package plan_test

import (
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestSubquery(t *testing.T) {
	require := require.New(t)
	table := memory.NewTable("", nil)
	require.NoError(table.Insert(sql.NewEmptyContext(), nil))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral("one", sql.LongText),
		},
		plan.NewResolvedTable(table),
	))

	value, err := subquery.Eval(sql.NewEmptyContext(), nil)
	require.NoError(err)
	require.Equal(value, "one")
}

func TestSubqueryTooManyRows(t *testing.T) {
	require := require.New(t)
	table := memory.NewTable("", nil)
	require.NoError(table.Insert(sql.NewEmptyContext(), nil))
	require.NoError(table.Insert(sql.NewEmptyContext(), nil))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral("one", sql.LongText),
		},
		plan.NewResolvedTable(table),
	))

	_, err := subquery.Eval(sql.NewEmptyContext(), nil)
	require.Error(err)
}

func TestSubqueryMultipleRows(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	})

	require.NoError(table.Insert(ctx, sql.Row{"one"}))
	require.NoError(table.Insert(ctx, sql.Row{"two"}))
	require.NoError(table.Insert(ctx, sql.Row{"three"}))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "t", false),
		},
		plan.NewResolvedTable(table),
	))

	values, err := subquery.EvalMultiple(ctx)
	require.NoError(err)
	require.Equal(values, []interface{}{"one", "two", "three"})
}
