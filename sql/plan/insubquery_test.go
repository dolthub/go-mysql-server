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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestInSubquery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	}), nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	project := func(expr sql.Expression) sql.Node {
		return plan.NewProject([]sql.Expression{
			expr,
		}, plan.NewResolvedTable(table, nil, nil))
	}

	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Node
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow(nil),
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow("two"),
			true,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow("four"),
			false,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := plan.NewInSubquery(
				tt.left,
				plan.NewSubquery(tt.right, ""),
			).Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func TestNotInSubquery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	}), nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	project := func(expr sql.Expression) sql.Node {
		return plan.NewProject([]sql.Expression{
			expr,
		}, plan.NewResolvedTable(table, nil, nil))
	}

	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Node
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow(nil),
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			project(
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow("two"),
			false,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(1, sql.Text, "foo", false),
			),
			sql.NewRow("four"),
			true,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := plan.NewNotInSubquery(
				tt.left,
				plan.NewSubquery(tt.right, ""),
			).Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}
