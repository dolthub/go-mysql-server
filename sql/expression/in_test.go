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

package expression_test

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

func TestInTuple(t *testing.T) {
	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Expression
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, sql.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1)),
			true,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			false,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewInTuple(tt.left, tt.right).
				Eval(sql.NewEmptyContext(), tt.row)
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

func TestNotInTuple(t *testing.T) {
	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Expression
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, sql.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1)),
			false,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			true,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewNotInTuple(tt.left, tt.right).
				Eval(sql.NewEmptyContext(), tt.row)
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

func TestHashInTuple(t *testing.T) {
	testCases := []struct {
		name      string
		left      sql.Expression
		right     sql.Expression
		row       sql.Row
		result    interface{}
		staticErr *errors.Kind
		evalErr   *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols; right has tuple",
			expression.NewLiteral(1, sql.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			false,
			sql.ErrInvalidOperandColumns,
			nil,
		},
		{
			"left and right don't have the same cols; left has tuple",
			expression.NewTuple(
				expression.NewLiteral(1, sql.Int64),
				expression.NewLiteral(0, sql.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			false,
			sql.ErrInvalidOperandColumns,
			nil,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
			nil,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewLiteral(int64(2), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(0), sql.Int64),
			),
			sql.NewRow(int64(1)),
			true,
			nil,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewLiteral(int64(0), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			false,
			nil,
			nil,
		},
		{
			"left tuple is in right",
			expression.NewTuple(
				expression.NewLiteral(int64(2), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(2), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(0), sql.Int64),
				),
			),
			nil,
			true,
			nil,
			nil,
		},
		{
			"heterogeneous left tuple is in right",
			expression.NewTuple(
				expression.NewLiteral(int64(2), sql.Int64),
				expression.NewLiteral("a", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral("b", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(2), sql.Int64),
					expression.NewLiteral("a", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
				),
			),
			nil,
			true,
			nil,
			nil,
		},
		{
			"left get field tuple is in right",
			expression.NewTuple(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewGetField(1, sql.Int64, "foo", false),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(2), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(0), sql.Int64),
				),
			),
			sql.NewRow(int64(1), int64(0)),
			true,
			nil,
			nil,
		},
		{
			"left nested tuple is in right",
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(2), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
					),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewTuple(
					expression.NewTuple(
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewLiteral(int64(2), sql.Int64),
					),
					expression.NewLiteral(int64(0), sql.Int64),
				),
			),
			nil,
			true,
			nil,
			nil,
		},
		{
			name: "left has a function",
			left: expression.NewTuple(
				function.NewLower(
					expression.NewLiteral("hi", sql.TinyText),
				),
			),
			right: expression.NewTuple(
				expression.NewLiteral("hi", sql.TinyText),
			),
			result: true,
		},
		{
			name: "left has a convert (type cast)",
			left: expression.NewConvert(
				expression.NewGetField(0, sql.Int64, "foo", false),
				"char",
			),
			right: expression.NewTuple(
				expression.NewLiteral("1", sql.TinyText),
			),
			row: sql.NewRow(int64(1), int64(0)),

			result: true,
		},
		{
			name: "left has a comparer",
			left: expression.NewGreaterThan(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewLiteral(1, sql.Int64),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, sql.Boolean),
			),
			row:    sql.NewRow(int64(2), int64(0)),
			result: true,
		},
		{
			name: "left has an is null",
			left: expression.NewIsNull(
				expression.NewLiteral(nil, sql.Null),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, sql.Boolean),
			),
			result: true,
		},
		{
			name: "left has an is true",
			left: expression.NewIsTrue(
				expression.NewLiteral(true, sql.Boolean),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, sql.Boolean),
			),
			result: true,
		},
		{
			name: "left has an arithmetic",
			left: expression.NewPlus(
				expression.NewLiteral(4, sql.Int64),
				expression.NewGetField(0, sql.Int64, "foo", false),
			),
			right: expression.NewTuple(
				expression.NewLiteral(6, sql.Int64),
			),
			row:    sql.NewRow(int64(2), int64(0)),
			result: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &sql.Context{}
			require := require.New(t)
			expr, err := expression.NewHashInTuple(ctx, tt.left, tt.right)
			if tt.staticErr != nil {
				require.Error(err)
				require.True(tt.staticErr.Is(err))
			} else {
				result, err := expr.Eval(sql.NewEmptyContext(), tt.row)
				if tt.evalErr != nil {
					require.Error(err)
					require.True(tt.evalErr.Is(err))
				} else {
					require.NoError(err)
					require.Equal(tt.result, result)
				}
			}
		})
	}
}
