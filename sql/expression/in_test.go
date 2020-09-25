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

package expression_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
			expression.ErrInvalidOperandColumns,
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
			expression.ErrInvalidOperandColumns,
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
