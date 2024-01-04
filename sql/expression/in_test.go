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
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var testEnumType = types.MustCreateEnumType([]string{"", "one", "two"}, sql.Collation_Default)

var testSetType = types.MustCreateSetType([]string{"", "one", "two"}, sql.Collation_Default)

func TestRoundTripNames(t *testing.T) {
	assert.Equal(t, "(foo IN (foo, 2))", expression.NewInTuple(expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewTuple(
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewLiteral(int64(2), types.Int64),
		)).String())
	hit, err := expression.NewHashInTuple(nil, expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(2), types.Int64),
		))
	assert.NoError(t, err)
	assert.Equal(t, "(foo HASH IN (2))", hit.String())
}

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
			expression.NewLiteral(nil, types.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, types.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(int64(2), types.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, types.Int64, "foo", false),
				expression.NewLiteral(int64(2), types.Int64),
			),
			sql.NewRow(int64(1)),
			true,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, types.Int64, "bar", false),
				expression.NewLiteral(int64(2), types.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			false,
			nil,
		},
		{
			name: "right values contain a different, coercible type",
			left: expression.NewLiteral(1, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "right values contain a different, coercible type, and left value is zero value",
			left: expression.NewLiteral(0, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "enum on left side; invalid values on right",
			left: expression.NewLiteral("one", testEnumType),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "enum on left side; valid enum values on right",
			left: expression.NewLiteral("one", testEnumType),
			right: expression.NewTuple(
				expression.NewLiteral("", types.TinyText),
				expression.NewLiteral("one", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "set on left side; invalid set values on right",
			left: expression.NewLiteral("one", testSetType),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "set on left side; valid set values on right",
			left: expression.NewLiteral("one", testSetType),
			right: expression.NewTuple(
				expression.NewLiteral("", types.TinyText),
				expression.NewLiteral("one", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "date on right side; non-dates on left",
			left: expression.NewLiteral(time.Now(), types.DatetimeMaxPrecision),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			err:    types.ErrConvertingToTime,
			row:    nil,
			result: false,
		}}

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
			expression.NewLiteral(nil, types.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, types.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			nil,
			sql.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(int64(2), types.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, types.Int64, "foo", false),
				expression.NewLiteral(int64(2), types.Int64),
			),
			sql.NewRow(int64(1)),
			false,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, types.Int64, "bar", false),
				expression.NewLiteral(int64(2), types.Int64),
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
			expression.NewLiteral(nil, types.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same number of cols; right has tuple",
			expression.NewLiteral(1, types.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			false,
			sql.ErrInvalidOperandColumns,
			nil,
		},
		{
			"left and right don't have the same number of cols; left has tuple",
			expression.NewTuple(
				expression.NewLiteral(1, types.Int64),
				expression.NewLiteral(0, types.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewLiteral(int64(2), types.Int64),
			),
			nil,
			false,
			sql.ErrInvalidOperandColumns,
			nil,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(int64(2), types.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
			nil,
		},
		{
			"left is in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewLiteral(int64(2), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(0), types.Int64),
			),
			sql.NewRow(int64(1)),
			true,
			nil,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewTuple(
				expression.NewLiteral(int64(0), types.Int64),
				expression.NewLiteral(int64(2), types.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			false,
			nil,
			nil,
		},
		{
			"left tuple is in right",
			expression.NewTuple(
				expression.NewLiteral(int64(2), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(2), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(0), types.Int64),
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
				expression.NewLiteral(int64(2), types.Int64),
				expression.NewLiteral("a", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral("b", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(2), types.Int64),
					expression.NewLiteral("a", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
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
				expression.NewGetField(0, types.Int64, "foo", false),
				expression.NewGetField(1, types.Int64, "foo", false),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(2), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(1), types.Int64),
					expression.NewLiteral(int64(0), types.Int64),
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
					expression.NewLiteral(int64(2), types.Int64),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewLiteral(int64(1), types.Int64),
			),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewTuple(
						expression.NewLiteral(int64(2), types.Int64),
						expression.NewLiteral(int64(1), types.Int64),
					),
					expression.NewLiteral(int64(1), types.Int64),
				),
				expression.NewTuple(
					expression.NewTuple(
						expression.NewLiteral(int64(1), types.Int64),
						expression.NewLiteral(int64(2), types.Int64),
					),
					expression.NewLiteral(int64(0), types.Int64),
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
					expression.NewLiteral("hi", types.TinyText),
				),
			),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
			),
			result: true,
		},
		{
			name: "right values contain a different, coercible type",
			left: expression.NewLiteral(1, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "right values contain zero floats that are equal to the left value",
			left: expression.NewLiteral(0, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral( 0.0, types.Float64),
				expression.NewLiteral(1.23, types.Float64),
			),
			row:    nil,
			result: true,
		},
		{
			name: "right values contain floats that are equal to the left value",
			left: expression.NewLiteral(1, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral( 1.0, types.Float64),
				expression.NewLiteral(1.23, types.Float64),
			),
			row:    nil,
			result: true,
		},
		{
			name: "right values contain decimals that are equal to the left value",
			left: expression.NewLiteral(1, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral( 1.0, types.MustCreateDecimalType(10, 5)),
				expression.NewLiteral(1.23, types.MustCreateDecimalType(10, 5)),
			),
			row:    nil,
			result: true,
		},
		{
			name: "right values contain a different, coercible type, and left value is zero value",
			left: expression.NewLiteral(0, types.Uint64),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "enum on left side; invalid values on right",
			left: expression.NewLiteral("one", testEnumType),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "enum on left side; valid enum values on right",
			left: expression.NewLiteral("one", testEnumType),
			right: expression.NewTuple(
				expression.NewLiteral("", types.TinyText),
				expression.NewLiteral("one", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "set on left side; invalid set values on right",
			left: expression.NewLiteral("one", testSetType),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			row:    nil,
			result: false,
		},
		{
			name: "set on left side; valid set values on right",
			left: expression.NewLiteral("one", testSetType),
			right: expression.NewTuple(
				expression.NewLiteral("", types.TinyText),
				expression.NewLiteral("one", types.TinyText),
			),
			row:    nil,
			result: true,
		},
		{
			name: "date on right side; non-dates on left",
			left: expression.NewLiteral(time.Now(), types.DatetimeMaxPrecision),
			right: expression.NewTuple(
				expression.NewLiteral("hi", types.TinyText),
				expression.NewLiteral("bye", types.TinyText),
			),
			staticErr: types.ErrConvertingToTime,
			row:       nil,
			result:    false,
		},
		{
			name: "left has a convert (type cast)",
			left: expression.NewConvert(
				expression.NewGetField(0, types.Int64, "foo", false),
				"char",
			),
			right: expression.NewTuple(
				expression.NewLiteral("1", types.TinyText),
			),
			row: sql.NewRow(int64(1), int64(0)),

			result: true,
		},
		{
			name: "left has a comparer",
			left: expression.NewGreaterThan(
				expression.NewGetField(0, types.Int64, "foo", false),
				expression.NewLiteral(1, types.Int64),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, types.Boolean),
			),
			row:    sql.NewRow(int64(2), int64(0)),
			result: true,
		},
		{
			name: "left has an is null",
			left: expression.NewIsNull(
				expression.NewLiteral(nil, types.Null),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, types.Boolean),
			),
			result: true,
		},
		{
			name: "left has an is true",
			left: expression.NewIsTrue(
				expression.NewLiteral(true, types.Boolean),
			),
			right: expression.NewTuple(
				expression.NewLiteral(true, types.Boolean),
			),
			result: true,
		},
		{
			name: "left has an arithmetic",
			left: expression.NewPlus(
				expression.NewLiteral(4, types.Int64),
				expression.NewGetField(0, types.Int64, "foo", false),
			),
			right: expression.NewTuple(
				expression.NewLiteral(6, types.Int64),
			),
			row:    sql.NewRow(int64(2), int64(0)),
			result: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			require := require.New(t)
			expr, err := expression.NewHashInTuple(ctx, tt.left, tt.right)
			if tt.staticErr != nil {
				require.Error(err)
				require.True(tt.staticErr.Is(err))
			} else {
				require.NoError(err)
				result, err := expr.Eval(ctx, tt.row)
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
