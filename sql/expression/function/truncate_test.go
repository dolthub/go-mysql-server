// Copyright 2025 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTruncate(t *testing.T) {
	testCases := []struct {
		name  string
		xExpr sql.Expression
		dExpr sql.Expression
		exp   interface{}
		err   *errors.Kind
	}{
		// https://github.com/dolthub/dolt/issues/9916
		{
			name:  "float64 is nil",
			xExpr: expression.NewLiteral(nil, types.Null),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   nil,
		},
		{
			name:  "precision is nil",
			xExpr: expression.NewLiteral(1.223, types.Float64),
			dExpr: expression.NewLiteral(nil, types.Null),
			exp:   nil,
		},
		{
			name:  "basic truncate positive precision",
			xExpr: expression.NewLiteral(1.223, types.Float64),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   1.2,
		},
		{
			name:  "truncate toward zero",
			xExpr: expression.NewLiteral(1.999, types.Float64),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   1.9,
		},
		{
			name:  "truncate to integer",
			xExpr: expression.NewLiteral(1.999, types.Float64),
			dExpr: expression.NewLiteral(0, types.Int32),
			exp:   1.0,
		},
		{
			name:  "negative number truncate",
			xExpr: expression.NewLiteral(-1.999, types.Float64),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   -1.9,
		},
		{
			name:  "negative precision truncate",
			xExpr: expression.NewLiteral(122.0, types.Float64),
			dExpr: expression.NewLiteral(-2, types.Int32),
			exp:   100.0,
		},
		{
			name:  "negative precision with integer",
			xExpr: expression.NewLiteral(122, types.Int64),
			dExpr: expression.NewLiteral(-2, types.Int32),
			exp:   int64(100),
		},
		{
			name:  "truncate toward zero for positive",
			xExpr: expression.NewLiteral(0.5, types.Float64),
			dExpr: expression.NewLiteral(0, types.Int32),
			exp:   0.0,
		},
		{
			name:  "truncate toward zero for negative",
			xExpr: expression.NewLiteral(-0.5, types.Float64),
			dExpr: expression.NewLiteral(0, types.Int32),
			exp:   0.0,
		},
		{
			name:  "float32 input",
			xExpr: expression.NewLiteral(float32(1.223), types.Float32),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   1.2,
		},
		{
			name:  "int32 input",
			xExpr: expression.NewLiteral(int32(122), types.Int32),
			dExpr: expression.NewLiteral(-2, types.Int32),
			exp:   int64(100),
		},
		{
			name:  "text input",
			xExpr: expression.NewLiteral("1.223", types.Text),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   1.2,
		},
		{
			name:  "text input with precision",
			xExpr: expression.NewLiteral("122", types.Text),
			dExpr: expression.NewLiteral(-2, types.Int32),
			exp:   100.0,
		},
		{
			name:  "decimal input",
			xExpr: expression.NewLiteral("1.999", types.MustCreateDecimalType(4, 3)),
			dExpr: expression.NewLiteral(1, types.Int32),
			exp:   "1.9",
		},
		{
			name:  "large precision",
			xExpr: expression.NewLiteral(1234567890.0987654321, types.Float64),
			dExpr: expression.NewLiteral(999_999_999, types.Int32),
			exp:   1234567890.0987654321,
		},
		{
			name:  "large negative precision",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-999_999_999, types.Int32),
			exp:   0.0,
		},
		{
			name:  "float precision",
			xExpr: expression.NewLiteral(5.855, types.Float64),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   5.85,
		},
		{
			name:  "float negative precision",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-1.0, types.Float64),
			exp:   50.0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTruncate(tt.xExpr, tt.dExpr)

			if tt.err != nil {
				t.Skip("Argument validation handled by framework")
				return
			}

			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)

			if tt.name == "decimal input" {
				if dec, ok := res.(decimal.Decimal); ok {
					require.Equal(t, tt.exp, dec.String())
				} else {
					require.Equal(t, tt.exp, res)
				}
			} else {
				require.Equal(t, tt.exp, res)
			}
		})
	}
}

func TestTruncateWithChildren(t *testing.T) {
	req := require.New(t)

	f := NewTruncate(
		expression.NewLiteral(1.223, types.Float64),
		expression.NewLiteral(1, types.Int32),
	)

	newF, err := f.WithChildren(
		expression.NewLiteral(2.456, types.Float64),
		expression.NewLiteral(2, types.Int32),
	)
	req.NoError(err)
	req.NotEqual(f, newF)

	res, err := newF.Eval(sql.NewEmptyContext(), nil)
	req.NoError(err)
	req.Equal(2.45, res)
}

func TestTruncateString(t *testing.T) {
	req := require.New(t)

	f := NewTruncate(
		expression.NewLiteral(1.223, types.Float64),
		expression.NewLiteral(1, types.Int32),
	)

	req.Equal("truncate(1.223,1)", f.String())
}

func TestTruncateType(t *testing.T) {
	req := require.New(t)

	f := NewTruncate(
		expression.NewLiteral(1.223, types.Float64),
		expression.NewLiteral(1, types.Int32),
	)
	req.Equal(types.Float64, f.Type())

	f = NewTruncate(
		expression.NewLiteral("1.223", types.Text),
		expression.NewLiteral(1, types.Int32),
	)
	req.Equal(types.Float64, f.Type())
}

func TestTruncateIsNullable(t *testing.T) {
	req := require.New(t)

	f := NewTruncate(
		expression.NewLiteral(nil, types.Null),
		expression.NewLiteral(1, types.Int32),
	)
	req.True(f.IsNullable())

	f = NewTruncate(
		expression.NewLiteral(1.223, types.Float64),
		expression.NewLiteral(1, types.Int32),
	)
	req.False(f.IsNullable())
}
