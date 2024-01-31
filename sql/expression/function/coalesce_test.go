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

package function

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestEmptyCoalesce(t *testing.T) {
	_, err := NewCoalesce()
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))
}

func TestCoalesce(t *testing.T) {
	testCases := []struct {
		name     string
		input    []sql.Expression
		expected interface{}
		typ      sql.Type
		nullable bool
	}{
		{
			name: "coalesce(1, 2, 3)",
			input: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral(2, types.Int32),
				expression.NewLiteral(3, types.Int32),
			},
			expected: 1,
			typ:      types.Int32,
			nullable: false,
		},
		{
			name: "coalesce(NULL, NULL, 3)",
			input: []sql.Expression{
				nil,
				nil,
				expression.NewLiteral(3, types.Int32),
			},
			expected: 3,
			typ:      types.Int32,
			nullable: false,
		},
		{
			name: "coalesce(NULL, NULL, '3')",
			input: []sql.Expression{
				nil,
				nil,
				expression.NewLiteral("3", types.LongText),
			},
			expected: "3",
			typ:      types.LongText,
			nullable: false,
		},
		{
			name: "coalesce(NULL, '2', 3)",
			input: []sql.Expression{
				nil,
				expression.NewLiteral("2", types.LongText),
				expression.NewLiteral(3, types.Int32),
			},
			expected: "2",
			typ:      types.LongText,
			nullable: false,
		},
		{
			name: "coalesce(NULL, NULL, NULL)",
			input: []sql.Expression{
				nil,
				nil,
				nil,
			},
			expected: nil,
			typ:      types.Null,
			nullable: true,
		},
		{
			name: "coalesce(int(1), decimal(2.0), string('3'))",
			input: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral(decimal.NewFromFloat(2.0), types.MustCreateDecimalType(10, 0)),
				expression.NewLiteral("3", types.LongText),
			},
			expected: 1,
			typ:      types.LongText,
			nullable: false,
		},
		{
			name: "coalesce(signed(1), unsigned(2))",
			input: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral(2, types.Uint32),
			},
			expected: 1,
			typ:      types.MustCreateDecimalType(20, 0),
			nullable: false,
		},
		{
			name: "coalesce(signed(1), unsigned(2))",
			input: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral(2, types.Uint32),
			},
			expected: 1,
			typ:      types.MustCreateDecimalType(20, 0),
			nullable: false,
		},
		{
			name: "coalesce(decimal(1.0), float64(2.0))",
			input: []sql.Expression{
				expression.NewLiteral(1, types.MustCreateDecimalType(10, 0)),
				expression.NewLiteral(2, types.Float64),
			},
			expected: 1,
			typ:      types.Float64,
			nullable: false,
		},
		{
			name: "coalesce(float64(2.0))",
			input: []sql.Expression{
				expression.NewLiteral(2, types.Float64),
			},
			expected: 2,
			typ:      types.Float64,
			nullable: false,
		},
		{
			name: "coalesce(1, float64(2.0))",
			input: []sql.Expression{
				expression.NewLiteral(1, types.Float64),
			},
			expected: 1,
			typ:      types.Float64,
			nullable: false,
		},
	}

	for _, tt := range testCases {
		c, err := NewCoalesce(tt.input...)
		require.NoError(t, err)

		require.Equal(t, tt.typ, c.Type())
		require.Equal(t, tt.nullable, c.IsNullable())
		v, err := c.Eval(sql.NewEmptyContext(), nil)
		require.NoError(t, err)
		require.Equal(t, tt.expected, v)
	}
}

func TestComposeCoalasce(t *testing.T) {
	ctx := sql.NewEmptyContext()
	c1, err := NewCoalesce(nil)
	require.NoError(t, err)
	require.Equal(t, types.Null, c1.Type())
	v, err := c1.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, v)

	c2, err := NewCoalesce(nil, expression.NewLiteral(1, types.Int32))
	require.NoError(t, err)
	require.Equal(t, types.Int32, c2.Type())
	v, err = c2.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)

	c3, err := NewCoalesce(nil, c1, c2)
	require.NoError(t, err)
	require.Equal(t, types.Int32, c3.Type())
	v, err = c3.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)

	c4, err := NewCoalesce(expression.NewLiteral(nil, types.Null), c1, c2)
	require.NoError(t, err)
	require.Equal(t, types.Int32, c4.Type())
	v, err = c4.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)
}
