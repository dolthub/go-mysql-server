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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestEmptyCoalesce(t *testing.T) {
	_, err := NewCoalesce(sql.NewEmptyContext())
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
		{"coalesce(1, 2, 3)", []sql.Expression{expression.NewLiteral(1, sql.Int32), expression.NewLiteral(2, sql.Int32), expression.NewLiteral(3, sql.Int32)}, 1, sql.Int32, false},
		{"coalesce(NULL, NULL, 3)", []sql.Expression{nil, nil, expression.NewLiteral(3, sql.Int32)}, 3, sql.Int32, false},
		{"coalesce(NULL, NULL, '3')", []sql.Expression{nil, nil, expression.NewLiteral("3", sql.LongText)}, "3", sql.LongText, false},
		{"coalesce(NULL, '2', 3)", []sql.Expression{nil, expression.NewLiteral("2", sql.LongText), expression.NewLiteral(3, sql.Int32)}, "2", sql.LongText, false},
		{"coalesce(NULL, NULL, NULL)", []sql.Expression{nil, nil, nil}, nil, nil, true},
	}

	for _, tt := range testCases {
		c, err := NewCoalesce(sql.NewEmptyContext(), tt.input...)
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
	c1, err := NewCoalesce(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, c1.Type())
	v, err := c1.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, v)

	c2, err := NewCoalesce(ctx, nil, expression.NewLiteral(1, sql.Int32))
	require.NoError(t, err)
	require.Equal(t, sql.Int32, c2.Type())
	v, err = c2.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)

	c, err := NewCoalesce(ctx, nil, c1, c2)
	require.NoError(t, err)
	require.Equal(t, sql.Int32, c.Type())
	v, err = c.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)
}
