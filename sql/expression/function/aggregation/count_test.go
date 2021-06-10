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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestCountEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(sql.NewEmptyContext(), expression.NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, nil))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Merge(ctx, b, b2))
	require.Equal(int64(7), eval(t, c, b))
}

func TestCountEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(sql.NewEmptyContext(), expression.NewStar())
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, sql.NewRow()))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Merge(ctx, b, b2))
	require.Equal(int64(7), eval(t, c, b))
}

func TestCountEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(sql.NewEmptyContext(), expression.NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.Equal(int64(1), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.Equal(int64(1), eval(t, c, b))
}

func TestCountDistinctEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(1), eval(t, c, b))
}

func TestCountDistinctEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewStar())
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b2, sql.NewRow(5)))
	require.NoError(c.Merge(ctx, b, b2))

	require.Equal(int64(6), eval(t, c, b))
}

func TestCountDistinctEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.Equal(int64(1), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow("bar")))
	require.Equal(int64(2), eval(t, c, b))
}
