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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestCountEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewLiteral(1, types.Int32))
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), evalBuffer(t, b))
}

func TestCountEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewStar())
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), evalBuffer(t, b))
}

func TestCountEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewGetField(0, types.Text, "", true))
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.Equal(int64(1), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.Equal(int64(1), evalBuffer(t, b))
}

func TestCountDistinctEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewLiteral(1, types.Int32))
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(1), evalBuffer(t, b))
}

func TestCountDistinctEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewStar())
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), evalBuffer(t, b))
}

func TestCountDistinctEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewGetField(0, types.Text, "", true))
	b, _ := c.NewBuffer()
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.Equal(int64(1), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow("bar")))
	require.Equal(int64(2), evalBuffer(t, b))
}
