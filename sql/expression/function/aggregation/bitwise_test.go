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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestBitAnd_String(t *testing.T) {
	assert := require.New(t)
	m := NewBitAnd(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BITAND(field)", m.String())
}

func TestBitAnd_Eval_Int(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitAnd(expression.NewGetField(0, types.Int64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1))
	b.Update(ctx, sql.NewRow(3))
	b.Update(ctx, sql.NewRow(7))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(1), v)
}

func TestBitAnd_Eval_Float64(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitAnd(expression.NewGetField(0, types.Float64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1.123123))
	b.Update(ctx, sql.NewRow(3.3452345))
	b.Update(ctx, sql.NewRow(7.1123123))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(1), v)
}

func TestBitAnd_Eval_Text(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitAnd(expression.NewGetField(0, types.Text, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow("a"))
	b.Update(ctx, sql.NewRow("A"))
	b.Update(ctx, sql.NewRow("b"))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitAnd_Eval_NULL(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitAnd(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(^uint64(0), v)
}

func TestBitAnd_Eval_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitAnd(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(^uint64(0), v)
}

func TestBitOr_String(t *testing.T) {
	assert := require.New(t)
	m := NewBitOr(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BITOR(field)", m.String())
}

func TestBitOr_Eval_Int(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitOr(expression.NewGetField(0, types.Int64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1))
	b.Update(ctx, sql.NewRow(2))
	b.Update(ctx, sql.NewRow(4))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(7), v)
}

func TestBitOr_Eval_Float64(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitOr(expression.NewGetField(0, types.Float64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1.123123))
	b.Update(ctx, sql.NewRow(2.3452345))
	b.Update(ctx, sql.NewRow(4.1123123))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(7), v)
}

func TestBitOr_Eval_Text(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitOr(expression.NewGetField(0, types.Text, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow("a"))
	b.Update(ctx, sql.NewRow("A"))
	b.Update(ctx, sql.NewRow("b"))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitOr_Eval_NULL(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitOr(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitOr_Eval_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitOr(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitXor_String(t *testing.T) {
	assert := require.New(t)
	m := NewBitXor(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BITXOR(field)", m.String())
}

func TestBitXor_Eval_Int(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitXor(expression.NewGetField(0, types.Int64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1))
	b.Update(ctx, sql.NewRow(2))
	b.Update(ctx, sql.NewRow(5))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(6), v)
}

func TestBitXor_Eval_Float64(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitXor(expression.NewGetField(0, types.Float64, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(1.123123))
	b.Update(ctx, sql.NewRow(2.3452345))
	b.Update(ctx, sql.NewRow(5.1123123))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(6), v)
}

func TestBitXor_Eval_Text(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitXor(expression.NewGetField(0, types.Text, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow("a"))
	b.Update(ctx, sql.NewRow("A"))
	b.Update(ctx, sql.NewRow("b"))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitXor_Eval_NULL(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitXor(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))
	b.Update(ctx, sql.NewRow(nil))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}

func TestBitXor_Eval_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewBitXor(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := m.NewBuffer()

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(uint64(0), v)
}
