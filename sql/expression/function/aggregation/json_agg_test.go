// Copyright 2021 Dolthub, Inc.
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

func TestJsonArrayAgg_Name(t *testing.T) {
	assert := require.New(t)

	m := NewJSONArrayAgg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "field", true))
	assert.Equal("JSON_ARRAYAGG(field)", m.String())
}

func TestJsonArrayAgg_SimpleIntField(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJSONArrayAgg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "field", true))
	b := j.NewBuffer()

	j.Update(ctx, b, sql.NewRow(float64(7)))
	j.Update(ctx, b, sql.NewRow(float64(2)))

	v, err := j.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(sql.MustJSON(`[7, 2]`), v)
}

func TestJsonArrayAgg_Strings(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJSONArrayAgg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "field", true))
	b := j.NewBuffer()

	j.Update(ctx, b, sql.NewRow("hi"))
	j.Update(ctx, b, sql.NewRow("hello"))

	v, err := j.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(sql.MustJSON(`["hi","hello"]`), v)
}

func TestJsonArrayAgg_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJSONArrayAgg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "field", true))
	b := j.NewBuffer()

	v, err := j.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(sql.JSONDocument{Val: []interface{}(nil)}, v)
}

func TestJsonArrayAgg_JSON(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJSONArrayAgg(sql.NewEmptyContext(), expression.NewGetField(0, sql.JSON, "field", true))
	b := j.NewBuffer()
	j.Update(ctx, b, sql.NewRow(sql.MustJSON(`{"key1": "value1", "key2": "value2"}`)))

	v, err := j.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(sql.MustJSON(`[{"key1": "value1", "key2": "value2"}]`), v)
}
