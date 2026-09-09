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

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonArrayAgg_Name(t *testing.T) {
	assert := require.New(t)

	m := NewJsonArray(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("JSON_ARRAYAGG(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("JSON_ARRAYAGG(field) over ()", windowed.String())
	exprtest.AssertFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "JSON_ARRAYAGG")
}

func TestJSONObjectAggString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := NewJSONObjectAgg(
		ctx,
		expression.NewGetField(0, types.Text, "k", false),
		expression.NewGetField(1, types.Int64, "v", false),
	).(sql.WindowAdaptableExpression).WithWindow(
		ctx,
		sql.NewWindowDefinition(
			[]sql.Expression{expression.NewGetField(2, types.Int64, "g", false)},
			nil, nil, "", "",
		),
	)
	require.Equal(t, "JSON_OBJECTAGG(k, v) over ( partition by g)", expr.String())
	exprtest.AssertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestJsonArrayAgg_SimpleIntField(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJsonArray(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := j.NewBuffer(ctx)

	b.Update(ctx, sql.NewRow(float64(7)))
	b.Update(ctx, sql.NewRow(float64(2)))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(types.MustJSON(`[7, 2]`), v)
}

func TestJsonArrayAgg_Strings(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJsonArray(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := j.NewBuffer(ctx)

	b.Update(ctx, sql.NewRow("hi"))
	b.Update(ctx, sql.NewRow("hello"))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(types.MustJSON(`["hi","hello"]`), v)
}

func TestJsonArrayAgg_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJsonArray(expression.NewGetField(0, types.Int32, "field", true))
	b, _ := j.NewBuffer(ctx)

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(types.JSONDocument{Val: []interface{}(nil)}, v)
}

func TestJsonArrayAgg_JSON(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	j := NewJsonArray(expression.NewGetField(0, types.JSON, "field", true))
	b, _ := j.NewBuffer(ctx)
	b.Update(ctx, sql.NewRow(types.MustJSON(`{"key1": "value1", "key2": "value2"}`)))

	v, err := b.Eval(ctx)
	assert.NoError(err)
	assert.Equal(types.MustJSON(`[{"key1": "value1", "key2": "value2"}]`), v)
}
