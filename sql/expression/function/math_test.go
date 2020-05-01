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

package function

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRand(t *testing.T) {
	r, _ := NewRand()

	assert.Equal(t, sql.Float64, r.Type())
	assert.Equal(t, "RAND()", r.String())

	f, err := r.Eval(nil, nil)
	require.NoError(t, err)
	f64, ok := f.(float64)
	require.True(t, ok, "not a float64")

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642, ok := f.(float64)
	require.True(t, ok, "not a float64")

	assert.NotEqual(t, f64, f642) // i guess this could fail, but come on
}

func TestRandWithSeed(t *testing.T) {
	r, _ := NewRand(expression.NewLiteral(10, sql.Int8))

	assert.Equal(t, sql.Float64, r.Type())
	assert.Equal(t, "RAND(10)", r.String())

	f, err := r.Eval(nil, nil)
	require.NoError(t, err)
	f64 := f.(float64)

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642 := f.(float64)

	assert.Equal(t, f64, f642)

	r, _ = NewRand(expression.NewLiteral("not a number", sql.LongText))
	assert.Equal(t, `RAND("not a number")`, r.String())

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f64 = f.(float64)

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642 = f.(float64)

	assert.Equal(t, f64, f642)
}