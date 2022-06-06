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

func TestSwapXY(t *testing.T) {
	t.Run("point swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 2, Y: 1}, v)
	})

	t.Run("linestring swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("polygon swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("swap wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(123, sql.Int64))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("null is null", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("geometry point swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 2, Y: 1}, v)
	})

	t.Run("geometry linestring swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, sql.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("geometry polygon swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, sql.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, err = typ.Convert(v)
		require.NoError(err)
	})
}
