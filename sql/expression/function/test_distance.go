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

func TestDistance(t *testing.T) {
	t.Run("two points", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral(sql.Point{X: 0, Y: 1}, sql.PointType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("two float points", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Point{X: 1.5, Y: 1.5}, sql.PointType{}),
			expression.NewLiteral(sql.Point{X: 3.5, Y: 1.5}, sql.PointType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("point to line", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 0, Y: 5},{X: 100, Y: 100},{X: 300, Y: 300}}}, sql.LinestringType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("line to point", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 0, Y: 5},{X: 100, Y: 100},{X: 300, Y: 300}}}, sql.LinestringType{}),
			expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("line to line", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 1, Y: 5},{X: 200, Y: 100},{X: 123, Y: 123}}}, sql.LinestringType{}),
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 5, Y: 5},{X: 450, Y: 900},{X: 666, Y: 999}}}, sql.LinestringType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(4, v)
	})

	t.Run("line to poly", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 1, Y: 1},{X: 2, Y: 2}}}, sql.LinestringType{}),
			expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0},{X: 1, Y: 1},{X: 1, Y: 0},{X: 0, Y: 0}}}}}, sql.PolygonType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("poly to line", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0},{X: 1, Y: 1},{X: 1, Y: 0},{X: 0, Y: 0}}}}}, sql.PolygonType{}),
			expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 1, Y: 1},{X: 2, Y: 2}}}, sql.LinestringType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("poly to poly", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0},{X: 1, Y: 1},{X: 1, Y: 0},{X: 0, Y: 0}}}}}, sql.PolygonType{}),
			expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 1, Y: 0},{X: 2, Y: 1},{X: 2, Y: 0},{X: 1, Y: 0}}}}}, sql.PolygonType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("null geometry 1", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			nil,
			expression.NewLiteral(sql.Point{X: 0, Y: 1}, sql.PointType{}),
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null geometry 2", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Point{X: 0, Y: 1}, sql.PointType{}),
			nil,
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null units", func(t *testing.T) {
		require := require.New(t)
		f, err := NewDistance(
			expression.NewLiteral(sql.Point{X: 0, Y: 1}, sql.PointType{}),
			expression.NewLiteral(sql.Point{X: 0, Y: 1}, sql.PointType{}),
			nil,
		)
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
