// Copyright 2023 Dolthub, Inc.
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

package spatial

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestWithin(t *testing.T) {
	t.Run("point within point", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 2}
		p2 := sql.Point{X: 1, Y: 2}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within point", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 2}
		p2 := sql.Point{X: 123, Y: 456}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point within linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point is endpoint of linestring", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 1}
		p2 := sql.Point{X: 2, Y: 2}
		p3 := sql.Point{X: 3, Y: 3}
		l := sql.LineString{Points: []sql.Point{p1, p2, p3}}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 100, Y: 200}
		l := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point within polygon", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 1, Y: 1}
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 0, Y: 2}
		c := sql.Point{X: 2, Y: 2}
		d := sql.Point{X: 2, Y: 0}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}
		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within polygon intersects vertex", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		a := sql.Point{X: -1, Y: 0}
		b := sql.Point{X: 0, Y: 1}
		c := sql.Point{X: 1, Y: 0}
		d := sql.Point{X: 0, Y: -1}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}
		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point on polygon boundary not within", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: -1, Y: 0}
		b := sql.Point{X: 0, Y: 1}
		c := sql.Point{X: 1, Y: 0}
		d := sql.Point{X: 0, Y: -1}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		var f sql.Expression
		var v interface{}
		var err error
		f = NewWithin(expression.NewLiteral(a, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(b, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(c, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(d, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

	})

}
