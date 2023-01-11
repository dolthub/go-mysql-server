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
	// Point vs Point
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

	// Point vs LineString
	t.Run("point within linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within closed linestring of length 0", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 123, Y: 456}
		l := sql.LineString{Points: []sql.Point{p, p}}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l = sql.LineString{Points: []sql.Point{p, p, p, p, p}}
		f = NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
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

	t.Run("endpoints of linestring are not within linestring", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 1}
		p2 := sql.Point{X: 2, Y: 2}
		p3 := sql.Point{X: 3, Y: 3}
		l := sql.LineString{Points: []sql.Point{p1, p2, p3}}

		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("endpoints of linestring that overlap with linestring are not within linestring", func(t *testing.T) {
		require := require.New(t)

		// it looks like two triangles:
		//  /\  |  /\
		// /__s_|_e__\
		s := sql.Point{X: -1, Y: 0}
		p1 := sql.Point{X: -2, Y: 1}

		p2 := sql.Point{X: -3, Y: 0}
		p3 := sql.Point{X: 3, Y: 0}

		p4 := sql.Point{X: 2, Y: 1}
		e := sql.Point{X: 1, Y: 0}

		l := sql.LineString{Points: []sql.Point{s, p1, p2, p3, p4, e}}

		f := NewWithin(expression.NewLiteral(s, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(e, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Point vs Polygon
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

	t.Run("point within polygon (square) with hole", func(t *testing.T) {
		require := require.New(t)

		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}

		poly := sql.Polygon{Lines: []sql.LineString{l1, l2}}

		// passes through segments c2d2, a1b1, and a2b2; overlaps segment d2a2
		p1 := sql.Point{X: -3, Y: 2}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through segments c2d2, a1b1, and a2b2
		p2 := sql.Point{X: -3, Y: 0}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through segments c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := sql.Point{X: -3, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within polygon (diamond) with hole", func(t *testing.T) {
		require := require.New(t)

		a1 := sql.Point{X: 0, Y: 4}
		b1 := sql.Point{X: 4, Y: 0}
		c1 := sql.Point{X: 0, Y: -4}
		d1 := sql.Point{X: -4, Y: 0}

		a2 := sql.Point{X: 0, Y: 2}
		b2 := sql.Point{X: 2, Y: 0}
		c2 := sql.Point{X: 0, Y: -2}
		d2 := sql.Point{X: -2, Y: 0}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}

		poly := sql.Polygon{Lines: []sql.LineString{l1, l2}}

		p1 := sql.Point{X: -3, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex a2 and segment a1b1
		p2 := sql.Point{X: -1, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p3 := sql.Point{X: -1, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
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

		f := NewWithin(expression.NewLiteral(a, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
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

	t.Run("point not within polygon intersects vertex", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: -1, Y: 0}
		b := sql.Point{X: 0, Y: 1}
		c := sql.Point{X: 1, Y: 0}
		d := sql.Point{X: 0, Y: -1}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		// passes through vertex b
		p1 := sql.Point{X: -0.5, Y: 1}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through vertex a and c
		p2 := sql.Point{X: -2, Y: 0}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through vertex d
		p3 := sql.Point{X: -0.5, Y: -1}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point not within polygon (square) with hole", func(t *testing.T) {
		require := require.New(t)

		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}

		poly := sql.Polygon{Lines: []sql.LineString{l1, l2}}

		// passes through segments a1b1 and a2b2
		p1 := sql.Point{X: 0, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment d2a2
		p2 := sql.Point{X: -5, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := sql.Point{X: -5, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point not within polygon (diamond) with hole", func(t *testing.T) {
		require := require.New(t)

		a1 := sql.Point{X: 0, Y: 4}
		b1 := sql.Point{X: 4, Y: 0}
		c1 := sql.Point{X: 0, Y: -4}
		d1 := sql.Point{X: -4, Y: 0}

		a2 := sql.Point{X: 0, Y: 2}
		b2 := sql.Point{X: 2, Y: 0}
		c2 := sql.Point{X: 0, Y: -2}
		d2 := sql.Point{X: -2, Y: 0}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}

		poly := sql.Polygon{Lines: []sql.LineString{l1, l2}}

		// passes through vertexes d2, b2, and b1
		p1 := sql.Point{X: -3, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex a2 and segment a1b1
		p2 := sql.Point{X: -1, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex c2 and segment b1c1
		p3 := sql.Point{X: -1, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within polygon (square) with hole in hole", func(t *testing.T) {
		require := require.New(t)

		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}

		a3 := sql.Point{X: 2, Y: 2}
		b3 := sql.Point{X: 2, Y: -2}
		c3 := sql.Point{X: -2, Y: -2}
		d3 := sql.Point{X: -2, Y: 2}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}
		l3 := sql.LineString{Points: []sql.Point{a3, b3, c3, d3, a3}}

		poly := sql.Polygon{Lines: []sql.LineString{l1, l2, l3}}

		// passes through segments a1b1 and a2b2
		p1 := sql.Point{X: 0, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment d2a2
		p2 := sql.Point{X: -5, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := sql.Point{X: -5, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(poly, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Point vs MultiPoint
	t.Run("points within multipoint", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 1}
		p2 := sql.Point{X: 2, Y: 2}
		p3 := sql.Point{X: 3, Y: 3}
		mp := sql.MultiPoint{Points: []sql.Point{p1, p2, p3}}

		var f sql.Expression
		var v interface{}
		var err error
		f = NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within multipoint", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		p1 := sql.Point{X: 1, Y: 1}
		p2 := sql.Point{X: 2, Y: 2}
		p3 := sql.Point{X: 3, Y: 3}
		mp := sql.MultiPoint{Points: []sql.Point{p1, p2, p3}}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Point vs MultiLineString
	t.Run("points within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: -1, Y: -1}
		p2 := sql.Point{X: 1, Y: 1}
		p3 := sql.Point{X: 123, Y: 456}
		l1 := sql.LineString{Points: []sql.Point{p1, p2}}
		l2 := sql.LineString{Points: []sql.Point{p3, p3}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l1, l2}}

		var f sql.Expression
		var v interface{}
		var err error
		f = NewWithin(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(ml, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p := sql.Point{X: 0, Y: 0}
		f = NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(ml, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: -1, Y: -1}
		p2 := sql.Point{X: 1, Y: 1}
		p3 := sql.Point{X: 123, Y: 456}
		l1 := sql.LineString{Points: []sql.Point{p1, p2}}
		l2 := sql.LineString{Points: []sql.Point{p3, p3}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l1, l2}}

		var f sql.Expression
		var v interface{}
		var err error
		f = NewWithin(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		p := sql.Point{X: 100, Y: 1000}
		f = NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Point vs MultiPolygon
	t.Run("point within multipolygon", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}

		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}
		mp := sql.MultiPolygon{Polygons: []sql.Polygon{{Lines: []sql.LineString{l1}}, {Lines: []sql.LineString{l2}}}}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not within multipolygon", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 100, Y: 100}

		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}

		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}
		mp := sql.MultiPolygon{Polygons: []sql.Polygon{{Lines: []sql.LineString{l1}}, {Lines: []sql.LineString{l2}}}}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Point vs GeometryCollection
	t.Run("point within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		gc := sql.GeomColl{}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("point within geometrycollection", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{p}}

		f := NewWithin(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// LineString vs Point
	t.Run("linestring never within point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs LineString
	t.Run("linestring within linestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: -5, Y: -5}
		d := sql.Point{X: 5, Y: 5}
		l1 := sql.LineString{Points: []sql.Point{a, b}}
		l2 := sql.LineString{Points: []sql.Point{c, d}}
		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring within itself", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{a, b}}
		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("many line segments within larger line segment", func(t *testing.T) {
		require := require.New(t)

		a := sql.Point{X: 1, Y: 1}
		b := sql.Point{X: 2, Y: 2}
		c := sql.Point{X: 3, Y: 3}
		l1 := sql.LineString{Points: []sql.Point{a, b, c}}

		p := sql.Point{X: 0, Y: 0}
		q := sql.Point{X: 4, Y: 4}
		l2 := sql.LineString{Points: []sql.Point{p, q}}

		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("larger line segment within many small line segments", func(t *testing.T) {
		require := require.New(t)

		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		d := sql.Point{X: 3, Y: 3}
		e := sql.Point{X: 4, Y: 4}
		l1 := sql.LineString{Points: []sql.Point{b, d}}
		l2 := sql.LineString{Points: []sql.Point{a, b, c, d, e}}

		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("alternating line segments", func(t *testing.T) {
		require := require.New(t)

		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		d := sql.Point{X: 3, Y: 3}
		e := sql.Point{X: 4, Y: 4}
		l1 := sql.LineString{Points: []sql.Point{b, d}}
		l2 := sql.LineString{Points: []sql.Point{a, c, e}}

		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not within perpendicular linestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 1, Y: 0}
		d := sql.Point{X: 0, Y: 1}
		l1 := sql.LineString{Points: []sql.Point{a, b}}
		l2 := sql.LineString{Points: []sql.Point{c, d}}
		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(l2, sql.LineStringType{}), expression.NewLiteral(l1, sql.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("axis-aligned perpendicular linestring not within", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 0, Y: 1}
		c := sql.Point{X: 1, Y: 0}
		l1 := sql.LineString{Points: []sql.Point{a, b}}
		l2 := sql.LineString{Points: []sql.Point{a, c}}
		f := NewWithin(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(l2, sql.LineStringType{}), expression.NewLiteral(l1, sql.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("terminal line points not in line", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{a, b}}
		la := sql.LineString{Points: []sql.Point{a, a}}
		lb := sql.LineString{Points: []sql.Point{b, b}}
		f := NewWithin(expression.NewLiteral(la, sql.LineStringType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(lb, sql.LineStringType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs Polygon
	t.Run("linestring within polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		p := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		i := sql.Point{X: -1, Y: -1}
		j := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring touching boundary is within polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		p := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		i := sql.Point{X: -1, Y: -1}
		j := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		p := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		i := sql.Point{X: -100, Y: 100}
		j := sql.Point{X: 100, Y: 100}
		l := sql.LineString{Points: []sql.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring crosses through polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		p := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{a, b, c, d, a}}}}

		i := sql.Point{X: -100, Y: -100}
		j := sql.Point{X: 100, Y: 100}
		l := sql.LineString{Points: []sql.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring boundary is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		l := sql.LineString{Points: []sql.Point{a, b, c, d, a}}
		p := sql.Polygon{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring in hole is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a1 := sql.Point{X: 4, Y: 4}
		b1 := sql.Point{X: 4, Y: -4}
		c1 := sql.Point{X: -4, Y: -4}
		d1 := sql.Point{X: -4, Y: 4}
		l1 := sql.LineString{Points: []sql.Point{a1, b1, c1, d1, a1}}

		a2 := sql.Point{X: 2, Y: 2}
		b2 := sql.Point{X: 2, Y: -2}
		c2 := sql.Point{X: -2, Y: -2}
		d2 := sql.Point{X: -2, Y: 2}
		l2 := sql.LineString{Points: []sql.Point{a2, b2, c2, d2, a2}}
		p := sql.Polygon{Lines: []sql.LineString{l1, l2}}

		i := sql.Point{X: -1, Y: -1}
		j := sql.Point{X: 1, Y: 1}
		l := sql.LineString{Points: []sql.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring crosses exterior not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 0}
		b := sql.Point{X: -4, Y: 0}
		c := sql.Point{X: -2, Y: 4}
		d := sql.Point{X: 0, Y: 2}
		e := sql.Point{X: 2, Y: 4}
		l1 := sql.LineString{Points: []sql.Point{a, b, c, d, e, a}}
		p := sql.Polygon{Lines: []sql.LineString{l1}}

		i := sql.Point{X: -2, Y: 3}
		j := sql.Point{X: 2, Y: 3}
		l := sql.LineString{Points: []sql.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(p, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs MultiPoint
	t.Run("linestring never within multipoint", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 4, Y: 4}
		b := sql.Point{X: 4, Y: -4}
		c := sql.Point{X: -4, Y: -4}
		d := sql.Point{X: -4, Y: 4}
		l := sql.LineString{Points: []sql.Point{a, b, c, d}}
		mp := sql.MultiPoint{Points: []sql.Point{a, b, c, d}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs MultiLineString
	t.Run("linestring within multilinestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		l := sql.LineString{Points: []sql.Point{a, b, c}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// TODO: need to do that weird even odd thing...
	t.Run("linestring within split multilinestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		l1 := sql.LineString{Points: []sql.Point{a, b}}
		l2 := sql.LineString{Points: []sql.Point{b, c}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l1, l2}}
		l := sql.LineString{Points: []sql.Point{a, c}}
		f := NewWithin(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("terminal line points not ", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		ab := sql.LineString{Points: []sql.Point{a, b}}
		bc := sql.LineString{Points: []sql.Point{b, c}}
		ml := sql.MultiLineString{Lines: []sql.LineString{ab, bc}}

		aa := sql.LineString{Points: []sql.Point{a, a}}
		bb := sql.LineString{Points: []sql.Point{b, b}}
		cc := sql.LineString{Points: []sql.Point{c, c}}
		f := NewWithin(expression.NewLiteral(aa, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(bb, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
		f = NewWithin(expression.NewLiteral(cc, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs MultiPolygon
	t.Run("terminal line points not in multilinestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 2, Y: 2}
		ab := sql.LineString{Points: []sql.Point{a, b}}
		bc := sql.LineString{Points: []sql.Point{b, c}}
		ml := sql.MultiLineString{Lines: []sql.LineString{ab, bc}}

		aa := sql.LineString{Points: []sql.Point{a, a}}
		bb := sql.LineString{Points: []sql.Point{b, b}}
		cc := sql.LineString{Points: []sql.Point{c, c}}
		f := NewWithin(expression.NewLiteral(aa, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(bb, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
		f = NewWithin(expression.NewLiteral(cc, sql.LineStringType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs GeometryCollection

	// Polygon vs Point
	t.Run("polygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p, p, p}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, sql.PolygonType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs LineString
	t.Run("polygon never within linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p, p, p}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, sql.PolygonType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs Polygon

	// Polygon vs MultiPoint
	t.Run("polygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p, p, p}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, sql.PolygonType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs MultiLineString
	t.Run("polygon never within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p, p, p}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, sql.PolygonType{}), expression.NewLiteral(ml, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs MultiPolygon

	// Polygon vs GeometryCollection

	// MultiPoint vs Point

	// MultiPoint vs LineString

	// MultiPoint vs Polygon

	// MultiPoint vs MultiPoint

	// MultiPoint vs MultiLineString

	// MultiPoint vs MultiPolygon

	// MultiPoint vs GeometryCollection

	// MultiLineString vs Point
	t.Run("multilinestring never within point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l}}

		f := NewWithin(expression.NewLiteral(ml, sql.LineStringType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiLineString vs LineString

	// MultiLineString vs Polygon

	// MultiLineString vs MultiPoint

	// MultiLineString vs MultiLineString

	// MultiLineString vs MultiPolygon

	// MultiLineString vs GeometryCollection

	// MultiPolygon vs Point
	t.Run("multipolygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}
		mpoly := sql.MultiPolygon{Polygons: []sql.Polygon{poly}}

		f := NewWithin(expression.NewLiteral(mpoly, sql.MultiPolygonType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPolygon vs LineString
	t.Run("multipolygon never within linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		l := sql.LineString{Points: []sql.Point{p, p}}
		poly := sql.Polygon{Lines: []sql.LineString{l}}
		mpoly := sql.MultiPolygon{Polygons: []sql.Polygon{poly}}

		f := NewWithin(expression.NewLiteral(mpoly, sql.MultiPolygonType{}), expression.NewLiteral(l, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPolygon vs Polygon

	// MultiPolygon vs MultiPoint

	// MultiPolygon vs MultiLineString

	// MultiPolygon vs MultiPolygon

	// MultiPolygon vs GeometryCollection

	// GeometryCollection vs Point

	// GeometryCollection vs LineString

	// GeometryCollection vs Polygon

	// GeometryCollection vs MultiPoint

	// GeometryCollection vs MultiLineString

	// GeometryCollection vs MultiPolygon

	// GeometryCollection vs GeometryCollection
}
