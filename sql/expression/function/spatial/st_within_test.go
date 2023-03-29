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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPointWithinPoint(t *testing.T) {
	t.Run("point within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 1, Y: 2}
		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within point", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 1, Y: 2}
		p2 := types.Point{X: 123, Y: 456}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(p2, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinLineString(t *testing.T) {
	t.Run("point within linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within closed linestring of length 0", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 123, Y: 456}
		l := types.LineString{Points: []types.Point{p, p}}

		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(l, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l = types.LineString{Points: []types.Point{p, p, p, p, p}}
		f = NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(l, types.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 100, Y: 200}
		l := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(l, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("terminal points are not within linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(simpleLineString.Points[0], types.PointType{}), expression.NewLiteral(simpleLineString, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(simpleLineString.Points[2], types.PointType{}), expression.NewLiteral(simpleLineString, types.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("overlapping terminal points are not within linestring", func(t *testing.T) {
		require := require.New(t)

		// it looks like two triangles:
		//  /\  |  /\
		// /__s_|_e__\
		s := types.Point{X: -1, Y: 0}
		p1 := types.Point{X: -2, Y: 1}
		p2 := types.Point{X: -3, Y: 0}
		p3 := types.Point{X: 3, Y: 0}
		p4 := types.Point{X: 2, Y: 1}
		e := types.Point{X: 1, Y: 0}

		l := types.LineString{Points: []types.Point{s, p1, p2, p3, p4, e}}

		f := NewWithin(expression.NewLiteral(s, types.PointType{}), expression.NewLiteral(l, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(e, types.PointType{}), expression.NewLiteral(l, types.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinPolygon(t *testing.T) {
	t.Run("point within polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(square, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within polygon intersects vertex", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		a := types.Point{X: -1, Y: 0}
		b := types.Point{X: 0, Y: 1}
		c := types.Point{X: 1, Y: 0}
		d := types.Point{X: 0, Y: -1}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}
		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within polygon (square) with hole", func(t *testing.T) {
		require := require.New(t)
		// passes through segments c2d2, a1b1, and a2b2; overlaps segment d2a2
		p1 := types.Point{X: -3, Y: 2}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through segments c2d2, a1b1, and a2b2
		p2 := types.Point{X: -3, Y: 0}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through segments c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := types.Point{X: -3, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point within polygon (diamond) with hole", func(t *testing.T) {
		require := require.New(t)

		p1 := types.Point{X: -3, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex a2 and segment a1b1
		p2 := types.Point{X: -1, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p3 := types.Point{X: -1, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point on polygon boundary not within", func(t *testing.T) {
		require := require.New(t)

		f := NewWithin(expression.NewLiteral(diamond.Lines[0].Points[0], types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(diamond.Lines[0].Points[1], types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(diamond.Lines[0].Points[2], types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(diamond.Lines[0].Points[3], types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point not within polygon intersects vertex", func(t *testing.T) {
		require := require.New(t)

		// passes through vertex b
		p1 := types.Point{X: -1, Y: 4}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through vertex a and c
		p2 := types.Point{X: -5, Y: 0}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through vertex d
		p3 := types.Point{X: -1, Y: -4}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(diamond, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point not within polygon (square) with hole", func(t *testing.T) {
		require := require.New(t)

		// passes through segments a1b1 and a2b2
		p1 := types.Point{X: 0, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment d2a2
		p2 := types.Point{X: -5, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := types.Point{X: -5, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point not within polygon (diamond) with hole", func(t *testing.T) {
		require := require.New(t)

		// passes through vertexes d2, b2, and b1
		p1 := types.Point{X: -3, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex a2 and segment a1b1
		p2 := types.Point{X: -1, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// passes through vertex c2 and segment b1c1
		p3 := types.Point{X: -1, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(diamondWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within polygon (square) with hole in hole", func(t *testing.T) {
		require := require.New(t)

		a3 := types.Point{X: 1, Y: 1}
		b3 := types.Point{X: 1, Y: -1}
		c3 := types.Point{X: -1, Y: -1}
		d3 := types.Point{X: -1, Y: 1}

		l3 := types.LineString{Points: []types.Point{a3, b3, c3, d3, a3}}
		poly := types.Polygon{Lines: []types.LineString{squareWithHole.Lines[0], squareWithHole.Lines[1], l3}}

		// passes through segments a1b1 and a2b2
		p1 := types.Point{X: 0, Y: 0}
		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment d2a2
		p2 := types.Point{X: -5, Y: 2}
		f = NewWithin(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// passes through segments c1d1, c2d2, a1b1, and a2b2; overlaps segment b2c2
		p3 := types.Point{X: -5, Y: -2}
		f = NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("point within non-simple polygon", func(t *testing.T) {
		require := require.New(t)
		// looks like a bowtie
		a := types.Point{X: -2, Y: 2}
		b := types.Point{X: 2, Y: 2}
		c := types.Point{X: 2, Y: -2}
		d := types.Point{X: -2, Y: -2}
		l := types.LineString{Points: []types.Point{a, c, b, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}

		o := types.Point{}
		w := types.Point{X: -1, Y: 0}
		x := types.Point{X: 0, Y: 1}
		y := types.Point{X: 1, Y: 0}
		z := types.Point{X: 0, Y: -1}

		f := NewWithin(expression.NewLiteral(o, types.PointType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(w, types.PointType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(x, types.PointType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(y, types.PointType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(z, types.PointType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinMultiPoint(t *testing.T) {
	t.Run("points within multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewWithin(expression.NewLiteral(simpleMultiPoint.Points[0], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(simpleMultiPoint.Points[0], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewWithin(expression.NewLiteral(simpleMultiPoint.Points[0], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewWithin(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinMultiLineString(t *testing.T) {
	t.Run("points within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: -1, Y: -1}
		p2 := types.Point{X: 1, Y: 1}
		p3 := types.Point{X: 123, Y: 456}
		l1 := types.LineString{Points: []types.Point{p1, p2}}
		l2 := types.LineString{Points: []types.Point{p3, p3}}
		ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}

		f := NewWithin(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(ml, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p := types.Point{X: 0, Y: 0}
		f = NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(ml, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: -1, Y: -1}
		p2 := types.Point{X: 1, Y: 1}
		p3 := types.Point{X: 123, Y: 456}
		l1 := types.LineString{Points: []types.Point{p1, p2}}
		l2 := types.LineString{Points: []types.Point{p3, p3}}
		ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}

		f := NewWithin(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		p := types.Point{X: 100, Y: 1000}
		f = NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinMultiPolygon(t *testing.T) {
	t.Run("point within multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(simpleMultiPolygon, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not within multipolygon", func(t *testing.T) {
		// TODO: fix this one
		require := require.New(t)
		p := types.Point{X: 100, Y: 100}

		a1 := types.Point{X: 4, Y: 4}
		b1 := types.Point{X: 4, Y: -4}
		c1 := types.Point{X: -4, Y: -4}
		d1 := types.Point{X: -4, Y: 4}

		a2 := types.Point{X: 2, Y: 2}
		b2 := types.Point{X: 2, Y: -2}
		c2 := types.Point{X: -2, Y: -2}
		d2 := types.Point{X: -2, Y: 2}

		l1 := types.LineString{Points: []types.Point{a1, b1, c1, d1, a1}}
		l2 := types.LineString{Points: []types.Point{a2, b2, c2, d2, a2}}
		mp := types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{l1}}, {Lines: []types.LineString{l2}}}}

		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointWithinGeometryCollection(t *testing.T) {
	t.Run("point within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("point within geometrycollection", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		gc := types.GeomColl{Geoms: []types.GeometryValue{p}}

		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not within geometrycollection", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		a := types.Point{X: 1, Y: 0}
		gc := types.GeomColl{Geoms: []types.GeometryValue{a}}

		f := NewWithin(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestWithin(t *testing.T) {
	t.Skip("comptation geometry is too hard...")

	// LineString vs Point
	t.Run("linestring never within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs LineString
	t.Run("linestring within linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: -5, Y: -5}
		d := types.Point{X: 5, Y: 5}
		l1 := types.LineString{Points: []types.Point{a, b}}
		l2 := types.LineString{Points: []types.Point{c, d}}
		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring within itself", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{a, b}}
		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("many line segments within larger line segment", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: 1, Y: 1}
		b := types.Point{X: 2, Y: 2}
		c := types.Point{X: 3, Y: 3}
		l1 := types.LineString{Points: []types.Point{a, b, c}}

		p := types.Point{X: 0, Y: 0}
		q := types.Point{X: 4, Y: 4}
		l2 := types.LineString{Points: []types.Point{p, q}}

		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("larger line segment within many small line segments", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 2, Y: 2}
		d := types.Point{X: 3, Y: 3}
		e := types.Point{X: 4, Y: 4}
		l1 := types.LineString{Points: []types.Point{b, d}}
		l2 := types.LineString{Points: []types.Point{a, b, c, d, e}}

		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("alternating line segments", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 2, Y: 2}
		d := types.Point{X: 3, Y: 3}
		e := types.Point{X: 4, Y: 4}
		l1 := types.LineString{Points: []types.Point{b, d}}
		l2 := types.LineString{Points: []types.Point{a, c, e}}

		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not within perpendicular linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: 0}
		d := types.Point{X: 0, Y: 1}
		l1 := types.LineString{Points: []types.Point{a, b}}
		l2 := types.LineString{Points: []types.Point{c, d}}
		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(l2, types.LineStringType{}), expression.NewLiteral(l1, types.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("axis-aligned perpendicular linestring not within", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 0, Y: 1}
		c := types.Point{X: 1, Y: 0}
		l1 := types.LineString{Points: []types.Point{a, b}}
		l2 := types.LineString{Points: []types.Point{a, c}}
		f := NewWithin(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(l2, types.LineStringType{}), expression.NewLiteral(l1, types.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("terminal line points not in line", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{a, b}}
		la := types.LineString{Points: []types.Point{a, a}}
		lb := types.LineString{Points: []types.Point{b, b}}
		f := NewWithin(expression.NewLiteral(la, types.LineStringType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(lb, types.LineStringType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs Polygon
	t.Run("linestring within polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		i := types.Point{X: -1, Y: -1}
		j := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring touching boundary is within polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		i := types.Point{X: -1, Y: -1}
		j := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		i := types.Point{X: -100, Y: 100}
		j := types.Point{X: 100, Y: 100}
		l := types.LineString{Points: []types.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring crosses through polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		i := types.Point{X: -100, Y: -100}
		j := types.Point{X: 100, Y: 100}
		l := types.LineString{Points: []types.Point{i, j, a, b}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring boundary is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring in hole is not within polygon", func(t *testing.T) {
		require := require.New(t)
		a1 := types.Point{X: 4, Y: 4}
		b1 := types.Point{X: 4, Y: -4}
		c1 := types.Point{X: -4, Y: -4}
		d1 := types.Point{X: -4, Y: 4}
		l1 := types.LineString{Points: []types.Point{a1, b1, c1, d1, a1}}

		a2 := types.Point{X: 2, Y: 2}
		b2 := types.Point{X: 2, Y: -2}
		c2 := types.Point{X: -2, Y: -2}
		d2 := types.Point{X: -2, Y: 2}
		l2 := types.LineString{Points: []types.Point{a2, b2, c2, d2, a2}}
		p := types.Polygon{Lines: []types.LineString{l1, l2}}

		i := types.Point{X: -1, Y: -1}
		j := types.Point{X: 1, Y: 1}
		l := types.LineString{Points: []types.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring crosses exterior not within polygon", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 0}
		b := types.Point{X: -4, Y: 0}
		c := types.Point{X: -2, Y: 4}
		d := types.Point{X: 0, Y: 2}
		e := types.Point{X: 2, Y: 4}
		l1 := types.LineString{Points: []types.Point{a, b, c, d, e, a}}
		p := types.Polygon{Lines: []types.LineString{l1}}

		i := types.Point{X: -2, Y: 3}
		j := types.Point{X: 2, Y: 3}
		l := types.LineString{Points: []types.Point{i, j}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(p, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring within non-simple polygon", func(t *testing.T) {
		require := require.New(t)
		// looks like a bowtie
		a := types.Point{X: -2, Y: 2}
		b := types.Point{X: 2, Y: 2}
		c := types.Point{X: 2, Y: -2}
		d := types.Point{X: -2, Y: -2}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}

		w := types.Point{X: -1, Y: 0}
		x := types.Point{X: 0, Y: 1}
		y := types.Point{X: 1, Y: 0}
		z := types.Point{X: 0, Y: -1}

		wx := types.LineString{Points: []types.Point{w, x}}
		yz := types.LineString{Points: []types.Point{y, z}}
		wy := types.LineString{Points: []types.Point{w, y}}
		xz := types.LineString{Points: []types.Point{x, z}}

		f := NewWithin(expression.NewLiteral(wx, types.LineStringType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(yz, types.LineStringType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(wy, types.LineStringType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// Oddly, the LineString that is completely out of the Polygon is the one that is true
		f = NewWithin(expression.NewLiteral(xz, types.LineStringType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// LineString vs MultiPoint
	t.Run("linestring never within multipoint", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 4, Y: 4}
		b := types.Point{X: 4, Y: -4}
		c := types.Point{X: -4, Y: -4}
		d := types.Point{X: -4, Y: 4}
		l := types.LineString{Points: []types.Point{a, b, c, d}}
		mp := types.MultiPoint{Points: []types.Point{a, b, c, d}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(mp, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs MultiLineString
	t.Run("linestring within multilinestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 2, Y: 2}
		l := types.LineString{Points: []types.Point{a, b, c}}
		ml := types.MultiLineString{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// TODO: need to do that weird even odd thing...
	t.Run("linestring within split multilinestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 2, Y: 2}
		l1 := types.LineString{Points: []types.Point{a, b}}
		l2 := types.LineString{Points: []types.Point{b, c}}
		ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}
		l := types.LineString{Points: []types.Point{a, c}}
		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("terminal line points not ", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 2, Y: 2}
		ab := types.LineString{Points: []types.Point{a, b}}
		bc := types.LineString{Points: []types.Point{b, c}}
		ml := types.MultiLineString{Lines: []types.LineString{ab, bc}}

		aa := types.LineString{Points: []types.Point{a, a}}
		bb := types.LineString{Points: []types.Point{b, b}}
		cc := types.LineString{Points: []types.Point{c, c}}
		f := NewWithin(expression.NewLiteral(aa, types.LineStringType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
		f = NewWithin(expression.NewLiteral(bb, types.LineStringType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
		f = NewWithin(expression.NewLiteral(cc, types.LineStringType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs MultiPolygon
	t.Run("linestring within two separate touching polygons", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: -2, Y: 1}
		b := types.Point{X: 0, Y: 1}
		c := types.Point{X: 0, Y: -1}
		d := types.Point{X: -2, Y: -1}
		e := types.Point{X: 2, Y: 1}
		f := types.Point{X: 2, Y: -1}
		// these are two rectangles that share a side on the y axis
		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}
		p2 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{b, e, f, c, b}}}}
		mp := types.MultiPolygon{Polygons: []types.Polygon{p1, p2}}

		l := types.LineString{Points: []types.Point{{X: -1, Y: 0}, {X: 1, Y: 0}}}
		ff := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err := ff.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l = types.LineString{Points: []types.Point{{X: -3, Y: 0}, {X: 3, Y: 0}}}
		ff = NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err = ff.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("linestring within two separate not touching polygons", func(t *testing.T) {
		require := require.New(t)
		// triangle
		a := types.Point{X: -3, Y: 0}
		b := types.Point{X: -2, Y: 2}
		c := types.Point{X: -1, Y: 0}
		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, a}}}}

		// triangle
		d := types.Point{X: 1, Y: 0}
		e := types.Point{X: 2, Y: 2}
		f := types.Point{X: 3, Y: 0}
		p2 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{d, e, f, d}}}}

		mp := types.MultiPolygon{Polygons: []types.Polygon{p1, p2}}

		l := types.LineString{Points: []types.Point{{X: -2, Y: 1}, {X: 2, Y: 1}}}
		ff := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err := ff.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs GeometryCollection
	t.Run("linestring within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		l := types.LineString{Points: []types.Point{{}, {}}}
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("linestring within geometrycollection", func(t *testing.T) {
		require := require.New(t)
		l := types.LineString{Points: []types.Point{{}, {}}}
		gc := types.GeomColl{Geoms: []types.GeometryValue{l}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not within geometrycollection", func(t *testing.T) {
		require := require.New(t)
		l := types.LineString{Points: []types.Point{{}, {}}}
		l1 := types.LineString{Points: []types.Point{{X: 1, Y: 1}, {}}}
		gc := types.GeomColl{Geoms: []types.GeometryValue{l1}}

		f := NewWithin(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// LineString vs GeometryCollection

	// Polygon vs Point
	t.Run("polygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p, p, p}}
		poly := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, types.PolygonType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs LineString
	t.Run("polygon never within linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p, p, p}}
		poly := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, types.PolygonType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs Polygon
	t.Run("empty polygon within polygon", func(t *testing.T) {
		require := require.New(t)

		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {}, {}, {}}}}}

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p2 := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(p1, types.PolygonType{}), expression.NewLiteral(p2, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon within polygon touching border", func(t *testing.T) {
		require := require.New(t)
		// triangle inside polygon
		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, {}, a}}}}
		p2 := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(p1, types.PolygonType{}), expression.NewLiteral(p2, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("empty polygon on vertex not within polygon", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, a, a, a}}}}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p2 := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(p1, types.PolygonType{}), expression.NewLiteral(p2, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("empty polygon not within itself", func(t *testing.T) {
		require := require.New(t)
		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {}, {}, {}}}}}

		f := NewWithin(expression.NewLiteral(p, types.PolygonType{}), expression.NewLiteral(p, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("polygon not within overlapping polygon", func(t *testing.T) {
		require := require.New(t)
		// right triangles
		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 0}
		c := types.Point{X: -1, Y: 0}
		d := types.Point{X: 1, Y: 1}
		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, a}}}}
		p2 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{b, c, d, b}}}}

		f := NewWithin(expression.NewLiteral(p1, types.LineStringType{}), expression.NewLiteral(p2, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewWithin(expression.NewLiteral(p2, types.LineStringType{}), expression.NewLiteral(p1, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs MultiPoint
	t.Run("polygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p, p, p}}
		poly := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, types.PolygonType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs MultiLineString
	t.Run("polygon never within multilinestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p, p, p}}
		ml := types.MultiLineString{Lines: []types.LineString{l}}
		poly := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(poly, types.PolygonType{}), expression.NewLiteral(ml, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs MultiPolygon
	t.Run("polygon not within split touching multipolygon", func(t *testing.T) {
		require := require.New(t)
		a1 := types.Point{X: -1, Y: 1}
		b1 := types.Point{X: 1, Y: 1}
		c1 := types.Point{X: 1, Y: -1}
		d1 := types.Point{X: -1, Y: -1}
		l1 := types.LineString{Points: []types.Point{a1, b1, c1, d1, a1}}
		p1 := types.Polygon{Lines: []types.LineString{l1}}

		a2 := types.Point{X: -2, Y: 2}
		b2 := types.Point{X: 2, Y: 2}
		c2 := types.Point{X: 2, Y: -2}
		d2 := types.Point{X: -2, Y: -2}
		e2 := types.Point{X: 0, Y: 2}
		f2 := types.Point{X: 0, Y: -2}
		l2 := types.LineString{Points: []types.Point{a2, e2, f2, d2, a1}}
		p2 := types.Polygon{Lines: []types.LineString{l2}}
		l3 := types.LineString{Points: []types.Point{e2, b2, c2, f2, e2}}
		p3 := types.Polygon{Lines: []types.LineString{l3}}
		mp := types.MultiPolygon{Polygons: []types.Polygon{p2, p3}}

		f := NewWithin(expression.NewLiteral(p1, types.PolygonType{}), expression.NewLiteral(mp, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// Polygon vs GeometryCollection
	t.Run("polygon within empty geometry collection returns null", func(t *testing.T) {
		require := require.New(t)

		p := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {}, {}, {}}}}}
		g := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(p, types.PolygonType{}), expression.NewLiteral(g, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("empty polygon within geometry collection", func(t *testing.T) {
		require := require.New(t)

		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {}, {}, {}}}}}

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p2 := types.Polygon{Lines: []types.LineString{l}}
		g := types.GeomColl{Geoms: []types.GeometryValue{p2}}

		f := NewWithin(expression.NewLiteral(p1, types.PolygonType{}), expression.NewLiteral(g, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// MultiPoint vs Point
	t.Run("multipoint within point", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{}
		mp := types.MultiPoint{Points: []types.Point{p1, p1, p1}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(p1, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint not within point", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{}
		p2 := types.Point{X: 1, Y: 2}
		mp := types.MultiPoint{Points: []types.Point{p1, p2}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(p1, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPoint vs LineString
	t.Run("multipoint terminal points within empty linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{}
		mp := types.MultiPoint{Points: []types.Point{p, p}}
		l := types.LineString{Points: []types.Point{p, p}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint within linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{}
		b := types.Point{X: 2, Y: 2}
		p := types.Point{X: 1, Y: 1}
		mp := types.MultiPoint{Points: []types.Point{p}}
		ab := types.LineString{Points: []types.Point{a, b}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(ab, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint some within linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{}
		b := types.Point{X: 2, Y: 2}
		p := types.Point{X: 1, Y: 1}
		mp := types.MultiPoint{Points: []types.Point{a, p, b}}
		ab := types.LineString{Points: []types.Point{a, b}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(ab, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("multipoint terminal points not within linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 1, Y: 1}
		b := types.Point{X: 2, Y: 2}
		mp := types.MultiPoint{Points: []types.Point{a, b}}
		ab := types.LineString{Points: []types.Point{a, b}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(ab, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPoint vs Polygon
	t.Run("multipoint within polygon", func(t *testing.T) {
		require := require.New(t)
		mp := types.MultiPoint{Points: []types.Point{{}}}
		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint origin and vertexes within polygon with", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		mp := types.MultiPoint{Points: []types.Point{a, b, c, d, {}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint vertexes not within polygon", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		mp := types.MultiPoint{Points: []types.Point{a, b, c, d}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("multipoint points on interior, boundary, and exterior not within polygon", func(t *testing.T) {
		require := require.New(t)

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		mp := types.MultiPoint{Points: []types.Point{a, {}, types.Point{X: 100, Y: 100}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{a, b, c, d, a}}}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("multipoint terminal points not within empty polygon", func(t *testing.T) {
		require := require.New(t)
		mp := types.MultiPoint{Points: []types.Point{{}}}
		poly := types.Polygon{Lines: []types.LineString{{}, {}, {}, {}}}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(poly, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPoint vs MultiPoint

	// MultiPoint vs MultiLineString

	// MultiPoint vs MultiPolygon

	// MultiPoint vs GeometryCollection
	t.Run("multipoint within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		mp := types.MultiPoint{Points: []types.Point{{}, {}}}
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	// MultiLineString vs Point
	t.Run("multilinestring never within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p}}
		ml := types.MultiLineString{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(ml, types.LineStringType{}), expression.NewLiteral(p, types.PointType{}))
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
	t.Run("multilinestring within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		l := types.LineString{Points: []types.Point{{}, {}}}
		ml := types.MultiLineString{Lines: []types.LineString{l, l}}
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(ml, types.MultiLineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	// MultiPolygon vs Point
	t.Run("multipolygon never within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p}}
		poly := types.Polygon{Lines: []types.LineString{l}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly}}

		f := NewWithin(expression.NewLiteral(mpoly, types.MultiPolygonType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPolygon vs LineString
	t.Run("multipolygon never within linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{p, p}}
		poly := types.Polygon{Lines: []types.LineString{l}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly}}

		f := NewWithin(expression.NewLiteral(mpoly, types.MultiPolygonType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	// MultiPolygon vs Polygon

	// MultiPolygon vs MultiPoint

	// MultiPolygon vs MultiLineString

	// MultiPolygon vs MultiPolygon

	// MultiPolygon vs GeometryCollection
	t.Run("multipolygon within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		l := types.LineString{Points: []types.Point{{}, {}, {}, {}}}
		p := types.Polygon{Lines: []types.LineString{l}}
		mp := types.MultiPolygon{Polygons: []types.Polygon{p, p}}
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(mp, types.MultiPolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	// GeometryCollection vs Point
	t.Run("geometrycollection within point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{}
		gc := types.GeomColl{Geoms: []types.GeometryValue{p}}
		f := NewWithin(expression.NewLiteral(gc, types.GeomCollType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// GeometryCollection vs LineString
	t.Run("geometrycollection within linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: -5, Y: -5}
		d := types.Point{X: 5, Y: 5}
		ab := types.LineString{Points: []types.Point{a, b}}
		cd := types.LineString{Points: []types.Point{c, d}}
		gc := types.GeomColl{Geoms: []types.GeometryValue{cd}}
		f := NewWithin(expression.NewLiteral(ab, types.GeomCollType{}), expression.NewLiteral(gc, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// GeometryCollection vs Polygon
	t.Run("geometrycollection within polygon", func(t *testing.T) {
		require := require.New(t)

		p1 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {}, {}, {}}}}}
		gc := types.GeomColl{Geoms: []types.GeometryValue{p1}}

		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p2 := types.Polygon{Lines: []types.LineString{l}}

		f := NewWithin(expression.NewLiteral(gc, types.GeomCollType{}), expression.NewLiteral(p2, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	// GeometryCollection vs MultiPoint

	// GeometryCollection vs MultiLineString

	// GeometryCollection vs MultiPolygon

	// GeometryCollection vs GeometryCollection
	t.Run("empty geometry collection within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(gc, types.GeomCollType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("geometry collection within empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc1 := types.GeomColl{Geoms: []types.GeometryValue{types.Point{}}}
		gc2 := types.GeomColl{}

		f := NewWithin(expression.NewLiteral(gc1, types.GeomCollType{}), expression.NewLiteral(gc2, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
