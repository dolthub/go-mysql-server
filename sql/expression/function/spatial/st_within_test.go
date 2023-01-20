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

func TestWithinUnsupported(t *testing.T) {
	t.Run("linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.LineString{}, types.LineStringType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.Polygon{}, types.PolygonType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.MultiPoint{}, types.MultiPointType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.MultiLineString{}, types.MultiLineStringType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.MultiPolygon{}, types.MultiPolygonType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("geometry collection", func(t *testing.T) {
		require := require.New(t)
		f := NewWithin(expression.NewLiteral(types.GeomColl{Geoms: []types.GeometryValue{types.Point{}}}, types.GeomCollType{}), expression.NewLiteral(types.Point{}, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
