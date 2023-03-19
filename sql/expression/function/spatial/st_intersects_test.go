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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

var (
	triangle = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{X: -1, Y: 0}, {X: 0, Y: 2}, {X: 1, Y: 0}, {X: -1, Y: 0}}},
	}}
	square = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{X: -4, Y: 4}, {X: 4, Y: 4}, {X: 4, Y: -4}, {X: -4, Y: -4}, {X: -4, Y: 4}}},
	}}
	squareWithHole = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{X: -4, Y: 4}, {X: 4, Y: 4}, {X: 4, Y: -4}, {X: -4, Y: -4}, {X: -4, Y: 4}}},
		{Points: []types.Point{{X: -2, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: -2}, {X: -2, Y: -2}, {X: -2, Y: 2}}},
	}}
	diamond = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{X: 0, Y: 4}, {X: 4, Y: 0}, {X: 0, Y: -4}, {X: -4, Y: 0}, {X: 0, Y: 4}}},
	}}
	diamondWithHole = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{X: 0, Y: 4}, {X: 4, Y: 0}, {X: 0, Y: -4}, {X: -4, Y: 0}, {X: 0, Y: 4}}},
		{Points: []types.Point{{X: 0, Y: 2}, {X: 2, Y: 0}, {X: 0, Y: -2}, {X: -2, Y: 0}, {X: 0, Y: 2}}},
	}}

	emptyLineString = types.LineString{Points: []types.Point{{}, {}}}
	emptyPolygon    = types.Polygon{Lines: []types.LineString{
		{Points: []types.Point{{}, {}, {}, {}}},
	}}
	emptyMultiPoint      = types.MultiPoint{Points: []types.Point{{}}}
	emptyMultiLineString = types.MultiLineString{Lines: []types.LineString{emptyLineString}}
	emptyMultiPolygon    = types.MultiPolygon{Polygons: []types.Polygon{emptyPolygon}}

	simpleLineString      = types.LineString{Points: []types.Point{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: 3, Y: 3}}}
	simpleMultiPoint      = types.MultiPoint{Points: []types.Point{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: 3, Y: 3}}}
	simpleMultiLineString = types.MultiLineString{Lines: []types.LineString{simpleLineString}}
	simpleMultiPolygon    = types.MultiPolygon{Polygons: []types.Polygon{square}}
	simpleGeomColl        = types.GeomColl{Geoms: []types.GeometryValue{types.Point{}}}
)

func TestPointIntersectsPoint(t *testing.T) {
	t.Run("point intersects point", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 1, Y: 2}
		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects point", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 1, Y: 2}
		p2 := types.Point{X: 123, Y: 456}
		f := NewIntersects(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(p2, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsLineString(t *testing.T) {
	t.Run("points intersects linestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleLineString.Points[0], types.PointType{}), expression.NewLiteral(simpleLineString, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleLineString.Points[1], types.PointType{}), expression.NewLiteral(simpleLineString, types.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleLineString.Points[2], types.PointType{}), expression.NewLiteral(simpleLineString, types.LineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects empty linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(emptyLineString, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects linestring", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 100, Y: 200}
		l := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsPolygon(t *testing.T) {
	t.Run("point intersects polygon", func(t *testing.T) {
		require := require.New(t)

		// vertexes intersect
		f := NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[0], types.PointType{}), expression.NewLiteral(triangle, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[1], types.PointType{}), expression.NewLiteral(triangle, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[2], types.PointType{}), expression.NewLiteral(triangle, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// border intersect
		f = NewIntersects(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(triangle, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// interior intersect
		q := types.Point{X: 0, Y: 1}
		f = NewIntersects(expression.NewLiteral(q, types.PointType{}), expression.NewLiteral(triangle, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects empty polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(emptyPolygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects polygon with hole", func(t *testing.T) {
		require := require.New(t)

		p1 := types.Point{X: -3, Y: 2}
		f := NewIntersects(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p2 := types.Point{X: -3, Y: 0}
		f = NewIntersects(expression.NewLiteral(p2, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p3 := types.Point{X: -3, Y: -2}
		f = NewIntersects(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiPoint(t *testing.T) {
	t.Run("points intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[0], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[1], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[2], types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		p := types.Point{X: 0, Y: 0}
		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiLineString(t *testing.T) {
	p1 := types.Point{X: -1, Y: -1}
	p2 := types.Point{X: 1, Y: 1}
	p3 := types.Point{X: 123, Y: 456}
	l1 := types.LineString{Points: []types.Point{p1, p2}}
	l2 := types.LineString{Points: []types.Point{p3, p3}}
	ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}
	t.Run("points intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(p3, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p := types.Point{X: 0, Y: 0}
		f = NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		p4 := types.Point{X: -100, Y: -123123}
		f := NewIntersects(expression.NewLiteral(p4, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		p5 := types.Point{X: 100, Y: 1001}
		f = NewIntersects(expression.NewLiteral(p5, types.PointType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{square}}
	t.Run("point intersects multipolygon", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(types.Point{}, types.PointType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 100, Y: 100}
		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsGeometryCollection(t *testing.T) {
	p := types.Point{}

	t.Run("point intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}

		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("point intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{p}}

		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 1, Y: 0}
		gc := types.GeomColl{Geoms: []types.GeometryValue{a}}

		f := NewIntersects(expression.NewLiteral(p, types.PointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsLineString(t *testing.T) {
	t.Run("linestring intersects linestring", func(t *testing.T) {
		require := require.New(t)
		a := types.Point{X: 0, Y: 0}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 0, Y: 1}
		d := types.Point{X: 1, Y: 0}
		ab := types.LineString{Points: []types.Point{a, b}}
		cd := types.LineString{Points: []types.Point{c, d}}

		f := NewIntersects(expression.NewLiteral(ab, types.LineStringType{}), expression.NewLiteral(cd, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("empty linestring intersects empty linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(emptyLineString, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects linestring", func(t *testing.T) {
		require := require.New(t)
		l1 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		l2 := types.LineString{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}
		f := NewIntersects(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsPolygon(t *testing.T) {
	t.Run("linestring intersects polygon", func(t *testing.T) {
		require := require.New(t)

		// border intersect
		f := NewIntersects(expression.NewLiteral(squareWithHole.Lines[0], types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(squareWithHole.Lines[1], types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// interior intersect
		l1 := types.LineString{Points: []types.Point{{X: -3, Y: 3}, {X: 3, Y: 3}}}
		f = NewIntersects(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l2 := types.LineString{Points: []types.Point{{X: -3, Y: -3}, {X: 3, Y: 3}}}
		f = NewIntersects(expression.NewLiteral(l2, types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l3 := types.LineString{Points: []types.Point{{X: -5}, {X: 5}}}
		f = NewIntersects(expression.NewLiteral(l3, types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring does not intersect polygon", func(t *testing.T) {
		require := require.New(t)

		// in hole
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// completely outside
		a := types.Point{X: 100, Y: 100}
		b := types.Point{X: 200, Y: 200}
		l := types.LineString{Points: []types.Point{a, b}}
		f = NewIntersects(expression.NewLiteral(l, types.LineStringType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("empty linestring intersects empty polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(emptyPolygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})
}

func TestLineStringIntersectsMultiPoint(t *testing.T) {
	t.Run("linestring intersects multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(simpleLineString, types.LineStringType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsMultiLineString(t *testing.T) {
	t.Run("linestring intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleLineString, types.LineStringType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		a := types.Point{X: 1.5, Y: 10}
		b := types.Point{X: 1.5, Y: -10}
		ab := types.LineString{Points: []types.Point{a, b}}
		f = NewIntersects(expression.NewLiteral(ab, types.LineStringType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
	t.Run("linestring intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(squareWithHole.Lines[0], types.LineStringType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(mp, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsGeometryCollection(t *testing.T) {
	t.Run("linestring intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("linestring intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{emptyLineString}}
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{simpleLineString}}
		f := NewIntersects(expression.NewLiteral(emptyLineString, types.LineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPolygonIntersectsPolygon(t *testing.T) {
	t.Run("polygon intersects polygon", func(t *testing.T) {
		require := require.New(t)

		smallSquare := types.Polygon{Lines: []types.LineString{squareWithHole.Lines[1]}}

		f := NewIntersects(expression.NewLiteral(smallSquare, types.PolygonType{}), expression.NewLiteral(square, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(square, types.PolygonType{}), expression.NewLiteral(smallSquare, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon does not intersect polygon", func(t *testing.T) {
		require := require.New(t)

		// in hole
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(squareWithHole, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// completely outside
		a := types.Point{X: 100, Y: 100}
		b := types.Point{X: 100, Y: 200}
		c := types.Point{X: 200, Y: 200}
		d := types.Point{X: 200, Y: 100}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}
		f = NewIntersects(expression.NewLiteral(p, types.PolygonType{}), expression.NewLiteral(square, types.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("empty polygon intersects empty polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(emptyPolygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})
}

func TestPolygonIntersectsMultiPoint(t *testing.T) {
	t.Run("polygon intersects multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(square, types.PolygonType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		mp := types.MultiPoint{Points: []types.Point{{}}}
		f = NewIntersects(expression.NewLiteral(squareWithHole, types.PolygonType{}), expression.NewLiteral(mp, types.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPolygonIntersectsMultiLineString(t *testing.T) {
	t.Run("polygon intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(square, types.PolygonType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		ml := types.MultiLineString{Lines: []types.LineString{emptyLineString}}
		f = NewIntersects(expression.NewLiteral(squareWithHole, types.PolygonType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPolygonIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
	t.Run("polygon intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(squareWithHole, types.PolygonType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPolygonIntersectsGeometryCollection(t *testing.T) {
	t.Run("polygon intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("polygon intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{emptyPolygon}}
		f := NewIntersects(expression.NewLiteral(square, types.PolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("polygon not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{squareWithHole}}
		f := NewIntersects(expression.NewLiteral(emptyPolygon, types.PolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPointIntersectsMultiPoint(t *testing.T) {
	t.Run("multipoint intersects multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPointIntersectsMultiLineString(t *testing.T) {
	t.Run("multipoint intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("c not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPointIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
	t.Run("multipoint intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(simpleMultiPoint, types.MultiPointType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPointIntersectsGeometryCollection(t *testing.T) {
	t.Run("multipoint intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("multipoint intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{emptyMultiPoint}}
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipoint not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{simpleMultiPoint}}
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiPointType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiLineStringIntersectsMultiLineString(t *testing.T) {
	t.Run("multilinestring intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleLineString, types.MultiLineStringType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multilinestring not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiLineStringType{}), expression.NewLiteral(simpleMultiLineString, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiLineStringIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
	t.Run("multilinestring intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(simpleMultiPoint, types.MultiLineStringType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multilinestring not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyMultiPoint, types.MultiLineStringType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiLineStringIntersectsGeometryCollection(t *testing.T) {
	t.Run("multilinestring intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyMultiLineString, types.MultiLineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("multilinestring intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{emptyMultiLineString}}
		f := NewIntersects(expression.NewLiteral(emptyMultiLineString, types.MultiLineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multilinestring not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{simpleLineString}}
		f := NewIntersects(expression.NewLiteral(emptyMultiLineString, types.MultiLineStringType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPolygonIntersectsMultiPolygon(t *testing.T) {
	mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
	t.Run("multipolygon intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(mp, types.MultiPolygonType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipolygon not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyMultiPolygon, types.MultiPolygonType{}), expression.NewLiteral(mp, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestMultiPolygonIntersectsGeometryCollection(t *testing.T) {
	t.Run("multipolygon intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyMultiPolygon, types.MultiPolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("multipolygon intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{Geoms: []types.GeometryValue{emptyMultiPolygon}}
		f := NewIntersects(expression.NewLiteral(emptyMultiPolygon, types.MultiPolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multipolygon not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		mp := types.MultiPolygon{Polygons: []types.Polygon{squareWithHole}}
		gc := types.GeomColl{Geoms: []types.GeometryValue{mp}}
		f := NewIntersects(expression.NewLiteral(emptyMultiPolygon, types.MultiPolygonType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestGeometryCollectionIntersectsGeometryCollection(t *testing.T) {
	t.Run("empty geometrycollection intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := types.GeomColl{}
		f := NewIntersects(expression.NewLiteral(gc, types.GeomCollType{}), expression.NewLiteral(gc, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("geometrycollection intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)

		gc1 := types.GeomColl{Geoms: []types.GeometryValue{types.Point{}}}
		f := NewIntersects(expression.NewLiteral(gc1, types.GeomCollType{}), expression.NewLiteral(gc1, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		gc2 := types.GeomColl{Geoms: []types.GeometryValue{square}}
		f = NewIntersects(expression.NewLiteral(gc1, types.GeomCollType{}), expression.NewLiteral(gc2, types.GeomCollType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(gc2, types.GeomCollType{}), expression.NewLiteral(gc1, types.GeomCollType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("geometrycollection not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc1 := types.GeomColl{Geoms: []types.GeometryValue{squareWithHole}}
		gc2 := types.GeomColl{Geoms: []types.GeometryValue{emptyLineString}}
		f := NewIntersects(expression.NewLiteral(gc1, types.GeomCollType{}), expression.NewLiteral(gc2, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		f = NewIntersects(expression.NewLiteral(gc1, types.GeomCollType{}), expression.NewLiteral(gc2, types.GeomCollType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}
