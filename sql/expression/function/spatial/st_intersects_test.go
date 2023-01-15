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

var (
	emptyLineString = sql.LineString{Points: []sql.Point{{}, {}}}
	simpleLineString = sql.LineString{Points: []sql.Point{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: 3, Y: 3}}}
	simpleMultiPoint = sql.MultiPoint{Points: []sql.Point{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: 3, Y: 3}}}
	simpleMultiLineString = sql.MultiLineString{Lines: []sql.LineString{simpleLineString}}
	emptyPolygon = sql.Polygon{Lines: []sql.LineString{
		{Points: []sql.Point{{}, {}, {}, {}}},
	}}
	square = sql.Polygon{Lines: []sql.LineString{
		{Points: []sql.Point{{X: -4, Y: 4},{X: 4, Y: 4},{X: 4, Y: -4},{X: -4, Y: -4},{X: -4, Y: 4}}},
	}}
	squareWithHole = sql.Polygon{Lines: []sql.LineString{
		{Points: []sql.Point{{X: -4, Y: 4},{X: 4, Y: 4},{X: 4, Y: -4},{X: -4, Y: -4},{X: -4, Y: 4}}},
		{Points: []sql.Point{{X: -2, Y: 2},{X: 2, Y: 2},{X: 2, Y: -2},{X: -2, Y: -2},{X: -2, Y: 2}}},
	}}
	triangle = sql.Polygon{Lines: []sql.LineString{
		{Points: []sql.Point{{X: -1, Y: 0},{X: 0, Y: 2},{X: 1, Y: 0},{X: -1, Y: 0}}},
	}}
)

func TestPointIntersectsPoint(t *testing.T) {
	t.Run("point intersects point", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 1, Y: 2}
		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects point", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 1, Y: 2}
		p2 := sql.Point{X: 123, Y: 456}
		f := NewIntersects(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsLineString(t *testing.T) {
	t.Run("points intersects linestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleLineString.Points[0], sql.PointType{}), expression.NewLiteral(simpleLineString, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleLineString.Points[1], sql.PointType{}), expression.NewLiteral(simpleLineString, sql.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleLineString.Points[2], sql.PointType{}), expression.NewLiteral(simpleLineString, sql.PointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects empty linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(sql.Point{}, sql.PointType{}), expression.NewLiteral(emptyLineString, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects linestring", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 100, Y: 200}
		l := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(l, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsPolygon(t *testing.T) {
	t.Run("point intersects polygon", func(t *testing.T) {
		require := require.New(t)

		// vertexes intersect
		f := NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[0], sql.PointType{}), expression.NewLiteral(triangle, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[1], sql.PointType{}), expression.NewLiteral(triangle, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(triangle.Lines[0].Points[2], sql.PointType{}), expression.NewLiteral(triangle, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// border intersect
		f = NewIntersects(expression.NewLiteral(sql.Point{}, sql.PointType{}), expression.NewLiteral(triangle, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// interior intersect
		q := sql.Point{X: 0, Y: 1}
		f = NewIntersects(expression.NewLiteral(q, sql.PointType{}), expression.NewLiteral(triangle, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects empty polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(sql.Point{}, sql.PointType{}), expression.NewLiteral(emptyPolygon, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point intersects polygon with hole", func(t *testing.T) {
		require := require.New(t)

		p1 := sql.Point{X: -3, Y: 2}
		f := NewIntersects(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p2 := sql.Point{X: -3, Y: 0}
		f = NewIntersects(expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p3 := sql.Point{X: -3, Y: -2}
		f = NewIntersects(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(sql.Point{}, sql.PointType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiPoint(t *testing.T) {
	t.Run("points intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[0], sql.PointType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[1], sql.PointType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(simpleMultiPoint.Points[2], sql.PointType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		p := sql.Point{X: 0, Y: 0}
		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiLineString(t *testing.T) {
	p1 := sql.Point{X: -1, Y: -1}
	p2 := sql.Point{X: 1, Y: 1}
	p3 := sql.Point{X: 123, Y: 456}
	l1 := sql.LineString{Points: []sql.Point{p1, p2}}
	l2 := sql.LineString{Points: []sql.Point{p3, p3}}
	ml := sql.MultiLineString{Lines: []sql.LineString{l1, l2}}
	t.Run("points intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(p3, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		p := sql.Point{X: 0, Y: 0}
		f = NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		p4 := sql.Point{X: -100, Y: -123123}
		f := NewIntersects(expression.NewLiteral(p4, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		p5 := sql.Point{X: 100, Y: 1001}
		f = NewIntersects(expression.NewLiteral(p5, sql.PointType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsMultiPolygon(t *testing.T) {
	mp := sql.MultiPolygon{Polygons: []sql.Polygon{square}}
	t.Run("point intersects multipolygon", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(sql.Point{}, sql.PointType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("points not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 100, Y: 100}
		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestPointIntersectsGeometryCollection(t *testing.T) {
	p := sql.Point{}

	t.Run("point intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := sql.GeomColl{}

		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("point intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{p}}

		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 1, Y: 0}
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{a}}

		f := NewIntersects(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsLineString(t *testing.T) {
	t.Run("linestring intersects linestring", func(t *testing.T) {
		require := require.New(t)
		a := sql.Point{X: 0, Y: 0}
		b := sql.Point{X: 1, Y: 1}
		c := sql.Point{X: 0, Y: 1}
		d := sql.Point{X: 1, Y: 0}
		ab := sql.LineString{Points: []sql.Point{a, b}}
		cd := sql.LineString{Points: []sql.Point{c, d}}

		f := NewIntersects(expression.NewLiteral(ab, sql.LineStringType{}), expression.NewLiteral(cd, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("empty linestring intersects empty linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(emptyLineString, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects linestring", func(t *testing.T) {
		require := require.New(t)
		l1 := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 2, Y: 2}}}
		l2 := sql.LineString{Points: []sql.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}
		f := NewIntersects(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsPolygon(t *testing.T) {
	t.Run("linestring intersects polygon", func(t *testing.T) {
		require := require.New(t)

		// border intersect
		f := NewIntersects(expression.NewLiteral(squareWithHole.Lines[0], sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		f = NewIntersects(expression.NewLiteral(squareWithHole.Lines[1], sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		// interior intersect
		l1 := sql.LineString{Points: []sql.Point{{X: -3, Y: 3}, {X: 3, Y: 3}}}
		f = NewIntersects(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l2 := sql.LineString{Points: []sql.Point{{X: -3, Y: -3}, {X: 3, Y: 3}}}
		f = NewIntersects(expression.NewLiteral(l2, sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		l3 := sql.LineString{Points: []sql.Point{{X: -5}, {X: 5}}}
		f = NewIntersects(expression.NewLiteral(l3, sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring does not intersect polygon", func(t *testing.T) {
		require := require.New(t)

		// in hole
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)

		// completely outside
		a := sql.Point{X: 100, Y: 100}
		b := sql.Point{X: 200, Y: 200}
		l := sql.LineString{Points: []sql.Point{a, b}}
		f = NewIntersects(expression.NewLiteral(l, sql.LineStringType{}), expression.NewLiteral(squareWithHole, sql.PolygonType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("empty linestring intersects empty polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(emptyPolygon, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})
}

func TestLineStringIntersectsMultiPoint(t *testing.T) {
	t.Run("linestring intersects multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(simpleLineString, sql.LineStringType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multipoint", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(simpleMultiPoint, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsMultiLineString(t *testing.T) {
	t.Run("linestring intersects multilinestring", func(t *testing.T) {
		require := require.New(t)

		f := NewIntersects(expression.NewLiteral(simpleLineString, sql.LineStringType{}), expression.NewLiteral(simpleMultiLineString, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)

		a := sql.Point{X: 1.5, Y: 10}
		b := sql.Point{X: 1.5, Y: -10}
		ab := sql.LineString{Points:  []sql.Point{a, b}}
		f = NewIntersects(expression.NewLiteral(ab, sql.LineStringType{}), expression.NewLiteral(simpleMultiLineString, sql.MultiLineStringType{}))
		v, err = f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(simpleMultiLineString, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsMultiPolygon(t *testing.T) {
	mp := sql.MultiPolygon{Polygons: []sql.Polygon{squareWithHole}}
	t.Run("linestring intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(squareWithHole.Lines[0], sql.LineStringType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects multipolygon", func(t *testing.T) {
		require := require.New(t)
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.LineStringType{}), expression.NewLiteral(mp, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}

func TestLineStringIntersectsGeometryCollection(t *testing.T) {
	t.Run("linestring intersects empty geometrycollection returns null", func(t *testing.T) {
		require := require.New(t)
		gc := sql.GeomColl{}
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("point intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{emptyLineString}}
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring not intersects geometrycollection", func(t *testing.T) {
		require := require.New(t)
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{simpleLineString}}
		f := NewIntersects(expression.NewLiteral(emptyLineString, sql.PointType{}), expression.NewLiteral(gc, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}