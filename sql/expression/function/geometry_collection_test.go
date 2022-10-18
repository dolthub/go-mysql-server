// Copyright 2020-2022 Dolthub, Inc.
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

func TestGeomColl(t *testing.T) {
	t.Run("create valid empty geometrycollection", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomColl()
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.GeomColl{Geoms: []sql.GeometryValue{}}, v)
	})

	t.Run("create valid geometrycollection with every geometry", func(t *testing.T) {
		require := require.New(t)
		point := sql.Point{X: 1, Y: 2}
		line := sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := sql.MultiPoint{Points: []sql.Point{point, point}}
		mline := sql.MultiLineString{Lines: []sql.LineString{line, line}}
		mpoly := sql.MultiPolygon{Polygons: []sql.Polygon{poly, poly}}
		gColl := sql.GeomColl{}

		f, err := NewGeomColl(
			expression.NewLiteral(point, sql.PointType{}),
			expression.NewLiteral(line, sql.LineStringType{}),
			expression.NewLiteral(poly, sql.PolygonType{}),
			expression.NewLiteral(mpoint, sql.MultiPointType{}),
			expression.NewLiteral(mline, sql.MultiLineStringType{}),
			expression.NewLiteral(mpoly, sql.MultiPolygonType{}),
			expression.NewLiteral(gColl, sql.GeomCollType{}),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.GeomColl{Geoms: []sql.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}, v)
	})
}

func TestNewGeomColl(t *testing.T) {
	require := require.New(t)
	_, err := NewGeomColl(expression.NewLiteral(nil, sql.GeometryType{}))
	require.NoError(err)
}
