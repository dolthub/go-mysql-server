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

package spatial

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGeomColl(t *testing.T) {
	t.Run("create valid empty geometrycollection", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomColl()
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.GeomColl{Geoms: []types.GeometryValue{}}, v)
	})

	t.Run("create valid geometrycollection with every geometry", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{Points: []types.Point{point, point}}
		mline := types.MultiLineString{Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{}

		f, err := NewGeomColl(
			expression.NewLiteral(point, types.PointType{}),
			expression.NewLiteral(line, types.LineStringType{}),
			expression.NewLiteral(poly, types.PolygonType{}),
			expression.NewLiteral(mpoint, types.MultiPointType{}),
			expression.NewLiteral(mline, types.MultiLineStringType{}),
			expression.NewLiteral(mpoly, types.MultiPolygonType{}),
			expression.NewLiteral(gColl, types.GeomCollType{}),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.GeomColl{Geoms: []types.GeometryValue{
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
	_, err := NewGeomColl(expression.NewLiteral(nil, types.GeometryType{}))
	require.NoError(err)
}
