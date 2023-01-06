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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestDistance(t *testing.T) {
	t.Run("point distance from itself", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 0, Y: 0}
		f, err := NewDistance(expression.NewLiteral(p, sql.PointType{}), expression.NewLiteral(p, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0.0, v)
	})

	t.Run("simple point distance", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 100, Y: 200}
		p2 := sql.Point{X: 101, Y: 201}
		f, err := NewDistance(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(math.Sqrt(2), v)
	})

	t.Run("geomcollection vs multipoint", func(t *testing.T) {
		require := require.New(t)
		p0 := sql.Point{X: 0, Y: 0}
		p1 := sql.Point{X: 1, Y: 1}
		p2 := sql.Point{X: 2, Y: 2}
		l := sql.LineString{Points: []sql.Point{p0, p1, p2}}
		mp := sql.MultiPoint{Points: []sql.Point{p2, p1, p0}}
		gc := sql.GeomColl{Geoms: []sql.GeometryValue{p0, l}}
		f, err := NewDistance(expression.NewLiteral(gc, sql.GeomCollType{}), expression.NewLiteral(mp, sql.MultiPointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0.0, v)
	})

	t.Run("different SRIDs error", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{SRID: sql.CartesianSRID, X: 0, Y: 0}
		p2 := sql.Point{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}
		f, err := NewDistance(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("geospatial SRID unsupported", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}
		p2 := sql.Point{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}
		f, err := NewDistance(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("cartesian has no units", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{SRID: sql.CartesianSRID, X: 0, Y: 0}
		p2 := sql.Point{SRID: sql.CartesianSRID, X: 0, Y: 0}
		f, err := NewDistance(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}), expression.NewLiteral("meters", sql.LongText))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
