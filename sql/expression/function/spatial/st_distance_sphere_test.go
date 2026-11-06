// Copyright 2026 Dolthub, Inc.
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

func evalDistanceSphere(t *testing.T, args ...sql.Expression) (interface{}, error) {
	ctx := sql.NewEmptyContext()
	f, err := NewDistanceSphere(ctx, args...)
	require.NoError(t, err)
	return f.Eval(ctx, nil)
}

func pointLit(p types.Point) sql.Expression {
	return expression.NewLiteral(p, types.PointType{})
}

func TestDistanceSphere(t *testing.T) {
	t.Run("matches mysql for two points", func(t *testing.T) {
		v, err := evalDistanceSphere(t, pointLit(types.Point{X: 13.18, Y: 32.88}), pointLit(types.Point{X: 13.20, Y: 32.90}))
		require.NoError(t, err)
		require.InDelta(t, 2903.966318769266, v, 1e-6)
	})

	t.Run("same point is zero", func(t *testing.T) {
		p := types.Point{X: 13.18, Y: 32.88}
		v, err := evalDistanceSphere(t, pointLit(p), pointLit(p))
		require.NoError(t, err)
		require.Equal(t, 0.0, v)
	})

	t.Run("geographic srid uses the same coordinates", func(t *testing.T) {
		v, err := evalDistanceSphere(t,
			pointLit(types.Point{SRID: types.GeoSpatialSRID, X: 13.18, Y: 32.88}),
			pointLit(types.Point{SRID: types.GeoSpatialSRID, X: 13.20, Y: 32.90}))
		require.NoError(t, err)
		require.InDelta(t, 2903.966318769266, v, 1e-6)
	})

	t.Run("custom radius scales the distance", func(t *testing.T) {
		v, err := evalDistanceSphere(t,
			pointLit(types.Point{X: 0, Y: 0}), pointLit(types.Point{X: 0, Y: 90}),
			expression.NewLiteral(2.0, types.Float64))
		require.NoError(t, err)
		require.InDelta(t, 3.141592653589793, v, 1e-9)
	})

	t.Run("multipoint takes the closest pair", func(t *testing.T) {
		mp := types.MultiPoint{Points: []types.Point{{X: 50, Y: 50}, {X: 13.20, Y: 32.90}}}
		v, err := evalDistanceSphere(t, pointLit(types.Point{X: 13.18, Y: 32.88}), expression.NewLiteral(mp, types.MultiPointType{}))
		require.NoError(t, err)
		require.InDelta(t, 2903.966318769266, v, 1e-6)
	})

	t.Run("null argument is null", func(t *testing.T) {
		v, err := evalDistanceSphere(t, pointLit(types.Point{}), expression.NewLiteral(nil, types.Null))
		require.NoError(t, err)
		require.Nil(t, v)
	})

	t.Run("latitude out of range errors", func(t *testing.T) {
		_, err := evalDistanceSphere(t, pointLit(types.Point{X: 0, Y: 91}), pointLit(types.Point{}))
		require.True(t, ErrLatitudeOutOfRange.Is(err))
	})

	t.Run("longitude out of range errors", func(t *testing.T) {
		_, err := evalDistanceSphere(t, pointLit(types.Point{X: 181, Y: 0}), pointLit(types.Point{}))
		require.True(t, ErrLongitudeOutOfRange.Is(err))
	})

	t.Run("non-positive radius errors", func(t *testing.T) {
		_, err := evalDistanceSphere(t, pointLit(types.Point{}), pointLit(types.Point{}), expression.NewLiteral(0.0, types.Float64))
		require.True(t, ErrNonPositiveRadius.Is(err))
	})

	t.Run("polygon errors", func(t *testing.T) {
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{}, {X: 1}, {X: 1, Y: 1}, {}}}}}
		_, err := evalDistanceSphere(t, pointLit(types.Point{}), expression.NewLiteral(poly, types.PolygonType{}))
		require.True(t, ErrDistanceSphereType.Is(err))
	})

	t.Run("different srids error", func(t *testing.T) {
		_, err := evalDistanceSphere(t, pointLit(types.Point{}), pointLit(types.Point{SRID: types.GeoSpatialSRID}))
		require.Error(t, err)
	})
}
