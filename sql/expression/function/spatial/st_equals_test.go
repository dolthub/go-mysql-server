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

func TestSTEquals(t *testing.T) {
	t.Run("point vs point equals", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 123, Y: 456}
		p2 := types.Point{X: 123, Y: 456}
		f := NewSTEquals(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(p2, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point vs point not equals", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 123, Y: 456}
		p2 := types.Point{X: 789, Y: 321}
		f := NewSTEquals(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(p2, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("null vs point is null", func(t *testing.T) {
		require := require.New(t)
		p := types.Point{X: 789, Y: 321}
		f := NewSTEquals(expression.NewLiteral(nil, types.Null), expression.NewLiteral(p, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("different SRIDs error", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{SRID: 0, X: 123, Y: 456}
		p2 := types.Point{SRID: 4326, X: 123, Y: 456}
		f := NewSTEquals(expression.NewLiteral(p1, types.PointType{}), expression.NewLiteral(p2, types.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestSTEqualsSkipped(t *testing.T) {
	t.Skip("comparisons that aren't point vs point are unsupported")

	t.Run("linestring vs linestring equals", func(t *testing.T) {
		require := require.New(t)
		l1 := types.LineString{Points: []types.Point{{X: 12, Y: 34}, {X: 56, Y: 78}, {X: 56, Y: 78}}}
		l2 := types.LineString{Points: []types.Point{{X: 56, Y: 78}, {X: 12, Y: 34}, {X: 12, Y: 34}}}
		f := NewSTEquals(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("linestring vs linestring not equals", func(t *testing.T) {
		require := require.New(t)
		l1 := types.LineString{Points: []types.Point{{X: 12, Y: 34}, {X: 56, Y: 78}}}
		l2 := types.LineString{Points: []types.Point{{X: 56, Y: 78}, {X: 12, Y: 34}, {X: 123, Y: 349}}}
		f := NewSTEquals(expression.NewLiteral(l1, types.LineStringType{}), expression.NewLiteral(l2, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("polygon vs multilinestring not equal", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 0, Y: 0}
		p2 := types.Point{X: 0, Y: 1}
		p3 := types.Point{X: 1, Y: 0}
		p4 := types.Point{X: 1, Y: 1}
		l1 := types.LineString{Points: []types.Point{p1, p2, p3, p4}}
		p := types.Polygon{Lines: []types.LineString{l1}}
		ml := types.MultiLineString{Lines: []types.LineString{l1}}
		f := NewSTEquals(expression.NewLiteral(p, types.PolygonType{}), expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("empty geometry vs empty geometry equal", func(t *testing.T) {
		require := require.New(t)
		g1 := types.GeomColl{}
		g2 := types.GeomColl{}
		f := NewSTEquals(expression.NewLiteral(g1, types.GeomCollType{}), expression.NewLiteral(g2, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("point geometry vs linestring geometry not equal", func(t *testing.T) {
		require := require.New(t)
		p1 := types.Point{X: 0, Y: 0}
		p2 := types.Point{X: 0, Y: 1}
		l1 := types.LineString{Points: []types.Point{p1, p2}}
		g1 := types.GeomColl{Geoms: []types.GeometryValue{p1, p2}}
		g2 := types.GeomColl{Geoms: []types.GeometryValue{l1}}
		f := NewSTEquals(expression.NewLiteral(g1, types.GeomCollType{}), expression.NewLiteral(g2, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})
}
