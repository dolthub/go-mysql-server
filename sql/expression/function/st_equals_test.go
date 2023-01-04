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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestSTEquals(t *testing.T) {
	t.Run("point vs point equals", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 123, Y: 456}
		p2 := sql.Point{X: 123, Y: 456}
		f := NewSTEquals(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(1), v)
	})

	t.Run("point vs point not equals", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 123, Y: 456}
		p2 := sql.Point{X: 789, Y: 321}
		f := NewSTEquals(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(0), v)
	})

	t.Run("linestring vs linestring equals", func(t *testing.T) {
		require := require.New(t)
		l1 := sql.LineString{Points: []sql.Point{{X: 12, Y: 34},{X: 56, Y: 78},{X: 56, Y: 78}}}
		l2 := sql.LineString{Points: []sql.Point{{X: 56, Y: 78}, {X: 12, Y: 34},{X: 12, Y: 34}}}
		f := NewSTEquals(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(1), v)
	})

	t.Run("linestring vs linestring not equals", func(t *testing.T) {
		require := require.New(t)
		l1 := sql.LineString{Points: []sql.Point{{X: 12, Y: 34},{X: 56, Y: 78}}}
		l2 := sql.LineString{Points: []sql.Point{{X: 56, Y: 78}, {X: 12, Y: 34}, {X: 123, Y: 349}}}
		f := NewSTEquals(expression.NewLiteral(l1, sql.LineStringType{}), expression.NewLiteral(l2, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(0), v)
	})

	t.Run("polygon vs multilinestring not equal", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 0, Y: 0}
		p2 := sql.Point{X: 0, Y: 1}
		p3 := sql.Point{X: 1, Y: 0}
		p4 := sql.Point{X: 1, Y: 1}
		l1 := sql.LineString{Points: []sql.Point{p1,p2,p3,p4}}
		p := sql.Polygon{Lines: []sql.LineString{l1}}
		ml := sql.MultiLineString{Lines: []sql.LineString{l1}}
		f := NewSTEquals(expression.NewLiteral(p, sql.PolygonType{}), expression.NewLiteral(ml, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(0), v)
	})

	t.Run("empty geometry vs empty geometry equal", func (t *testing.T) {
		require := require.New(t)
		g1 := sql.GeomColl{}
		g2 := sql.GeomColl{}
		f := NewSTEquals(expression.NewLiteral(g1, sql.GeomCollType{}), expression.NewLiteral(g2, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(1), v)
	})

	t.Run("point geometry vs linestring geometry not equal", func (t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{X: 0, Y: 0}
		p2 := sql.Point{X: 0, Y: 1}
		l1 := sql.LineString{Points: []sql.Point{p1,p2}}
		g1 := sql.GeomColl{Geoms: []sql.GeometryValue{p1, p2}}
		g2 := sql.GeomColl{Geoms: []sql.GeometryValue{l1}}
		f := NewSTEquals(expression.NewLiteral(g1, sql.GeomCollType{}), expression.NewLiteral(g2, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(int8(0), v)
	})

	t.Run("null vs point is null", func(t *testing.T) {
		require := require.New(t)
		p := sql.Point{X: 789, Y: 321}
		f := NewSTEquals(expression.NewLiteral(nil, sql.Null), expression.NewLiteral(p, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("different SRIDs error", func(t *testing.T) {
		require := require.New(t)
		p1 := sql.Point{SRID: 0, X: 123, Y: 456}
		p2 := sql.Point{SRID: 4326, X: 123, Y: 456}
		f := NewSTEquals(expression.NewLiteral(p1, sql.PointType{}), expression.NewLiteral(p2, sql.PointType{}))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

}
