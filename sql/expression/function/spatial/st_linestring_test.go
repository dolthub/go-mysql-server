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

func TestStartPoint(t *testing.T) {
	t.Run("simple case", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 123, Y: 456}
		e := types.Point{X: 456, Y: 789}
		l := types.LineString{Points: []types.Point{s, e}}
		f := NewStartPoint(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(s, v)
	})

	t.Run("simple case with SRID", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{SRID: types.GeoSpatialSRID, X: 123, Y: 456}
		e := types.Point{SRID: types.GeoSpatialSRID, X: 456, Y: 789}
		l := types.LineString{Points: []types.Point{s, e}}
		f := NewStartPoint(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(s, v)
	})

	t.Run("null argument", func(t *testing.T) {
		require := require.New(t)
		f := NewStartPoint(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-linestring argument", func(t *testing.T) {
		require := require.New(t)
		f := NewStartPoint(expression.NewLiteral(types.Point{SRID: types.GeoSpatialSRID, X: 123, Y: 456}, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-geometry argument", func(t *testing.T) {
		require := require.New(t)
		f := NewStartPoint(expression.NewLiteral(123, types.Int8))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestEndPoint(t *testing.T) {
	t.Run("simple case", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 123, Y: 456}
		e := types.Point{X: 456, Y: 789}
		l := types.LineString{Points: []types.Point{s, e}}
		f := NewEndPoint(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(e, v)
	})

	t.Run("simple case with SRID", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{SRID: types.GeoSpatialSRID, X: 123, Y: 456}
		e := types.Point{SRID: types.GeoSpatialSRID, X: 456, Y: 789}
		l := types.LineString{Points: []types.Point{s, e}}
		f := NewEndPoint(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(e, v)
	})

	t.Run("null argument", func(t *testing.T) {
		require := require.New(t)
		f := NewEndPoint(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-linestring argument", func(t *testing.T) {
		require := require.New(t)
		f := NewEndPoint(expression.NewLiteral(types.Point{SRID: types.GeoSpatialSRID, X: 123, Y: 456}, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-geometry argument", func(t *testing.T) {
		require := require.New(t)
		f := NewEndPoint(expression.NewLiteral(123, types.Int8))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestIsClosed(t *testing.T) {
	t.Run("simple case is closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 0, Y: 0}
		p1 := types.Point{X: 1, Y: 1}
		p2 := types.Point{X: 2, Y: 2}
		p3 := types.Point{X: 3, Y: 3}
		l := types.LineString{Points: []types.Point{s, p1, p2, p3, s}}
		f := NewIsClosed(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("simple case with SRID is closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{SRID: types.GeoSpatialSRID, X: 0, Y: 0}
		p1 := types.Point{SRID: types.GeoSpatialSRID, X: 1, Y: 1}
		p2 := types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 2}
		p3 := types.Point{SRID: types.GeoSpatialSRID, X: 3, Y: 3}
		l := types.LineString{Points: []types.Point{s, p1, p2, p3, s}}
		f := NewIsClosed(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("simple case is not closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 0, Y: 0}
		p1 := types.Point{X: 1, Y: 1}
		p2 := types.Point{X: 2, Y: 2}
		p3 := types.Point{X: 3, Y: 3}
		l := types.LineString{Points: []types.Point{s, p1, p2, p3}}
		f := NewIsClosed(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("multilinestring all closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 0, Y: 0}
		p1 := types.Point{X: 1, Y: 1}
		p2 := types.Point{X: 2, Y: 2}
		p3 := types.Point{X: 3, Y: 3}
		l1 := types.LineString{Points: []types.Point{s, p1, p2, p3, s}}
		l2 := types.LineString{Points: []types.Point{s, p2, p1, s}}
		ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}
		f := NewIsClosed(expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("multilinestring one line not closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 0, Y: 0}
		p1 := types.Point{X: 1, Y: 1}
		p2 := types.Point{X: 2, Y: 2}
		p3 := types.Point{X: 3, Y: 3}
		l1 := types.LineString{Points: []types.Point{s, p1, p2, p3, s}}
		l2 := types.LineString{Points: []types.Point{s, p2, p1}}
		ml := types.MultiLineString{Lines: []types.LineString{l1, l2}}
		f := NewIsClosed(expression.NewLiteral(ml, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(false, v)
	})

	t.Run("zero length linestring is closed", func(t *testing.T) {
		require := require.New(t)
		s := types.Point{X: 0, Y: 0}
		l := types.LineString{Points: []types.Point{s, s, s}}
		f := NewIsClosed(expression.NewLiteral(l, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(true, v)
	})

	t.Run("null argument", func(t *testing.T) {
		require := require.New(t)
		f := NewIsClosed(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-linestring argument", func(t *testing.T) {
		require := require.New(t)
		f := NewIsClosed(expression.NewLiteral(types.Point{SRID: types.GeoSpatialSRID, X: 123, Y: 456}, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("non-geometry argument", func(t *testing.T) {
		require := require.New(t)
		f := NewIsClosed(expression.NewLiteral(123, types.Int8))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
