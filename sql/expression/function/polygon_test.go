// Copyright 2020-2021 Dolthub, Inc.
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

func TestPolygon(t *testing.T) {
	t.Run("create valid polygon", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, sql.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid polygon with multiple linestrings", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, sql.LineStringType{}),
			expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, sql.LineStringType{}),
			expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}, sql.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, {Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create invalid using invalid linestring", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}}}, sql.LineStringType{}))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create invalid using non-linearring linestring", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 0}}}, sql.LineStringType{}))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestNewPolygon(t *testing.T) {
	require := require.New(t)
	_, err := NewLineString(expression.NewLiteral(nil, sql.PointType{}),
		expression.NewLiteral(nil, sql.PointType{}),
		expression.NewLiteral(nil, sql.PointType{}),
	)
	require.NoError(err)
}
