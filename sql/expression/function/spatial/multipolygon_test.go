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
)

func TestMultiPolygon(t *testing.T) {
	t.Run("create valid multipolygon", func(t *testing.T) {
		require := require.New(t)
		line := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := sql.Polygon{Lines: []sql.LineString{line}}
		f, err := NewMultiPolygon(expression.NewLiteral(poly, sql.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPolygon{Polygons: []sql.Polygon{poly}}, v)
	})

	t.Run("create valid multipolygon with multiple polygons", func(t *testing.T) {
		require := require.New(t)
		line1 := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly1 := sql.Polygon{Lines: []sql.LineString{line1}}
		line2 := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly2 := sql.Polygon{Lines: []sql.LineString{line2}}
		f, err := NewMultiPolygon(
			expression.NewLiteral(poly1, sql.PolygonType{}),
			expression.NewLiteral(poly2, sql.PolygonType{}),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPolygon{Polygons: []sql.Polygon{poly1, poly2}}, v)
	})
}

func TestNewMultiPolygon(t *testing.T) {
	require := require.New(t)
	_, err := NewMultiPolygon(expression.NewLiteral(nil, sql.PolygonType{}),
		expression.NewLiteral(nil, sql.PolygonType{}),
		expression.NewLiteral(nil, sql.PolygonType{}),
	)
	require.NoError(err)
}
