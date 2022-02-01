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

func TestSRID(t *testing.T) {
	t.Run("select unspecified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 0, X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 4326, X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(4326), v)
	})

	t.Run("change SRID to 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 4326, X: 1, Y: 2}, sql.PointType{}),
			expression.NewLiteral(0, sql.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: 0, X: 1, Y: 2}, v)
	})

	t.Run("change SRID to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 0, X: 123.4, Y: 56.789}, sql.PointType{}),
			expression.NewLiteral(4326, sql.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: 4326, X: 123.4, Y: 56.789}, v)
	})

	t.Run("change SRID to invalid 1234", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 0, X: 123.4, Y: 56.789}, sql.PointType{}),
			expression.NewLiteral(1234, sql.Int32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("invalid number of arguments, 0", func(t *testing.T) {
		require := require.New(t)
		_, err := NewSRID()
		require.Error(err)
	})

	t.Run("change SRID of linestring to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, sql.LinestringType{}),
			expression.NewLiteral(4326, sql.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Linestring{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}, v)
	})

	t.Run("change SRID of polygon to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, sql.PolygonType{}),
			expression.NewLiteral(4326, sql.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{SRID: 4326, Lines: []sql.Linestring{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}, v)
	})
}
