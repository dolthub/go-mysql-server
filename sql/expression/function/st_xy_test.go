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

func TestSTX(t *testing.T) {
	t.Run("select int x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1.0, v)
	})

	t.Run("select float x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(sql.Point{X: 123.456, Y: 78.9}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(123.456, v)
	})

	t.Run("replace x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral(123.456, sql.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 123.456, Y: 0}, v)
	})

	t.Run("replace x value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral("-123.456", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: -123.456, Y: 0}, v)
	})

	t.Run("replace x value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral("-123.456", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: -123.456, Y: 0}, v)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral("notapoint", sql.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestSTY(t *testing.T) {
	t.Run("select int y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2.0, v)
	})

	t.Run("select float y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(sql.Point{X: 123.456, Y: 78.9}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(78.9, v)
	})

	t.Run("replace y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral(123.456, sql.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 0, Y: 123.456}, v)
	})

	t.Run("replace y value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral("-123.456", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 0, Y: -123.456}, v)
	})

	t.Run("replace y value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(sql.Point{X: 0, Y: 0}, sql.PointType{}),
			expression.NewLiteral("-123.456", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 0, Y: -123.456}, v)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral("notapoint", sql.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
