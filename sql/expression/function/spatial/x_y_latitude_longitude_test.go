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

package spatial

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSTX(t *testing.T) {
	t.Run("select int x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1.0, v)
	})

	t.Run("select float x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 123.456, Y: 78.9}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(123.456, v)
	})

	t.Run("replace x value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 123.456, Y: 0}, v)
	})

	t.Run("replace x value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: -123.456, Y: 0}, v)
	})

	t.Run("replace x value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: -123.456, Y: 0}, v)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral("notapoint", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type with one argument", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})

	t.Run("check return type with two arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTX(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}

func TestSTY(t *testing.T) {
	t.Run("select int y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2.0, v)
	})

	t.Run("select float y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 123.456, Y: 78.9}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(78.9, v)
	})

	t.Run("replace y value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 0, Y: 123.456}, v)
	})

	t.Run("replace y value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 0, Y: -123.456}, v)
	})

	t.Run("replace y value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 0, Y: -123.456}, v)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral("notapoint", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type with one argument", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})

	t.Run("check return type with two arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTY(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		typ := f.Type()
		_, ok := typ.(types.PointType)
		require.True(ok)
	})
}

func TestLongitude(t *testing.T) {
	t.Run("select longitude value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1.0, v)
	})

	t.Run("replace longitude value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 123.456, Y: 0}, v)
	})

	t.Run("replace x value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: -123.456, Y: 0}, v)
	})

	t.Run("replace x value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-123.456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: -123.456, Y: 0}, v)
	})

	t.Run("null point", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("replace with null value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("replace x value with out of range coordinate", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(999, types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral("notapoint", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type with one argument", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})

	t.Run("check return type with two arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLongitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(123.456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}

func TestLatitude(t *testing.T) {
	t.Run("select latitude value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2.0, v)
	})

	t.Run("replace latitude value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(12.3456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 0, Y: 12.3456}, v)
	})

	t.Run("replace y value with valid string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-12.3456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 0, Y: -12.3456}, v)
	})

	t.Run("replace y value with negative float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral("-12.3456", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 0, Y: -12.3456}, v)
	})

	t.Run("null point", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("replace with null value", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("replace y value with out of range coordinate", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(999, types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("non-point provided", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral("notapoint", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type with one argument", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})

	t.Run("check return type with two arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLatitude(expression.NewLiteral(types.Point{SRID: 4326, X: 0, Y: 0}, types.PointType{}),
			expression.NewLiteral(12.3456, types.Float64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}
