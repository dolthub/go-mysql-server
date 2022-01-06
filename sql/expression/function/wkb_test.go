package function

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestAsWKB(t *testing.T) {
	t.Run("convert point", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Point{X: -123.45, Y: 678.9}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Linestring{Points: []sql.Point{{1, 2}, {3, 4}}}, sql.LinestringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}}}, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral("notageometry", sql.Blob))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}

func TestGeomFromWKB(t *testing.T) {
	t.Run("convert point in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point in big endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF00000000000004000000000000000")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point bad point", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF0000000000000")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: -123.45, Y: 678.9}, v)
	})

	t.Run("convert linestring in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Linestring{Points: []sql.Point{{1, 2}, {3, 4}}}, v)
	})

	t.Run("convert polygon in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}}}, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
