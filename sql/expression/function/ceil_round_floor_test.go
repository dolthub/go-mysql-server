package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCeil(t *testing.T) {
	//Test Float 64
	f := NewCeil(expression.NewGetField(0, sql.Float64, "", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.NewRow(nil), nil, nil},
		{"float64 is ok", sql.NewRow(5.8), float64(6), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req := require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Float 32
	f = NewCeil(expression.NewGetField(0, sql.Float32, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float32 is nil", sql.NewRow(nil), nil, nil},
		{"float32 is ok", sql.NewRow(float32(5.8)), float32(6), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Integer
	f = NewCeil(expression.NewGetField(0, sql.Int32, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"int is nil", sql.NewRow(nil), nil, nil},
		{"int is ok", sql.NewRow(int32(6)), int32(6), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsNumber(f.Type()))
	req.False(f.IsNullable())

	//Test Non Numerical, Non Null
	f = NewCeil(expression.NewGetField(0, sql.Blob, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"blob is nil", sql.NewRow(nil), nil, sql.ErrInvalidType},
		{"int is ok", sql.NewRow([]byte{1, 2, 3}), nil, sql.ErrInvalidType},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

}

func TestFloor(t *testing.T) {
	//Test Float 64
	f := NewFloor(expression.NewGetField(0, sql.Float64, "", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.NewRow(nil), nil, nil},
		{"float64 is ok", sql.NewRow(5.8), float64(5), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req := require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Float 32
	f = NewFloor(expression.NewGetField(0, sql.Float32, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float32 is nil", sql.NewRow(nil), nil, nil},
		{"float32 is ok", sql.NewRow(float32(5.8)), float32(5), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Integer
	f = NewFloor(expression.NewGetField(0, sql.Int32, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"int is nil", sql.NewRow(nil), nil, nil},
		{"int is ok", sql.NewRow(int32(6)), int32(6), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsNumber(f.Type()))
	req.False(f.IsNullable())

	//Test Non Numerical, Non Null
	f = NewFloor(expression.NewGetField(0, sql.Blob, "", false))

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"blob is nil", sql.NewRow(nil), nil, sql.ErrInvalidType},
		{"int is ok", sql.NewRow([]byte{1, 2, 3}), nil, sql.ErrInvalidType},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestRound(t *testing.T) {
	//Test Float 64
	f := NewRound(
		expression.NewGetField(0, sql.Float64, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.NewRow(nil, nil), nil, nil},
		{"float64 without d", sql.NewRow(5.8, nil), float64(6), nil},
		{"float64 with d", sql.NewRow(5.855, 2), float64(5.86), nil},
		{"float64 with negative d", sql.NewRow(52.855, -1), float64(50), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req := require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Float 32
	f = NewRound(
		expression.NewGetField(0, sql.Float32, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float32 is nil", sql.NewRow(nil, nil), nil, nil},
		{"float32 without d", sql.NewRow(float32(5.8), nil), float32(6), nil},
		{"float32 with d", sql.NewRow(float32(5.855), 2), float32(5.86), nil},
		{"float32 with negative d", sql.NewRow(float32(52.855), -1), float32(50), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsDecimal(f.Type()))
	req.False(f.IsNullable())

	//Test Int 64
	f = NewRound(
		expression.NewGetField(0, sql.Int64, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"int64 is nil", sql.NewRow(nil, nil), nil, nil},
		{"int64 without d", sql.NewRow(int64(6), nil), int64(6), nil},
		{"int64 with d", sql.NewRow(int64(5), 2), int64(5), nil},
		{"int64 with negative d", sql.NewRow(int64(52), -1), int64(50), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsNumber(f.Type()))
	req.False(f.IsNullable())

	//Test Int 32
	f = NewRound(
		expression.NewGetField(0, sql.Int32, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	testCases = []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"int32 is nil", sql.NewRow(nil, nil), nil, nil},
		{"int32 without d", sql.NewRow(int32(6), nil), int32(6), nil},
		{"int32 with d", sql.NewRow(int32(5), 2), int32(5), nil},
		{"int32 with negative d", sql.NewRow(int32(52), -1), int32(50), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	req = require.New(t)

	req.True(sql.IsNumber(f.Type()))
	req.False(f.IsNullable())

	//Test Wrong Type
	f = NewRound(
		expression.NewGetField(0, sql.Blob, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	req = require.New(t)

	result, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(sql.NewRow([]byte{1, 2, 3}, 2)))
	req.Nil(result)
	req.True(sql.ErrInvalidType.Is(err))
}
