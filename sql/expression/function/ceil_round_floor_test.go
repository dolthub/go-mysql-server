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
