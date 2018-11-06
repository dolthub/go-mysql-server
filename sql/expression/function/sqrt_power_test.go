package function

import (
	"testing"
	"math"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSqrt(t *testing.T) {
	f := NewSqrt(
		expression.NewGetField(0, sql.Float64, "n", false),
	)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"invalid string", sql.NewRow("foo"), nil, false},
		{"valid string", sql.NewRow("9"), float64(3), false},
		{"number is zero", sql.NewRow(0), float64(0), false},
		{"positive number", sql.NewRow(8), float64(2.8284271247461903), false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}

	// Test negative number
	f = NewSqrt(
		expression.NewGetField(0, sql.Float64, "n", false),
	)
	require := require.New(t)
	v, err := f.Eval(sql.NewEmptyContext(), []interface{}{float64(-4)})
	require.NoError(err)
	require.IsType(float64(0), v)
	require.True(math.IsNaN(v.(float64)))
}

func TestPower(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"Base and exp are nil", sql.Float64, sql.NewRow(nil, nil), nil, false},
		{"Base is nil", sql.Float64, sql.NewRow(2, nil), nil, false},
		{"Exp is nil", sql.Float64, sql.NewRow(nil, 2), nil, false},

		{"Base is 0", sql.Float64, sql.NewRow(0, 2), float64(0), false},
		{"Base and exp is 0", sql.Float64, sql.NewRow(0, 0), float64(1), false},
		{"Exp is 0", sql.Float64, sql.NewRow(2, 0), float64(1), false},
		{"Base is negative", sql.Float64, sql.NewRow(-2, 2), float64(4), false},
		{"Exp is negative", sql.Float64, sql.NewRow(2, -2), float64(0.25), false},
		{"Base and exp are invalid strings", sql.Float64, sql.NewRow("a", "b"), nil, true},
		{"Base and exp are valid strings", sql.Float64, sql.NewRow("2", "2"), float64(4), false},
	}
	for _, tt := range testCases {
		f := NewPowerFunc(funcNamePow)(
			expression.NewGetField(0, tt.rowType, "", false),
			expression.NewGetField(1, tt.rowType, "", false),
		)
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}

	// Test inf numbers
	f := NewPowerFunc(funcNamePow)(
		expression.NewGetField(0, sql.Float64, "", false),
		expression.NewGetField(1, sql.Float64, "", false),
	)
	require := require.New(t)
	v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(2, math.Inf(1)))
	require.NoError(err)
	require.IsType(float64(0), v)
	require.True(math.IsInf(v.(float64), 1))

	v, err = f.Eval(sql.NewEmptyContext(), sql.NewRow(math.Inf(1), 2))
	require.NoError(err)
	require.IsType(float64(0), v)
	require.True(math.IsInf(v.(float64), 1))
}
