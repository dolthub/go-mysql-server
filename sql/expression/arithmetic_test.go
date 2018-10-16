package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestPlus(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    float64
	}{
		{"1 + 1", 1, 1, 2},
		{"-1 + 1", -1, 1, 0},
		{"0 + 0", 0, 0, 0},
		{"0.14159 + 3.0", 0.14159, 3.0, float64(0.14159) + float64(3)},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewPlus(
				NewLiteral(tt.left, sql.Float64),
				NewLiteral(tt.right, sql.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}

	require := require.New(t)
	result, err := NewPlus(NewLiteral("2", sql.Text), NewLiteral(3, sql.Float64)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(float64(5), result)
}

func TestMinus(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    float64
	}{
		{"1 - 1", 1, 1, 0},
		{"1 - 1", 1, 1, 0},
		{"0 - 0", 0, 0, 0},
		{"3.14159 - 3.0", 3.14159, 3.0, float64(3.14159) - float64(3.0)},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMinus(
				NewLiteral(tt.left, sql.Float64),
				NewLiteral(tt.right, sql.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}

	require := require.New(t)
	result, err := NewMinus(NewLiteral("10", sql.Text), NewLiteral(10, sql.Int64)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(float64(0), result)
}

func TestMult(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    float64
	}{
		{"1 * 1", 1, 1, 1},
		{"-1 * 1", -1, 1, -1},
		{"0 * 0", 0, 0, 0},
		{"3.14159 * 3.0", 3.14159, 3.0, float64(3.14159) * float64(3.0)},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMult(
				NewLiteral(tt.left, sql.Float64),
				NewLiteral(tt.right, sql.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}

	require := require.New(t)
	result, err := NewMult(NewLiteral("10", sql.Text), NewLiteral("10", sql.Text)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(float64(100), result)
}

func TestDiv(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    float64
	}{
		{"1 / 1", 1, 1, 1},
		{"-1 / 1", -1, 1, -1},
		{"0 / 1234567890", 0, 12345677890, 0},
		{"3.14159 / 3.0", 3.14159, 3.0, float64(3.14159) / float64(3.0)},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewDiv(
				NewLiteral(tt.left, sql.Float64),
				NewLiteral(tt.right, sql.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestShiftLeft(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right uint64
		expected    uint64
	}{
		{"1 << 1", 1, 1, 2},
		{"1 << 3", 1, 3, 8},
		{"1024 << 0", 1024, 0, 1024},
		{"0 << 1024", 0, 1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewShiftLeft(
				NewLiteral(tt.left, sql.Uint64),
				NewLiteral(tt.right, sql.Uint64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestShiftRight(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right uint64
		expected    uint64
	}{
		{"1 >> 1", 1, 1, 0},
		{"8 >> 1", 8, 1, 4},
		{"3 >> 1", 3, 1, 1},
		{"1024 >> 0", 1024, 0, 1024},
		{"0 >> 1024", 0, 1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewShiftRight(
				NewLiteral(tt.left, sql.Uint64),
				NewLiteral(tt.right, sql.Uint64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestBitAnd(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    int64
	}{
		{"1 & 1", 1, 1, 1},
		{"8 & 1", 8, 1, 0},
		{"3 & 1", 3, 1, 1},
		{"1024 & 0", 1024, 0, 0},
		{"0 & 1024", 0, 1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitAnd(
				NewLiteral(tt.left, sql.Int64),
				NewLiteral(tt.right, sql.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestBitOr(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    int64
	}{
		{"1 | 1", 1, 1, 1},
		{"8 | 1", 8, 1, 9},
		{"3 | 1", 3, 1, 3},
		{"1024 | 0", 1024, 0, 1024},
		{"0 | 1024", 0, 1024, 1024},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitOr(
				NewLiteral(tt.left, sql.Int64),
				NewLiteral(tt.right, sql.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestBitXor(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    int64
	}{
		{"1 ^ 1", 1, 1, 0},
		{"8 ^ 1", 8, 1, 9},
		{"3 ^ 1", 3, 1, 2},
		{"1024 ^ 0", 1024, 0, 1024},
		{"0 ^ -1024", 0, -1024, -1024},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitXor(
				NewLiteral(tt.left, sql.Int64),
				NewLiteral(tt.right, sql.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestIntDiv(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    int64
	}{
		{"1 div 1", 1, 1, 1},
		{"8 div 3", 8, 3, 2},
		{"1 div 3", 1, 3, 0},
		{"0 div -1024", 0, -1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewIntDiv(
				NewLiteral(tt.left, sql.Int64),
				NewLiteral(tt.right, sql.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestMod(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    int64
	}{
		{"1 % 1", 1, 1, 0},
		{"8 % 3", 8, 3, 2},
		{"1 % 3", 1, 3, 1},
		{"0 % -1024", 0, -1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMod(
				NewLiteral(tt.left, sql.Int64),
				NewLiteral(tt.right, sql.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestAllFloat64(t *testing.T) {
	var testCases = []struct {
		op       string
		value    float64
		expected float64
	}{
		{"+", 1.0, 1.0},
		{"-", -8.0, 9.0},
		{"/", 3.0, 3.0},
		{"*", 0.0, 0.0},
	}

	// ((((0 + 1) - (-8)) / 3) * 0) == 0
	lval := NewLiteral(float64(0.0), sql.Float64)
	for _, tt := range testCases {
		t.Run(tt.op, func(t *testing.T) {
			require := require.New(t)
			result, err := NewArithmetic(lval,
				NewLiteral(tt.value, sql.Float64), tt.op,
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)

			lval = NewLiteral(result, sql.Float64)
		})
	}
}

func TestAllInt64(t *testing.T) {
	var testCases = []struct {
		op       string
		value    int64
		expected int64
	}{
		{"|", 1, 1},
		{"&", 3, 1},
		{"^", 1024, 1025},
		{"%", 1024, 1},
		{"div", 1024, 0},
	}

	// (((((0 | 1) & 3) ^ 1024) % 1024) div 1024) == 0
	lval := NewLiteral(int64(0), sql.Int64)
	for _, tt := range testCases {
		t.Run(tt.op, func(t *testing.T) {
			require := require.New(t)
			result, err := NewArithmetic(lval,
				NewLiteral(tt.value, sql.Int64), tt.op,
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)

			lval = NewLiteral(result, sql.Int64)
		})
	}
}

func TestUnaryMinus(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		typ      sql.Type
		expected interface{}
	}{
		{"int32", int32(1), sql.Int32, int32(-1)},
		{"uint32", uint32(1), sql.Uint32, int32(-1)},
		{"int64", int64(1), sql.Int64, int64(-1)},
		{"uint64", uint64(1), sql.Uint64, int64(-1)},
		{"float32", float32(1), sql.Float32, float32(-1)},
		{"float64", float64(1), sql.Float64, float64(-1)},
		{"int text", "1", sql.Text, float64(-1)},
		{"float text", "1.2", sql.Text, float64(-1.2)},
		{"nil", nil, sql.Text, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			f := NewUnaryMinus(NewLiteral(tt.input, tt.typ))
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
