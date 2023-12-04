package sql

import (
	"fmt"
	"math"
	"testing"

	"github.com/shopspring/decimal"
	assert "github.com/stretchr/testify/require"
)

func TestRoundUpFloat(t *testing.T) {
	tests := []struct {
		val interface{}
		exp interface{}
	}{
		// float 32
		{
			val: float32(-math.MaxFloat32),
			exp: float32(-math.MaxFloat32),
		},
		{
			val: float32(-1.1),
			exp: float32(-1.0),
		},
		{
			val: float32(-0.9),
			exp: float32(0.0),
		},
		{
			val: float32(-0.5),
			exp: float32(0.0),
		},
		{
			val: float32(-0.1),
			exp: float32(0.0),
		},
		{
			val: float32(0.0),
			exp: float32(0.0),
		},
		{
			val: float32(0.1),
			exp: float32(1.0),
		},
		{
			val: float32(0.5),
			exp: float32(1.0),
		},
		{
			val: float32(0.9),
			exp: float32(1.0),
		},
		{
			val: float32(1.1),
			exp: float32(2.0),
		},
		{
			val: float32(math.MaxFloat32),
			exp: float32(math.MaxFloat32),
		},

		// float64
		{
			val: -math.MaxFloat64,
			exp: -math.MaxFloat64,
		},
		{
			val: -1.1,
			exp: -1.0,
		},
		{
			val: -0.9,
			exp: 0.0,
		},
		{
			val: -0.5,
			exp: 0.0,
		},
		{
			val: -0.1,
			exp: 0.0,
		},
		{
			val: 0.0,
			exp: 0.0,
		},
		{
			val: 0.1,
			exp: 1.0,
		},
		{
			val: 0.5,
			exp: 1.0,
		},
		{
			val: 0.9,
			exp: 1.0,
		},
		{
			val: 1.1,
			exp: 2.0,
		},
		{
			val: math.MaxFloat64,
			exp: math.MaxFloat64,
		},

		// decimal
		{
			val: decimal.NewFromFloat(-math.MaxFloat64),
			exp: decimal.NewFromFloat(-math.MaxFloat64),
		},
		{
			val: decimal.NewFromFloat(-1.1),
			exp: decimal.NewFromFloat(-1.0),
		},
		{
			val: decimal.NewFromFloat(-0.9),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(-0.5),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(-0.1),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(0.0),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(0.1),
			exp: decimal.NewFromFloat(1.0),
		},
		{
			val: decimal.NewFromFloat(0.5),
			exp: decimal.NewFromFloat(1.0),
		},
		{
			val: decimal.NewFromFloat(0.9),
			exp: decimal.NewFromFloat(1.0),
		},
		{
			val: decimal.NewFromFloat(1.1),
			exp: decimal.NewFromFloat(2.0),
		},
		{
			val: decimal.NewFromFloat(math.MaxFloat64),
			exp: decimal.NewFromFloat(math.MaxFloat64),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("round up: %v", test.val), func(t *testing.T) {
			res := roundUpFloat(test.val)
			switch test.val.(type) {
			case float32, float64:
				assert.Equal(t, test.exp, res)
			case decimal.Decimal:
				assert.True(t, test.exp.(decimal.Decimal).Equals(res.(decimal.Decimal)))
			}
		})
	}
}

func TestRoundDownFloat(t *testing.T) {
	tests := []struct {
		val interface{}
		exp interface{}
	}{
		// float 32
		{
			val: float32(-math.MaxFloat32),
			exp: float32(-math.MaxFloat32),
		},
		{
			val: float32(-1.1),
			exp: float32(-2.0),
		},
		{
			val: float32(-0.9),
			exp: float32(-1.0),
		},
		{
			val: float32(-0.5),
			exp: float32(-1.0),
		},
		{
			val: float32(-0.1),
			exp: float32(-1.0),
		},
		{
			val: float32(0.0),
			exp: float32(0.0),
		},
		{
			val: float32(0.1),
			exp: float32(0.0),
		},
		{
			val: float32(0.5),
			exp: float32(0.0),
		},
		{
			val: float32(0.9),
			exp: float32(0.0),
		},
		{
			val: float32(1.1),
			exp: float32(1.0),
		},
		{
			val: float32(math.MaxFloat32),
			exp: float32(math.MaxFloat32),
		},

		// float64
		{
			val: -math.MaxFloat64,
			exp: -math.MaxFloat64,
		},
		{
			val: -1.1,
			exp: -2.0,
		},
		{
			val: -0.9,
			exp: -1.0,
		},
		{
			val: -0.5,
			exp: -1.0,
		},
		{
			val: -0.1,
			exp: -1.0,
		},
		{
			val: 0.0,
			exp: 0.0,
		},
		{
			val: 0.1,
			exp: 0.0,
		},
		{
			val: 0.5,
			exp: 0.0,
		},
		{
			val: 0.9,
			exp: 0.0,
		},
		{
			val: 1.1,
			exp: 1.0,
		},
		{
			val: math.MaxFloat64,
			exp: math.MaxFloat64,
		},

		// decimal
		{
			val: decimal.NewFromFloat(-math.MaxFloat64),
			exp: decimal.NewFromFloat(-math.MaxFloat64),
		},
		{
			val: decimal.NewFromFloat(-1.1),
			exp: decimal.NewFromFloat(-2.0),
		},
		{
			val: decimal.NewFromFloat(-0.9),
			exp: decimal.NewFromFloat(-1.0),
		},
		{
			val: decimal.NewFromFloat(-0.5),
			exp: decimal.NewFromFloat(-1.0),
		},
		{
			val: decimal.NewFromFloat(-0.1),
			exp: decimal.NewFromFloat(-1.0),
		},
		{
			val: decimal.NewFromFloat(0.0),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(0.1),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(0.5),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(0.9),
			exp: decimal.NewFromFloat(0.0),
		},
		{
			val: decimal.NewFromFloat(1.1),
			exp: decimal.NewFromFloat(1.0),
		},
		{
			val: decimal.NewFromFloat(math.MaxFloat64),
			exp: decimal.NewFromFloat(math.MaxFloat64),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("round down: %v", test.val), func(t *testing.T) {
			res := roundDownFloat(test.val)
			switch test.val.(type) {
			case float32, float64:
				assert.Equal(t, test.exp, res)
			case decimal.Decimal:
				assert.True(t, test.exp.(decimal.Decimal).Equals(res.(decimal.Decimal)))
			}
		})
	}
}
