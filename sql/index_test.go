package sql

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
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
			val: NewDecimalFromFloat64(-math.MaxFloat64),
			exp: NewDecimalFromFloat64(-math.MaxFloat64),
		},
		{
			val: NewDecimalFromFloat64(-1.1),
			exp: NewDecimalFromFloat64(-1.0),
		},
		{
			val: NewDecimalFromFloat64(-0.9),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(-0.5),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(-0.1),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(0.0),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(0.1),
			exp: NewDecimalFromFloat64(1.0),
		},
		{
			val: NewDecimalFromFloat64(0.5),
			exp: NewDecimalFromFloat64(1.0),
		},
		{
			val: NewDecimalFromFloat64(0.9),
			exp: NewDecimalFromFloat64(1.0),
		},
		{
			val: NewDecimalFromFloat64(1.1),
			exp: NewDecimalFromFloat64(2.0),
		},
		{
			val: NewDecimalFromFloat64(math.MaxFloat64),
			exp: NewDecimalFromFloat64(math.MaxFloat64),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("round up: %v", test.val), func(t *testing.T) {
			res := ceil(test.val)
			switch test.val.(type) {
			case float32, float64:
				assert.Equal(t, test.exp, res)
			case *apd.Decimal:
				assert.True(t, test.exp.(*apd.Decimal).Cmp(res.(*apd.Decimal)) == 0)
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
			val: NewDecimalFromFloat64(-math.MaxFloat64),
			exp: NewDecimalFromFloat64(-math.MaxFloat64),
		},
		{
			val: NewDecimalFromFloat64(-1.1),
			exp: NewDecimalFromFloat64(-2.0),
		},
		{
			val: NewDecimalFromFloat64(-0.9),
			exp: NewDecimalFromFloat64(-1.0),
		},
		{
			val: NewDecimalFromFloat64(-0.5),
			exp: NewDecimalFromFloat64(-1.0),
		},
		{
			val: NewDecimalFromFloat64(-0.1),
			exp: NewDecimalFromFloat64(-1.0),
		},
		{
			val: NewDecimalFromFloat64(0.0),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(0.1),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(0.5),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(0.9),
			exp: NewDecimalFromFloat64(0.0),
		},
		{
			val: NewDecimalFromFloat64(1.1),
			exp: NewDecimalFromFloat64(1.0),
		},
		{
			val: NewDecimalFromFloat64(math.MaxFloat64),
			exp: NewDecimalFromFloat64(math.MaxFloat64),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("round down: %v", test.val), func(t *testing.T) {
			res := floor(test.val)
			switch test.val.(type) {
			case float32, float64:
				assert.Equal(t, test.exp, res)
			case *apd.Decimal:
				assert.True(t, test.exp.(*apd.Decimal).Cmp(res.(*apd.Decimal)) == 0)
			}
		})
	}
}

// NewDecimalFromFloat64 returns *apd.Decimal set from given float64
func NewDecimalFromFloat64(x float64) *apd.Decimal {
	dec := new(apd.Decimal)
	dec, _ = dec.SetFloat64(x)
	return dec
}
