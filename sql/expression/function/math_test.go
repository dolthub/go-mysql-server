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
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestRand(t *testing.T) {
	r, _ := NewRand(sql.NewEmptyContext())

	assert.Equal(t, sql.Float64, r.Type())
	assert.Equal(t, "RAND()", r.String())

	f, err := r.Eval(nil, nil)
	require.NoError(t, err)
	f64, ok := f.(float64)
	require.True(t, ok, "not a float64")

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642, ok := f.(float64)
	require.True(t, ok, "not a float64")

	assert.NotEqual(t, f64, f642) // i guess this could fail, but come on
}

func TestRandWithSeed(t *testing.T) {
	r, _ := NewRand(sql.NewEmptyContext(), expression.NewLiteral(10, sql.Int8))

	assert.Equal(t, sql.Float64, r.Type())
	assert.Equal(t, "RAND(10)", r.String())

	f, err := r.Eval(nil, nil)
	require.NoError(t, err)
	f64 := f.(float64)

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642 := f.(float64)

	assert.Equal(t, f64, f642)

	r, _ = NewRand(sql.NewEmptyContext(), expression.NewLiteral("not a number", sql.LongText))
	assert.Equal(t, `RAND("not a number")`, r.String())

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f64 = f.(float64)

	assert.GreaterOrEqual(t, f64, float64(0))
	assert.Less(t, f64, float64(1))

	f, err = r.Eval(nil, nil)
	require.NoError(t, err)
	f642 = f.(float64)

	assert.Equal(t, f64, f642)
}

func TestRadians(t *testing.T) {
	f := sql.Function1{Name: "radians", Fn: NewRadians}
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(0.0, "0")
	tf.AddSucceeding(-math.Pi, "-180")
	tf.AddSucceeding(math.Pi, int16(180))
	tf.AddSucceeding(math.Pi/2.0, (90))
	tf.AddSucceeding(2*math.Pi, 360.0)
	tf.Test(t, nil, nil)
}

func TestDegrees(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
	}{
		{"string pi", "3.1415926536", 180.0},
		{"decimal 2pi", decimal.NewFromFloat(2 * math.Pi), 360.0},
		{"float64 pi/2", math.Pi / 2.0, 90.0},
		{"float32 3*pi/2", float32(3.0 * math.Pi / 2.0), 270.0},
	}

	f := sql.Function1{Name: "degrees", Fn: NewDegrees}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			degrees := f.Fn(sql.NewEmptyContext(), expression.NewLiteral(test.input, nil))
			res, err := degrees.Eval(nil, nil)
			require.NoError(t, err)
			assert.True(t, withinRoundingErr(test.expected, res.(float64)))
		})
	}
}

func TestCRC32(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint32
	}{
		{"CRC32('MySQL)", "MySQL", 3259397556},
		{"CRC32('mysql')", "mysql", 2501908538},

		{"CRC32('6')", "6", 498629140},
		{"CRC32(int 6)", 6, 498629140},
		{"CRC32(int8 6)", int8(6), 498629140},
		{"CRC32(int16 6)", int16(6), 498629140},
		{"CRC32(int32 6)", int32(6), 498629140},
		{"CRC32(int64 6)", int64(6), 498629140},
		{"CRC32(uint 6)", uint(6), 498629140},
		{"CRC32(uint8 6)", uint8(6), 498629140},
		{"CRC32(uint16 6)", uint16(6), 498629140},
		{"CRC32(uint32 6)", uint32(6), 498629140},
		{"CRC32(uint64 6)", uint64(6), 498629140},

		{"CRC32('6.0')", "6.0", 4068047280},
		{"CRC32(float32 6.0)", float32(6.0), 4068047280},
		{"CRC32(float64 6.0)", float64(6.0), 4068047280},
	}

	f := sql.Function1{Name: "crc32", Fn: NewCrc32}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			crc32 := f.Fn(sql.NewEmptyContext(), expression.NewLiteral(test.input, nil))
			res, err := crc32.Eval(nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, res)
		})
	}

	crc32 := f.Fn(sql.NewEmptyContext(), nil)
	res, err := crc32.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, res)

	nullLiteral := expression.NewLiteral(nil, sql.Null)
	crc32 = f.Fn(sql.NewEmptyContext(), nullLiteral)
	res, err = crc32.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
}

func TestTrigFunctions(t *testing.T) {
	asin := sql.Function1{Name: "asin", Fn: NewAsin}
	acos := sql.Function1{Name: "acos", Fn: NewAcos}
	atan := sql.Function1{Name: "atan", Fn: NewAtan}
	sin := sql.Function1{Name: "sin", Fn: NewSin}
	cos := sql.Function1{Name: "cos", Fn: NewCos}
	tan := sql.Function1{Name: "tan", Fn: NewTan}

	const numChecks = 24
	delta := (2 * math.Pi) / float64(numChecks)
	for i := 0; i <= numChecks; i++ {
		theta := delta * float64(i)
		thetaLiteral := expression.NewLiteral(theta, nil)
		sinVal, err := sin.Fn(sql.NewEmptyContext(), thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)
		cosVal, err := cos.Fn(sql.NewEmptyContext(), thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)
		tanVal, err := tan.Fn(sql.NewEmptyContext(), thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)

		sinF, _ := sinVal.(float64)
		cosF, _ := cosVal.(float64)
		tanF, _ := tanVal.(float64)

		assert.True(t, withinRoundingErr(math.Sin(theta), sinF))
		assert.True(t, withinRoundingErr(math.Cos(theta), cosF))
		assert.True(t, withinRoundingErr(math.Tan(theta), tanF))

		asinVal, err := asin.Fn(sql.NewEmptyContext(), expression.NewLiteral(sinF, nil)).Eval(nil, nil)
		assert.NoError(t, err)
		acosVal, err := acos.Fn(sql.NewEmptyContext(), expression.NewLiteral(cosF, nil)).Eval(nil, nil)
		assert.NoError(t, err)
		atanVal, err := atan.Fn(sql.NewEmptyContext(), expression.NewLiteral(tanF, nil)).Eval(nil, nil)
		assert.NoError(t, err)

		assert.True(t, withinRoundingErr(math.Asin(sinF), asinVal.(float64)))
		assert.True(t, withinRoundingErr(math.Acos(cosF), acosVal.(float64)))
		assert.True(t, withinRoundingErr(math.Atan(tanF), atanVal.(float64)))
	}
}

func withinRoundingErr(v1, v2 float64) bool {
	const roundingErr = 0.00001
	diff := v1 - v2

	if diff < 0 {
		diff = -diff
	}

	return diff < roundingErr
}

func TestSignFunc(t *testing.T) {
	f := sql.Function1{Name: "sign", Fn: NewSign}
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSignedVariations(int8(-1), -10)
	tf.AddFloatVariations(int8(-1), -10.0)
	tf.AddSignedVariations(int8(1), 100)
	tf.AddUnsignedVariations(int8(1), 100)
	tf.AddFloatVariations(int8(1), 100.0)
	tf.AddSignedVariations(int8(0), 0)
	tf.AddUnsignedVariations(int8(0), 0)
	tf.AddFloatVariations(int8(0), 0)
	tf.AddSucceeding(int8(1), time.Now())
	tf.AddSucceeding(int8(0), false)
	tf.AddSucceeding(int8(1), true)

	// string logic matches mysql.  It's really odd.  Uses the numeric portion of the string at the beginning.  If
	// it starts with a nonnumeric character then
	tf.AddSucceeding(int8(0), "0-1z1Xaoebu")
	tf.AddSucceeding(int8(-1), "-1z1Xaoebu")
	tf.AddSucceeding(int8(1), "1z1Xaoebu")
	tf.AddSucceeding(int8(0), "z1Xaoebu")
	tf.AddSucceeding(int8(-1), "-.1a,1,1")
	tf.AddSucceeding(int8(-1), "-0.1a,1,1")
	tf.AddSucceeding(int8(1), "0.1a,1,1")
	tf.AddSucceeding(int8(0), "-0,1,1")
	tf.AddSucceeding(int8(0), "-.z1,1,1")

	tf.Test(t, nil, nil)
}
