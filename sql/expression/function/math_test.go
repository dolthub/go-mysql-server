// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestRand(t *testing.T) {
	r, _ := NewRand()

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
	r, _ := NewRand(expression.NewLiteral(10, sql.Int8))

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

	r, _ = NewRand(expression.NewLiteral("not a number", sql.LongText))
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

func TestAbsValue(t *testing.T) {
	type toTypeFunc func(float64) interface{}

	decimal1616 := sql.MustCreateDecimalType(16,16)

	toInt64 := func(x float64) interface{} { return int64(x) }
	toInt32 := func(x float64) interface{} { return int32(x) }
	toInt := func(x float64) interface{} { return int(x) }
	toInt16 := func(x float64) interface{} { return int16(x) }
	toInt8 := func(x float64) interface{} { return int8(x) }
	toUint64 := func(x float64) interface{} { return uint64(x) }
	toUint32 := func(x float64) interface{} { return uint32(x) }
	toUint := func(x float64) interface{} { return uint(x) }
	toUint16 := func(x float64) interface{} { return uint16(x) }
	toUint8 := func(x float64) interface{} { return uint8(x) }
	toFloat64 := func(x float64) interface{} {return x}
	toFloat32 := func(x float64) interface{} {return float32(x)}
	toDecimal1616 := func(x float64) interface{} {return decimal.NewFromFloat(x)}

	signedTypes := map[sql.Type]toTypeFunc {
		sql.Int64: toInt64,
		sql.Int32: toInt32,
		sql.Int24: toInt,
		sql.Int16: toInt16,
		sql.Int8: toInt8}
	unsignedTypes := map[sql.Type]toTypeFunc {
		sql.Uint64: toUint64,
		sql.Uint32: toUint32,
		sql.Uint24: toUint,
		sql.Uint16: toUint16,
		sql.Uint8: toUint8}
	floatTypes := map[sql.Type]toTypeFunc{
		sql.Float64: toFloat64,
		sql.Float32: toFloat32,
		decimal1616: toDecimal1616,
	}
	expectedConv := map[sql.Type]toTypeFunc{
		sql.Int64: toInt64,
		sql.Int32: toInt64,
		sql.Int24: toInt64,
		sql.Int16: toInt64,
		sql.Int8: toInt64,
		sql.Uint64: toUint64,
		sql.Uint32: toUint64,
		sql.Uint24: toUint64,
		sql.Uint16: toUint64,
		sql.Uint8: toUint64,
		sql.Float64: toFloat64,
		sql.Float32: toFloat64,
		decimal1616: toDecimal1616,
	}

	testCases := []struct {
		name     string
		typeToConv    map[sql.Type]toTypeFunc
		val      float64
		expected float64
		err      error
	}{
		{
			"signed types positive int",
			signedTypes,
			5.0,
			5.0,
			nil,
		},{
			"signed types negative int",
			signedTypes,
			-5.0,
			5.0,
			nil,
		},
		{
			"unsigned types positive int",
			unsignedTypes,
			5.0,
			5.0,
			nil,
		},
		{
			"float positive int",
			floatTypes,
			5.0,
			5.0,
			nil,
		},{
			"float negative int",
			floatTypes,
			-5.0,
			5.0,
			nil,
		},
	}

	newAbsVal := NewUnaryMathFunc("abs", AbsFuncLogic{})
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			for sqlType, conv := range test.typeToConv {
				f := newAbsVal(expression.NewGetField(0, sqlType, "blob", true))

				row := sql.NewRow(conv(test.val))
				res, err := f.Eval(sql.NewEmptyContext(), row)

				if test.err == nil {
					require.NoError(t, err)
					require.Equal(t, expectedConv[sqlType](test.expected), res)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}

func TestRadians(t *testing.T) {
	tests := []struct{
		name string
		input interface{}
		expected float64
	}{
		{"int val of 180", int16(180), math.Pi},
		{"uint val of 90", uint(90), math.Pi / 2.0},
		{"float value of 360", 360.0, 2*math.Pi},
	}

	logic := WrapUnaryMathFloatFuncLogic(RadiansFuncLogic{})
	f := NewUnaryMathFunc("radians", logic)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			radians := f(expression.NewLiteral(test.input, nil))
			res, err := radians.Eval(nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, res)
		})
	}
}

func TestDegrees(t *testing.T) {
	tests := []struct{
		name string
		input interface{}
		expected float64
	}{
		{"float32 pi", float32(math.Pi), 180.0},
		{"decimal 2pi", decimal.NewFromFloat(2*math.Pi), 360.0},
		{"float6h4 pi/2", math.Pi/2.0, 90.0},
	}

	logic := WrapUnaryMathFloatFuncLogic(DegreesFuncLogic{})
	f := NewUnaryMathFunc("degrees", logic)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			degrees := f(expression.NewLiteral(test.input, nil))
			res, err := degrees.Eval(nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, res)
		})
	}
}

func TestTrigFunctions(t *testing.T) {
	sin := NewUnaryMathFunc("sin", WrapUnaryMathFloatFuncLogic(SinFuncLogic{}))
	cos := NewUnaryMathFunc("cos", WrapUnaryMathFloatFuncLogic(CosFuncLogic{}))
	tan := NewUnaryMathFunc("tan", WrapUnaryMathFloatFuncLogic(TanFuncLogic{}))
	asin := NewUnaryMathFunc("asin", WrapUnaryMathFloatFuncLogic(ASinFuncLogic{}))
	acos := NewUnaryMathFunc("acos", WrapUnaryMathFloatFuncLogic(ACosFuncLogic{}))
	atan := NewUnaryMathFunc("atan", WrapUnaryMathFloatFuncLogic(ATanFuncLogic{}))

	const NUM_CHECKS = 24
	delta := (2*math.Pi) / float64(NUM_CHECKS)
	for i := 0; i <= NUM_CHECKS; i++ {
		theta := delta * float64(i)
		thetaLiteral := expression.NewLiteral(theta, nil)
		sinVal, err := sin(thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)
		cosVal, err := cos(thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)
		tanVal, err := tan(thetaLiteral).Eval(nil, nil)
		assert.NoError(t, err)

		assert.Equal(t, math.Sin(theta), sinVal)
		assert.Equal(t, math.Cos(theta), cosVal)
		assert.Equal(t, math.Tan(theta), tanVal)

		sinF, _ := sinVal.(float64)
		cosF, _ := cosVal.(float64)
		tanF, _ := tanVal.(float64)

		asinVal, err := asin(expression.NewLiteral(sinF, nil)).Eval(nil, nil)
		assert.NoError(t, err)
		acosVal, err := acos(expression.NewLiteral(cosF, nil)).Eval(nil, nil)
		assert.NoError(t, err)
		atanVal, err := atan(expression.NewLiteral(tanF, nil)).Eval(nil, nil)
		assert.NoError(t, err)

		assert.Equal(t, math.Asin(sinF), asinVal)
		assert.Equal(t, math.Acos(cosF), acosVal)
		assert.Equal(t, math.Atan(tanF), atanVal)
	}
}
