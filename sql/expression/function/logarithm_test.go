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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var epsilon = math.Nextafter(1, 2) - 1

func TestLn(t *testing.T) {
	var testCases = []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"Input value is zero", sql.Float64, sql.NewRow(0), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is negative", sql.Float64, sql.NewRow(-1), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is valid string", sql.Float64, sql.NewRow("2"), float64(0.6931471805599453), nil},
		{"Input value is invalid string", sql.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", sql.Float64, sql.NewRow(3), float64(1.0986122886681096), nil},
		{"Input value is valid float32", sql.Float32, sql.NewRow(float32(6)), float64(1.791759469228055), nil},
		{"Input value is valid int64", sql.Int64, sql.NewRow(int64(8)), float64(2.0794415416798357), nil},
		{"Input value is valid int32", sql.Int32, sql.NewRow(int32(10)), float64(2.302585092994046), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(math.E)(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", false))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}

	// Test Nil
	f := NewLogBaseFunc(math.E)(sql.NewEmptyContext(), expression.NewGetField(0, sql.Float64, "", true))
	require := require.New(t)
	result, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(nil))
	require.NoError(err)
	require.Nil(result)
	require.True(f.IsNullable())
}

func TestLog2(t *testing.T) {
	var testCases = []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"Input value is zero", sql.Float64, sql.NewRow(0), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is negative", sql.Float64, sql.NewRow(-1), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is valid string", sql.Float64, sql.NewRow("2"), float64(1), nil},
		{"Input value is invalid string", sql.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", sql.Float64, sql.NewRow(3), float64(1.5849625007211563), nil},
		{"Input value is valid float32", sql.Float32, sql.NewRow(float32(6)), float64(2.584962500721156), nil},
		{"Input value is valid int64", sql.Int64, sql.NewRow(int64(8)), float64(3), nil},
		{"Input value is valid int32", sql.Int32, sql.NewRow(int32(10)), float64(3.321928094887362), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(float64(2))(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", false))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}

	// Test Nil
	f := NewLogBaseFunc(float64(2))(sql.NewEmptyContext(), expression.NewGetField(0, sql.Float64, "", true))
	require := require.New(t)
	result, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(nil))
	require.NoError(err)
	require.Nil(result)
	require.True(f.IsNullable())
}

func TestLog10(t *testing.T) {
	var testCases = []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"Input value is zero", sql.Float64, sql.NewRow(0), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is negative", sql.Float64, sql.NewRow(-1), nil, ErrInvalidArgumentForLogarithm},
		{"Input value is valid string", sql.Float64, sql.NewRow("2"), float64(0.3010299956639812), nil},
		{"Input value is invalid string", sql.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", sql.Float64, sql.NewRow(3), float64(0.4771212547196624), nil},
		{"Input value is valid float32", sql.Float32, sql.NewRow(float32(6)), float64(0.7781512503836436), nil},
		{"Input value is valid int64", sql.Int64, sql.NewRow(int64(8)), float64(0.9030899869919435), nil},
		{"Input value is valid int32", sql.Int32, sql.NewRow(int32(10)), float64(1), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(float64(10))(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", false))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}

	// Test Nil
	f := NewLogBaseFunc(float64(10))(sql.NewEmptyContext(), expression.NewGetField(0, sql.Float64, "", true))
	require := require.New(t)
	result, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(nil))
	require.NoError(err)
	require.Nil(result)
	require.True(f.IsNullable())
}

func TestLogInvalidArguments(t *testing.T) {
	_, err := NewLog(sql.NewEmptyContext())
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))

	_, err = NewLog(sql.NewEmptyContext(),
		expression.NewLiteral(1, sql.Float64),
		expression.NewLiteral(1, sql.Float64),
		expression.NewLiteral(1, sql.Float64),
	)
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))
}

func TestLog(t *testing.T) {
	var testCases = []struct {
		name     string
		input    []sql.Expression
		expected interface{}
		err      *errors.Kind
	}{
		{"Input base is 1", []sql.Expression{expression.NewLiteral(float64(1), sql.Float64), expression.NewLiteral(float64(10), sql.Float64)}, nil, ErrInvalidArgumentForLogarithm},
		{"Input base is zero", []sql.Expression{expression.NewLiteral(float64(0), sql.Float64), expression.NewLiteral(float64(10), sql.Float64)}, nil, ErrInvalidArgumentForLogarithm},
		{"Input base is negative", []sql.Expression{expression.NewLiteral(float64(-5), sql.Float64), expression.NewLiteral(float64(10), sql.Float64)}, nil, ErrInvalidArgumentForLogarithm},
		{"Input base is valid string", []sql.Expression{expression.NewLiteral("4", sql.LongText), expression.NewLiteral(float64(10), sql.Float64)}, float64(1.6609640474436813), nil},
		{"Input base is invalid string", []sql.Expression{expression.NewLiteral("bbb", sql.LongText), expression.NewLiteral(float64(10), sql.Float64)}, nil, sql.ErrInvalidType},

		{"Input value is zero", []sql.Expression{expression.NewLiteral(float64(0), sql.Float64)}, nil, ErrInvalidArgumentForLogarithm},
		{"Input value is negative", []sql.Expression{expression.NewLiteral(float64(-9), sql.Float64)}, nil, ErrInvalidArgumentForLogarithm},
		{"Input value is valid string", []sql.Expression{expression.NewLiteral("7", sql.LongText)}, float64(1.9459101490553132), nil},
		{"Input value is invalid string", []sql.Expression{expression.NewLiteral("766j", sql.LongText)}, nil, sql.ErrInvalidType},

		{"Input base is valid float64", []sql.Expression{expression.NewLiteral(float64(5), sql.Float64), expression.NewLiteral(float64(99), sql.Float64)}, float64(2.855108491376949), nil},
		{"Input base is valid float32", []sql.Expression{expression.NewLiteral(float32(6), sql.Float32), expression.NewLiteral(float64(80), sql.Float64)}, float64(2.4456556306420936), nil},
		{"Input base is valid int64", []sql.Expression{expression.NewLiteral(int64(8), sql.Int64), expression.NewLiteral(float64(64), sql.Float64)}, float64(2), nil},
		{"Input base is valid int32", []sql.Expression{expression.NewLiteral(int32(10), sql.Int32), expression.NewLiteral(float64(100), sql.Float64)}, float64(2), nil},

		{"Input value is valid float64", []sql.Expression{expression.NewLiteral(float64(5), sql.Float64), expression.NewLiteral(float64(66), sql.Float64)}, float64(2.6031788549643564), nil},
		{"Input value is valid float32", []sql.Expression{expression.NewLiteral(float32(3), sql.Float32), expression.NewLiteral(float64(50), sql.Float64)}, float64(3.560876795007312), nil},
		{"Input value is valid int64", []sql.Expression{expression.NewLiteral(int64(5), sql.Int64), expression.NewLiteral(float64(77), sql.Float64)}, float64(2.698958057527146), nil},
		{"Input value is valid int32", []sql.Expression{expression.NewLiteral(int32(4), sql.Int32), expression.NewLiteral(float64(40), sql.Float64)}, float64(2.6609640474436813), nil},
	}

	for _, tt := range testCases {
		f, _ := NewLog(sql.NewEmptyContext(), tt.input...)
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}

	// Test Nil
	f, _ := NewLog(sql.NewEmptyContext(), expression.NewLiteral(nil, sql.Float64))
	require := require.New(t)
	result, err := f.Eval(sql.NewEmptyContext(), nil)
	require.NoError(err)
	require.Nil(result)
	require.True(f.IsNullable())
}
