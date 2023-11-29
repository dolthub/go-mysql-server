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
	"github.com/dolthub/go-mysql-server/sql/types"
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
		{"Input value is null", types.Float64, sql.NewRow(nil), nil, nil},
		{"Input value is zero", types.Float64, sql.NewRow(0), nil, nil},
		{"Input value is negative", types.Float64, sql.NewRow(-1), nil, nil},
		{"Input value is valid string", types.Float64, sql.NewRow("2"), float64(0.6931471805599453), nil},
		{"Input value is invalid string", types.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", types.Float64, sql.NewRow(3), float64(1.0986122886681096), nil},
		{"Input value is valid float32", types.Float32, sql.NewRow(float32(6)), float64(1.791759469228055), nil},
		{"Input value is valid int64", types.Int64, sql.NewRow(int64(8)), float64(2.0794415416798357), nil},
		{"Input value is valid int32", types.Int32, sql.NewRow(int32(10)), float64(2.302585092994046), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(math.E)(expression.NewGetField(0, tt.rowType, "", true))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else if tt.expected == nil {
				require.NoError(err)
				require.Nil(result)
				require.True(f.IsNullable())
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}
}

func TestLog2(t *testing.T) {
	var testCases = []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"Input value is null", types.Float64, sql.NewRow(nil), nil, nil},
		{"Input value is zero", types.Float64, sql.NewRow(0), nil, nil},
		{"Input value is negative", types.Float64, sql.NewRow(-1), nil, nil},
		{"Input value is valid string", types.Float64, sql.NewRow("2"), float64(1), nil},
		{"Input value is invalid string", types.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", types.Float64, sql.NewRow(3), float64(1.5849625007211563), nil},
		{"Input value is valid float32", types.Float32, sql.NewRow(float32(6)), float64(2.584962500721156), nil},
		{"Input value is valid int64", types.Int64, sql.NewRow(int64(8)), float64(3), nil},
		{"Input value is valid int32", types.Int32, sql.NewRow(int32(10)), float64(3.321928094887362), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(float64(2))(expression.NewGetField(0, tt.rowType, "", true))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else if tt.expected == nil {
				require.NoError(err)
				require.Nil(result)
				require.True(f.IsNullable())
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}
}

func TestLog10(t *testing.T) {
	var testCases = []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"Input value is null", types.Float64, sql.NewRow(0), nil, nil},
		{"Input value is zero", types.Float64, sql.NewRow(0), nil, nil},
		{"Input value is negative", types.Float64, sql.NewRow(-1), nil, nil},
		{"Input value is valid string", types.Float64, sql.NewRow("2"), float64(0.3010299956639812), nil},
		{"Input value is invalid string", types.Float64, sql.NewRow("aaa"), nil, sql.ErrInvalidType},
		{"Input value is valid float64", types.Float64, sql.NewRow(3), float64(0.4771212547196624), nil},
		{"Input value is valid float32", types.Float32, sql.NewRow(float32(6)), float64(0.7781512503836436), nil},
		{"Input value is valid int64", types.Int64, sql.NewRow(int64(8)), float64(0.9030899869919435), nil},
		{"Input value is valid int32", types.Int32, sql.NewRow(int32(10)), float64(1), nil},
	}

	for _, tt := range testCases {
		f := NewLogBaseFunc(float64(10))(expression.NewGetField(0, tt.rowType, "", true))
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else if tt.expected == nil {
				require.NoError(err)
				require.Nil(result)
				require.True(f.IsNullable())
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}
}

func TestLogInvalidArguments(t *testing.T) {
	_, err := NewLog()
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))

	_, err = NewLog(
		expression.NewLiteral(1, types.Float64),
		expression.NewLiteral(1, types.Float64),
		expression.NewLiteral(1, types.Float64),
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
		{"Input base is 1", []sql.Expression{expression.NewLiteral(float64(1), types.Float64), expression.NewLiteral(float64(10), types.Float64)}, nil, nil},
		{"Input base is nil", []sql.Expression{expression.NewLiteral(nil, types.Float64), expression.NewLiteral(float64(10), types.Float64)}, nil, nil},
		{"Input base is zero", []sql.Expression{expression.NewLiteral(float64(0), types.Float64), expression.NewLiteral(float64(10), types.Float64)}, nil, nil},
		{"Input base is negative", []sql.Expression{expression.NewLiteral(float64(-5), types.Float64), expression.NewLiteral(float64(10), types.Float64)}, nil, nil},
		{"Input base is valid string", []sql.Expression{expression.NewLiteral("4", types.LongText), expression.NewLiteral(float64(10), types.Float64)}, float64(1.6609640474436813), nil},
		{"Input base is invalid string", []sql.Expression{expression.NewLiteral("bbb", types.LongText), expression.NewLiteral(float64(10), types.Float64)}, nil, sql.ErrInvalidType},

		{"Input value is null", []sql.Expression{expression.NewLiteral(nil, types.Float64)}, nil, nil},
		{"Input value is zero", []sql.Expression{expression.NewLiteral(float64(0), types.Float64)}, nil, nil},
		{"Input value is negative", []sql.Expression{expression.NewLiteral(float64(-9), types.Float64)}, nil, nil},
		{"Input value is valid string", []sql.Expression{expression.NewLiteral("7", types.LongText)}, float64(1.9459101490553132), nil},
		{"Input value is invalid string", []sql.Expression{expression.NewLiteral("766j", types.LongText)}, nil, sql.ErrInvalidType},

		{"Input base is valid float64", []sql.Expression{expression.NewLiteral(float64(5), types.Float64), expression.NewLiteral(float64(99), types.Float64)}, float64(2.855108491376949), nil},
		{"Input base is valid float32", []sql.Expression{expression.NewLiteral(float32(6), types.Float32), expression.NewLiteral(float64(80), types.Float64)}, float64(2.4456556306420936), nil},
		{"Input base is valid int64", []sql.Expression{expression.NewLiteral(int64(8), types.Int64), expression.NewLiteral(float64(64), types.Float64)}, float64(2), nil},
		{"Input base is valid int32", []sql.Expression{expression.NewLiteral(int32(10), types.Int32), expression.NewLiteral(float64(100), types.Float64)}, float64(2), nil},

		{"Input value is valid float64", []sql.Expression{expression.NewLiteral(float64(5), types.Float64), expression.NewLiteral(float64(66), types.Float64)}, float64(2.6031788549643564), nil},
		{"Input value is valid float32", []sql.Expression{expression.NewLiteral(float32(3), types.Float32), expression.NewLiteral(float64(50), types.Float64)}, float64(3.560876795007312), nil},
		{"Input value is valid int64", []sql.Expression{expression.NewLiteral(int64(5), types.Int64), expression.NewLiteral(float64(77), types.Float64)}, float64(2.698958057527146), nil},
		{"Input value is valid int32", []sql.Expression{expression.NewLiteral(int32(4), types.Int32), expression.NewLiteral(float64(40), types.Float64)}, float64(2.6609640474436813), nil},
	}

	for _, tt := range testCases {
		f, _ := NewLog(tt.input...)
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else if tt.expected == nil {
				require.NoError(err)
				require.Nil(result)
			} else {
				require.NoError(err)
				require.InEpsilonf(tt.expected, result, epsilon, fmt.Sprintf("Actual is: %v", result))
			}
		})
	}
}
