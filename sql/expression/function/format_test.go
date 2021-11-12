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
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestFormat(t *testing.T) {
	testCases := []struct {
		name     string
		xType    sql.Type
		dType    sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.Float64, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"float64 without d", sql.Float64, sql.Int32, sql.NewRow(5555.8, nil), nil, nil},
		{"float64 with d", sql.Float64, sql.Int32, sql.NewRow(5555.855, 4), "5,555.8550", nil},
		{"float64 with super big decimal place", sql.Float64, sql.Int32, sql.NewRow(5555.855, 15), "5,555.855000000000000", nil},
		{"float64 with negative d", sql.Float64, sql.Int32, sql.NewRow(5552.855, -1), "5,553", nil},
		{"float64 with float d", sql.Float64, sql.Float64, sql.NewRow(5555.855, float64(2.123)), "5,555.86", nil},
		{"float64 with float negative d", sql.Float64, sql.Float64, sql.NewRow(5552.855, float64(-1)), "5,553", nil},
		{"float64 with blob d", sql.Float64, sql.Blob, sql.NewRow(5555.855, []byte{1, 2, 3}), nil, nil},
		{"float64 with text d", sql.Float64, sql.Text, sql.NewRow(5555.855, "2"), "5,555.86", nil},
		{"negative float64 with d", sql.Float64, sql.Int32, sql.NewRow(-5555.855, 2), "-5,555.86", nil},
		{"blob is nil", sql.Blob, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"blob is ok", sql.Blob, sql.Int32, sql.NewRow([]byte{1, 2, 3}, nil), nil, nil},
		{"text int without d", sql.Text, sql.Int32, sql.NewRow("98765432", nil), nil, nil},
		{"text int with d", sql.Text, sql.Int32, sql.NewRow("98765432", 2), "98,765,432.00", nil},
		{"text int with negative d", sql.Text, sql.Int32, sql.NewRow("98765432", -1), "98,765,432", nil},
		{"text int with float d", sql.Text, sql.Float64, sql.NewRow("98765432", 2.123), "98,765,432.00", nil},
		{"text int with float negative d", sql.Text, sql.Float64, sql.NewRow("98765432", float32(-1)), "98,765,432", nil},
		{"text float without d", sql.Text, sql.Int32, sql.NewRow("98765432.1234", nil), nil, nil},
		{"text float with d", sql.Text, sql.Int32, sql.NewRow("98765432.1234", 2), "98,765,432.12", nil},
		{"text float with negative d", sql.Text, sql.Int32, sql.NewRow("98765432.8234", -1), "98,765,433", nil},
		{"text float with float d", sql.Text, sql.Float64, sql.NewRow("98765432.1234", float64(2.823)), "98,765,432.123", nil},
		{"text float with float negative d", sql.Text, sql.Float64, sql.NewRow("98765432.1234", float64(-1)), "98,765,432", nil},
		{"text float with blob d", sql.Text, sql.Blob, sql.NewRow("98765432.1234", []byte{1, 2, 3}), nil, nil},
		{"negative num text int with d", sql.Text, sql.Int32, sql.NewRow("-98765432", 2), "-98,765,432.00", nil},
		{"sci-notn big num", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 1), "5,932,886.0", nil},
		{"sci-notn big num with big dp", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 8), "5,932,886.00000000", nil},
		{"sci-notn big exp num", sql.Float64, sql.Int32, sql.NewRow(5.932887e+08, 2), "593,288,700.00", nil},
		{"sci-notn neg big exp num", sql.Float64, sql.Int32, sql.NewRow(-5.932887e+08, 2), "-593,288,700.00", nil},
		{"sci-notn text big exp num", sql.Text, sql.Int32, sql.NewRow("5.932887e+07", 3), "59,328,870.000", nil},
		{"sci-notn text neg big exp num", sql.Text, sql.Int32, sql.NewRow("-5.932887e+08", 2), "-593,288,700.00", nil},
		{"sci-notn exp small num", sql.Float64, sql.Int32, sql.NewRow(5.932887e-08, 2), "0.00", nil},
		{"sci-notn exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow(5.932887e-08, 9), "0.000000059", nil},
		{"sci-notn neg exp small num", sql.Float64, sql.Int32, sql.NewRow(-5.932887e-08, 2), "0.00", nil},
		{"sci-notn neg exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow(-5.932887e-08, 8), "-0.00000006", nil},
		{"sci-notn text neg exp small num", sql.Float64, sql.Int32, sql.NewRow("-5.932887e-08", 2), "0.00", nil},
		{"sci-notn text neg exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow("-5.932887e-08", 8), "-0.00000006", nil},
		//{"sci-notn big num with big dp", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 15), "5,932,886.000000000001000", nil},
		//{"sci-notn text big num", sql.Text, sql.Int32, sql.NewRow("5932886+.000000000001", 1), "5,932,886.0", nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 2)
		args[0] = expression.NewGetField(0, tt.xType, "Val", false)
		args[1] = expression.NewGetField(1, tt.dType, "Df", false)
		f, err := NewFormat(args...)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			require.Nil(err)

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
