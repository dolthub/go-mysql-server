// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"math"
	"testing"
)

type test struct {
	name     string
	nType    sql.Type
	row      sql.Row
	expected interface{}
}

func TestOct(t *testing.T) {
	tests := []test{
		// NULL input
		{"n is nil", types.Int32, sql.NewRow(nil), nil},

		// Positive numbers
		{"positive small", types.Int32, sql.NewRow(8), "10"},
		{"positive medium", types.Int32, sql.NewRow(64), "100"},
		{"positive large", types.Int32, sql.NewRow(4095), "7777"},
		{"positive huge", types.Int64, sql.NewRow(123456789), "726746425"},

		// Negative numbers
		{"negative small", types.Int32, sql.NewRow(-8), "1777777777777777777770"},
		{"negative medium", types.Int32, sql.NewRow(-64), "1777777777777777777700"},
		{"negative large", types.Int32, sql.NewRow(-4095), "1777777777777777770001"},

		// Zero
		{"zero", types.Int32, sql.NewRow(0), "0"},

		// String inputs
		{"string number", types.LongText, sql.NewRow("15"), "17"},
		{"alpha string", types.LongText, sql.NewRow("abc"), "0"},
		{"mixed string", types.LongText, sql.NewRow("123abc"), "173"},

		// Edge cases
		{"max int32", types.Int32, sql.NewRow(math.MaxInt32), "17777777777"},
		{"min int32", types.Int32, sql.NewRow(math.MinInt32), "1777777777760000000000"},
		{"max int64", types.Int64, sql.NewRow(math.MaxInt64), "777777777777777777777"},
		{"min int64", types.Int64, sql.NewRow(math.MinInt64), "1000000000000000000000"},

		// Decimal numbers
		{"decimal", types.Float64, sql.NewRow(15.5), "17"},
		{"negative decimal", types.Float64, sql.NewRow(-15.5), "1777777777777777777761"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewOct(expression.NewGetField(0, tt.nType, "n", true))
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if err != nil {
				t.Fatal(err)
			}
			if result != tt.expected {
				t.Errorf("got %v; expected %v", result, tt.expected)
			}
		})
	}
}
