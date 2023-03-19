// Copyright 2022 Dolthub, Inc.
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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestConv(t *testing.T) {
	testCases := []struct {
		name     string
		nType    sql.Type
		row      sql.Row
		expected interface{}
	}{
		// NULL inputs
		{"n is nil", types.Int32, sql.NewRow(nil, 16, 2), nil},
		{"fromBase is nil", types.LongText, sql.NewRow('a', nil, 2), nil},
		{"toBase is nil", types.LongText, sql.NewRow('a', 16, nil), nil},

		// invalid inputs
		{"invalid N", types.LongText, sql.NewRow("r", 16, 2), "0"},
		{"invalid N for 10 fromBase", types.LongText, sql.NewRow("B11", 10, 10), "0"},
		{"invalid negative N for 10 fromBase", types.LongText, sql.NewRow("-C17", 10, 10), "0"},
		{"invalid N for 16 fromBase", types.LongText, sql.NewRow("LAX", 16, 10), "0"},
		{"bigger N than max uint64 from base 2 to base 16", types.LongText, sql.NewRow("184467440737095485216", 10, -16), "-1"},
		{"bigger N than max uint64 from base 2 to base -16", types.LongText, sql.NewRow("184467440737095485216", 10, 16), "FFFFFFFFFFFFFFFF"},
		{"bigger N than max uint64 from base 2 to base 10", types.LongText, sql.NewRow("111111111111111111111111111111111111111111111111111111111111111111", 2, 10), "18446744073709551615"},
		{"bigger N than max uint64 from base 2 to base -10", types.LongText, sql.NewRow("111111111111111111111111111111111111111111111111111111111111111111", 2, -10), "-1"},
		{"invalid 37 fromBase", types.LongText, sql.NewRow(2, 37, 2), nil},
		{"invalid -1 fromBase", types.LongText, sql.NewRow(2, -1, 2), nil},
		{"invalid 0 fromBase", types.LongText, sql.NewRow(2, 0, 2), nil},
		{"invalid 1 fromBase", types.LongText, sql.NewRow(2, 1, 2), nil},
		{"invalid 37 toBase", types.LongText, sql.NewRow(2, 16, 37), nil},
		{"invalid -1 toBase", types.LongText, sql.NewRow(2, 10, -1), nil},
		{"invalid 0 toBase", types.LongText, sql.NewRow(2, 10, 0), nil},
		{"invalid 1 toBase", types.LongText, sql.NewRow(2, 10, 1), nil},

		// valid inputs
		{"truncate the first convertable subpart for N for 10 fromBase", types.LongText, sql.NewRow("11B", 10, 10), "11"},
		{"truncate the first convertable subpart for N for 16 fromBase", types.LongText, sql.NewRow("BX", 16, 10), "11"},
		{"truncate the first convertable subpart for longer N for 16 fromBase", types.LongText, sql.NewRow("ABCF9XCD", 16, 10), "703737"},
		{"truncate the first convertable subpart for longer N for 2 fromBase", types.LongText, sql.NewRow("18", 2, -16), "1"},
		{"base 16 to base 2", types.LongText, sql.NewRow("a", 16, 2), "1010"},
		{"base 16 to base 2 with positive sign", types.LongText, sql.NewRow("+a", 16, 2), "1010"},
		{"base 2 to base 16", types.LongText, sql.NewRow(1010, 2, 16), "A"},
		{"base 18 to base 8", types.LongText, sql.NewRow("6E", 18, 8), "172"},
		{"base 8 to base 18", types.LongText, sql.NewRow("172", 8, 18), "6E"},
		{"base 10 to base -18", types.LongText, sql.NewRow("-17", 10, -18), "-H"},
		{"base -18 to base -10", types.LongText, sql.NewRow("-H", -18, -10), "-17"},
		{"base -18 to base 10", types.LongText, sql.NewRow("-H", -18, 10), "18446744073709551599"},
		{"base 10 to base 16", types.LongText, sql.NewRow(-17, 10, 16), "FFFFFFFFFFFFFFEF"},
		{"base -10 to base 16", types.LongText, sql.NewRow(-17, -10, 16), "FFFFFFFFFFFFFFEF"},
		{"base 10 to base -16", types.LongText, sql.NewRow(-17, 10, -16), "-11"},
		{"base -10 to base -16", types.LongText, sql.NewRow(-17, -10, -16), "-11"},
		{"negative N for base 16 to base -10", types.LongText, sql.NewRow("-C17", 16, -10), "-3095"},
		{"negative N for base 16 to base 10", types.LongText, sql.NewRow("-C17", 16, 10), "18446744073709548521"},
		{"big N for base 10 to base -16", types.LongText, sql.NewRow("18446744073709548521", 10, -16), "-C17"},
		{"big N for base 10 to base 16", types.LongText, sql.NewRow("18446744073709548521", 10, 16), "FFFFFFFFFFFFF3E9"},
		{"max N for base 10 to base -16", types.LongText, sql.NewRow("18446744073709551615", 10, -16), "-1"},
		{"big N for base 10 to base -16", types.LongText, sql.NewRow("18446744073709551614", 10, -16), "-2"},
		{"n as hex", types.LongText, sql.NewRow(0x0a, 10, 10), "10"},
	}

	for _, tt := range testCases {
		f := NewConv(
			expression.NewGetField(0, tt.nType, "N", false),
			expression.NewGetField(1, types.Int64, "FromBase", false),
			expression.NewGetField(2, types.Int64, "ToBase", false),
		)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, _ := f.Eval(sql.NewEmptyContext(), tt.row)
			require.Equal(tt.expected, result)
		})
	}
}
