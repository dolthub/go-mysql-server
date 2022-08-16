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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestConv(t *testing.T) {
	testCases := []struct {
		name     string
		nType    sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"n is nil", sql.Int32, sql.NewRow(nil, 16, 2), nil, nil},
		{"fromBase is nil", sql.LongText, sql.NewRow('a', nil, 2), nil, nil},
		{"toBase is nil", sql.LongText, sql.NewRow('a', 16, nil), nil, nil},
		{"invalid n", sql.LongText, sql.NewRow("r", 16, 2), "0", nil},
		{"invalid fromBase", sql.LongText, sql.NewRow(2, 37, 2), nil, nil},
		{"invalid toBase", sql.LongText, sql.NewRow(2, 16, 37), nil, nil},
		{"base 16 to base 2", sql.LongText, sql.NewRow("a", 16, 2), "1010", nil},
		{"base 18 to base 8", sql.LongText, sql.NewRow("6E", 18, 8), "172", nil},
		{"base 10 to base -18", sql.LongText, sql.NewRow("-17", 10, -18), "-H", nil},
		{"n as hex", sql.LongText, sql.NewRow(0x0a, 10, 10), "10", nil},
	}

	for _, tt := range testCases {
		f := NewConv(
			expression.NewGetField(0, tt.nType, "N", false),
			expression.NewGetField(1, sql.Int64, "FromBase", false),
			expression.NewGetField(2, sql.Int64, "ToBase", false),
		)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

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
