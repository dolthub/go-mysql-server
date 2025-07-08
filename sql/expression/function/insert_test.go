// Copyright 2020-2024 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestInsert(t *testing.T) {
	f := NewInsert(
		expression.NewGetField(0, types.LongText, "", false),
		expression.NewGetField(1, types.Int64, "", false),
		expression.NewGetField(2, types.Int64, "", false),
		expression.NewGetField(3, types.LongText, "", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null str", sql.NewRow(nil, 1, 2, "new"), nil, false},
		{"null pos", sql.NewRow("hello", nil, 2, "new"), nil, false},
		{"null length", sql.NewRow("hello", 1, nil, "new"), nil, false},
		{"null newStr", sql.NewRow("hello", 1, 2, nil), nil, false},
		{"empty string", sql.NewRow("", 1, 2, "new"), "", false},
		{"position is 0", sql.NewRow("hello", 0, 2, "new"), "hello", false},
		{"position is negative", sql.NewRow("hello", -1, 2, "new"), "hello", false},
		{"negative length", sql.NewRow("hello", 1, -1, "new"), "hello", false},
		{"position beyond string length", sql.NewRow("hello", 10, 2, "new"), "hello", false},
		{"normal insertion", sql.NewRow("hello", 2, 2, "xyz"), "hxyzlo", false},
		{"insert at beginning", sql.NewRow("hello", 1, 2, "xyz"), "xyzllo", false},
		{"insert at end", sql.NewRow("hello", 5, 1, "xyz"), "hellxyz", false},
		{"replace entire string", sql.NewRow("hello", 1, 5, "world"), "world", false},
		{"length exceeds string", sql.NewRow("hello", 3, 10, "world"), "heworld", false},
		{"empty replacement", sql.NewRow("hello", 2, 2, ""), "hlo", false},
		{"zero length", sql.NewRow("hello", 3, 0, "xyz"), "hexyzllo", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
