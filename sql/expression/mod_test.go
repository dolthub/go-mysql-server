// Copyright 2023 Dolthub, Inc.
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

package expression

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestMod(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right int64
		expected    string
		null        bool
	}{
		{"1 % 1", 1, 1, "0", false},
		{"8 % 3", 8, 3, "2", false},
		{"1 % 3", 1, 3, "1", false},
		{"0 % -1024", 0, -1024, "0", false},
		{"-1 % 2", -1, 2, "-1", false},
		{"1 % -2", 1, -2, "1", false},
		{"-1 % -2", -1, -2, "-1", false},
		{"1 % 0", 1, 0, "0", true},
		{"0 % 0", 0, 0, "0", true},
		{"0.5 % 0.24", 0, 0, "0.02", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMod(
				NewLiteral(tt.left, types.Int64),
				NewLiteral(tt.right, types.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			if tt.null {
				require.Nil(result)
			} else {
				r, ok := result.(decimal.Decimal)
				require.True(ok)
				require.Equal(tt.expected, r.StringFixed(r.Exponent()*-1))
			}
		})
	}
}
