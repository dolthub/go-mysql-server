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

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/testutils"
)

// TestInstrIssue3650 checks that unwrapping the needle preserves the haystack.
func TestInstrIssue3650(t *testing.T) {
	textType := types.MustCreateString(sqltypes.Text, 100, sql.Collation_utf8mb4_0900_ai_ci)
	f := NewInstr(
		sql.NewEmptyContext(),
		expression.NewGetField(0, textType, "str", true),
		expression.NewGetField(1, textType, "substr", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected int
	}{
		{
			name:     "wrapped substr match",
			row:      sql.NewRow("foobar", testutils.NewMockStringWrapper("bar")),
			expected: 4,
		},
		{
			name: "wrapped substr no match",
			row:  sql.NewRow("foobar", testutils.NewMockStringWrapper("xyz")),
		},
		{
			name:     "wrapped substr case insensitive",
			row:      sql.NewRow("foobar", testutils.NewMockStringWrapper("BAR")),
			expected: 4,
		},
		{
			name:     "both wrapped",
			row:      sql.NewRow(testutils.NewMockStringWrapper("ébar"), testutils.NewMockStringWrapper("BAR")),
			expected: 2,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			v, err := f.Eval(ctx, tt.row)
			require.NoError(err)
			require.Equal(int64(tt.expected), v)
		})
	}
}
