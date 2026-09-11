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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/test"
)

// TestInstrIssue3650 covers the two defects reported in
// dolthub/go-mysql-server#3650:
//  1. INSTR was case-sensitive for nonbinary strings; MySQL's INSTR is
//     case-insensitive unless one argument is a binary string.
//  2. A wrapped substring argument clobbered the haystack, so INSTR
//     returned 1 unconditionally for any StringWrapper needle.
func TestInstrIssue3650(t *testing.T) {
	f := NewInstr(
		sql.NewEmptyContext(),
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "substr", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected int
	}{
		// Defect 1: case-insensitive for nonbinary strings.
		{"case-insensitive needle", sql.NewRow("xyza", "A"), 4},
		{"case-insensitive haystack", sql.NewRow("XYZA", "a"), 4},
		{"case-insensitive mixed", sql.NewRow("Hello World", "world"), 7},
		{"case-insensitive no match", sql.NewRow("abc", "Z"), 0},
		// Defect 2: wrapped substring must NOT overwrite the haystack.
		{"wrapped substr match", sql.NewRow("foobar", test.NewMockStringWrapper("bar")), 4},
		{"wrapped substr no match", sql.NewRow("foobar", test.NewMockStringWrapper("xyz")), 0},
		{"wrapped substr case-insensitive", sql.NewRow("foobar", test.NewMockStringWrapper("BAR")), 4},
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
