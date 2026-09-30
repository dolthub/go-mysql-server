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

package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlMode(t *testing.T) {
	// Test that ANSI MODE includes ANSI_QUOTES, PIPES_AS_CONCAT, and ONLY_FULL_GROUP_BY MODE
	sqlMode := NewSqlModeFromString("ansi")
	assert.True(t, sqlMode.AnsiQuotes())
	assert.True(t, sqlMode.ModeEnabled("ansi"))
	assert.True(t, sqlMode.ModeEnabled("ANSI"))
	assert.False(t, sqlMode.ModeEnabled("fake_MODE"))
	assert.True(t, sqlMode.ParserOptions().AnsiQuotes)
	assert.Equal(t, "ANSI", sqlMode.String())
	assert.True(t, sqlMode.PipesAsConcat())   // PIPES_AS_CONCAT is included in ANSI MODE
	assert.True(t, sqlMode.OnlyFullGroupBy()) // ONLY_FULL_GROUP_BY is included in ANSI MODE
	assert.False(t, sqlMode.ModeEnabled("pipes_as_concat"))

	// Test a mixed case SQL_MODE string where only ANSI_QUOTES is enabled
	sqlMode = NewSqlModeFromString("AnSi_quotEs")
	assert.True(t, sqlMode.AnsiQuotes())
	assert.True(t, sqlMode.ModeEnabled("ansi_quotes"))
	assert.True(t, sqlMode.ModeEnabled("ANSI_quoTes"))
	assert.False(t, sqlMode.ModeEnabled("fake_MODE"))
	assert.True(t, sqlMode.ParserOptions().AnsiQuotes)
	assert.Equal(t, "ANSI_QUOTES", sqlMode.String())
	assert.False(t, sqlMode.PipesAsConcat())
	assert.False(t, sqlMode.ModeEnabled("pipes_as_concat"))

	// Test when SQL_MODE does not include ANSI_QUOTES, includes PIPES_AS_CONCAT and STRICT_TRANS_TABLES
	sqlMode = NewSqlModeFromString("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,PIPES_AS_CONCAT")
	assert.False(t, sqlMode.AnsiQuotes())
	assert.True(t, sqlMode.ModeEnabled("STRICT_TRANS_TABLES"))
	assert.False(t, sqlMode.ModeEnabled("ansi_quotes"))
	assert.False(t, sqlMode.ParserOptions().AnsiQuotes)
	assert.True(t, sqlMode.PipesAsConcat())
	assert.True(t, sqlMode.ModeEnabled("pipes_as_concat"))
	assert.True(t, sqlMode.Strict())
	// TODO: the order should be PIPES_AS_CONCAT,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES
	// TODO: for whatever reason this is flakey, so test with contains
	res := sqlMode.String()
	assert.True(t, strings.Contains(res, "STRICT_TRANS_TABLES"))
	assert.True(t, strings.Contains(res, "ONLY_FULL_GROUP_BY"))
	assert.True(t, strings.Contains(res, "PIPES_AS_CONCAT"))
}

func TestConvertSqlModeBitmask(t *testing.T) {
	tests := []struct {
		input    any
		expected []string
	}{
		{uint64(1411383296), []string{ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION, STRICT_TRANS_TABLES}},
		{int64(1411383296), []string{ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION, STRICT_TRANS_TABLES}},
		{MODE_STRICT_TRANS_TABLES | MODE_ERROR_FOR_DIVISION_BY_ZERO | MODE_NO_ENGINE_SUBSTITUTION | 0x1, []string{STRICT_TRANS_TABLES, ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION}},

		{MODE_REAL_AS_FLOAT, []string{REAL_AS_FLOAT}},
		{MODE_PIPES_AS_CONCAT, []string{PIPES_AS_CONCAT}},
		{MODE_ANSI_QUOTES, []string{ANSI_QUOTES}},
		{MODE_IGNORE_SPACE, []string{IGNORE_SPACE}},
		{MODE_ONLY_FULL_GROUP_BY, []string{ONLY_FULL_GROUP_BY}},
		{MODE_NO_ENGINE_SUBSTITUTION, []string{NO_ENGINE_SUBSTITUTION}},
		{uint64(MODE_NO_ENGINE_SUBSTITUTION), []string{NO_ENGINE_SUBSTITUTION}},

		{MODE_ANSI_QUOTES | MODE_PIPES_AS_CONCAT, []string{ANSI_QUOTES, PIPES_AS_CONCAT}},
		{MODE_ANSI_QUOTES | MODE_IGNORE_SPACE, []string{ANSI_QUOTES, IGNORE_SPACE}},

		{MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT, []string{REAL_AS_FLOAT, PIPES_AS_CONCAT}},
		{MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT | MODE_ANSI_QUOTES, []string{REAL_AS_FLOAT, PIPES_AS_CONCAT, ANSI_QUOTES}},
		{MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT | MODE_ANSI_QUOTES | MODE_IGNORE_SPACE, []string{REAL_AS_FLOAT, PIPES_AS_CONCAT, ANSI_QUOTES, IGNORE_SPACE}},
		{MODE_ANSI_QUOTES | MODE_ONLY_FULL_GROUP_BY, []string{ANSI_QUOTES, ONLY_FULL_GROUP_BY}},
		{MODE_IGNORE_SPACE | MODE_ONLY_FULL_GROUP_BY, []string{IGNORE_SPACE, ONLY_FULL_GROUP_BY}},

		{MODE_STRICT_TRANS_TABLES, []string{STRICT_TRANS_TABLES}},
		{MODE_STRICT_TRANS_TABLES | MODE_ANSI_QUOTES, []string{STRICT_TRANS_TABLES, ANSI_QUOTES}},
		{MODE_STRICT_ALL_TABLES, []string{STRICT_ALL_TABLES}},
		{MODE_NO_ZERO_IN_DATE, []string{NO_ZERO_IN_DATE}},
		{MODE_ALLOW_INVALID_DATES, []string{ALLOW_INVALID_DATES}},
		{MODE_ERROR_FOR_DIVISION_BY_ZERO, []string{ERROR_FOR_DIVISION_BY_ZERO}},
		{MODE_NO_BACKSLASH_ESCAPES, []string{NO_BACKSLASH_ESCAPES}},
		{MODE_NO_AUTO_VALUE_ON_ZERO, []string{NO_AUTO_VALUE_ON_ZERO}},
		{MODE_NO_UNSIGNED_SUBTRACTION, []string{NO_UNSIGNED_SUBTRACTION}},
		{MODE_NO_DIR_IN_CREATE, []string{NO_DIR_IN_CREATE}},
		{MODE_HIGH_NOT_PRECEDENCE, []string{HIGH_NOT_PRECEDENCE}},
		{MODE_PAD_CHAR_TO_FULL_LENGTH, []string{PAD_CHAR_TO_FULL_LENGTH}},

		{0x10000000, []string{}},
		{MODE_STRICT_TRANS_TABLES | 0x10000000, []string{STRICT_TRANS_TABLES}},

		{MODE_NO_ENGINE_SUBSTITUTION | MODE_ANSI_QUOTES, []string{NO_ENGINE_SUBSTITUTION, ANSI_QUOTES}},
		{MODE_NO_ENGINE_SUBSTITUTION | MODE_ONLY_FULL_GROUP_BY, []string{NO_ENGINE_SUBSTITUTION, ONLY_FULL_GROUP_BY}},
		{MODE_STRICT_TRANS_TABLES | MODE_ERROR_FOR_DIVISION_BY_ZERO | MODE_NO_ENGINE_SUBSTITUTION, []string{STRICT_TRANS_TABLES, ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION}},
		{MODE_STRICT_TRANS_TABLES | MODE_NO_ZERO_IN_DATE | MODE_ERROR_FOR_DIVISION_BY_ZERO, []string{STRICT_TRANS_TABLES, NO_ZERO_IN_DATE, ERROR_FOR_DIVISION_BY_ZERO}},

		{uint64(0), []string{}},
		{int(0), []string{}},

		{int8(4), []string{ANSI_QUOTES}},
		{int16(4), []string{ANSI_QUOTES}},
		{int32(4), []string{ANSI_QUOTES}},
		{uint8(4), []string{ANSI_QUOTES}},
		{uint16(4), []string{ANSI_QUOTES}},
		{uint32(4), []string{ANSI_QUOTES}},

		{"TRADITIONAL", []string{"TRADITIONAL"}},
		{"ANSI", []string{"ANSI"}},
		{"STRICT_TRANS_TABLES,NO_ZERO_DATE", []string{"STRICT_TRANS_TABLES,NO_ZERO_DATE"}},
		{"", []string{}},

		{"not_a_number", []string{"not_a_number"}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T(%v)", tt.input, tt.input), func(t *testing.T) {
			result, err := ConvertSqlModeBitmask(tt.input)
			assert.NoError(t, err)

			if len(tt.expected) == 0 {
				assert.Equal(t, "", result)
			} else {
				for _, exp := range tt.expected {
					assert.Contains(t, result, exp)
				}
			}
		})
	}
}

func BenchmarkNewSqlModeFromString(b *testing.B) {
	sqlStr := "abc,def,hij,1234567890"
	for i := 0; i < b.N; i++ {
		_ = NewSqlModeFromString(sqlStr)
	}
}
