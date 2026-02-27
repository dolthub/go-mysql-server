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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlMode(t *testing.T) {
	// Test that ANSI mode includes ANSI_QUOTES, PIPES_AS_CONCAT, and ONLY_FULL_GROUP_BY mode
	sqlMode := NewSqlModeFromString("ansi")
	assert.True(t, sqlMode.AnsiQuotes())
	assert.True(t, sqlMode.ModeEnabled("ansi"))
	assert.True(t, sqlMode.ModeEnabled("ANSI"))
	assert.False(t, sqlMode.ModeEnabled("fake_mode"))
	assert.True(t, sqlMode.ParserOptions().AnsiQuotes)
	assert.Equal(t, "ANSI", sqlMode.String())
	assert.True(t, sqlMode.PipesAsConcat())   // PIPES_AS_CONCAT is included in ANSI mode
	assert.True(t, sqlMode.OnlyFullGroupBy()) // ONLY_FULL_GROUP_BY is included in ANSI mode
	assert.False(t, sqlMode.ModeEnabled("pipes_as_concat"))

	// Test a mixed case SQL_MODE string where only ANSI_QUOTES is enabled
	sqlMode = NewSqlModeFromString("AnSi_quotEs")
	assert.True(t, sqlMode.AnsiQuotes())
	assert.True(t, sqlMode.ModeEnabled("ansi_quotes"))
	assert.True(t, sqlMode.ModeEnabled("ANSI_quoTes"))
	assert.False(t, sqlMode.ModeEnabled("fake_mode"))
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
	assert.Equal(t, "ONLY_FULL_GROUP_BY,PIPES_AS_CONCAT,STRICT_TRANS_TABLES", sqlMode.String())
}

func TestConvertSqlModeBitmask(t *testing.T) {
	tests := []struct {
		input    any
		expected []string
	}{
		{uint64(1411383296), []string{ErrorForDivisionByZero, NoEngineSubstitution, StrictTransTables}},
		{int64(1411383296), []string{ErrorForDivisionByZero, NoEngineSubstitution, StrictTransTables}},
		{modeStrictTransTables | modeErrorForDivisionByZero | modeNoEngineSubstitution | 0x1, []string{StrictTransTables, ErrorForDivisionByZero, NoEngineSubstitution}},

		{modeRealAsFloat, []string{RealAsFloat}},
		{modePipesAsConcat, []string{PipesAsConcat}},
		{modeAnsiQuotes, []string{ANSIQuotes}},
		{modeIgnoreSpace, []string{IgnoreSpace}},
		{modeOnlyFullGroupBy, []string{OnlyFullGroupBy}},
		{modeNoEngineSubstitution, []string{NoEngineSubstitution}},
		{uint64(modeNoEngineSubstitution), []string{NoEngineSubstitution}},

		{modeAnsiQuotes | modePipesAsConcat, []string{ANSIQuotes, PipesAsConcat}},
		{modeAnsiQuotes | modeIgnoreSpace, []string{ANSIQuotes, IgnoreSpace}},

		{modeRealAsFloat | modePipesAsConcat, []string{RealAsFloat, PipesAsConcat}},
		{modeRealAsFloat | modePipesAsConcat | modeAnsiQuotes, []string{RealAsFloat, PipesAsConcat, ANSIQuotes}},
		{modeRealAsFloat | modePipesAsConcat | modeAnsiQuotes | modeIgnoreSpace, []string{RealAsFloat, PipesAsConcat, ANSIQuotes, IgnoreSpace}},
		{modeAnsiQuotes | modeOnlyFullGroupBy, []string{ANSIQuotes, OnlyFullGroupBy}},
		{modeIgnoreSpace | modeOnlyFullGroupBy, []string{IgnoreSpace, OnlyFullGroupBy}},

		{modeStrictTransTables, []string{StrictTransTables}},
		{modeStrictTransTables | modeAnsiQuotes, []string{StrictTransTables, ANSIQuotes}},
		{modeStrictAllTables, []string{StrictAllTables}},
		{modeNoZeroInDate, []string{NoZeroInDate}},
		{modeAllowInvalidDates, []string{AllowInvalidDates}},
		{modeErrorForDivisionByZero, []string{ErrorForDivisionByZero}},
		{modeNoBackslashEscapes, []string{NoBackslashEscapes}},
		{modeNoAutoValueOnZero, []string{NoAutoValueOnZero}},
		{modeNoUnsignedSubtraction, []string{NoUnsignedSubtraction}},
		{modeNoDirInCreate, []string{NoDirInCreate}},
		{modeHighNotPrecedence, []string{HighNotPrecedence}},
		{modePadCharToFullLength, []string{PadCharToFullLength}},

		{0x10000000, []string{}},
		{modeStrictTransTables | 0x10000000, []string{StrictTransTables}},

		{modeNoEngineSubstitution | modeAnsiQuotes, []string{NoEngineSubstitution, ANSIQuotes}},
		{modeNoEngineSubstitution | modeOnlyFullGroupBy, []string{NoEngineSubstitution, OnlyFullGroupBy}},
		{modeStrictTransTables | modeErrorForDivisionByZero | modeNoEngineSubstitution, []string{StrictTransTables, ErrorForDivisionByZero, NoEngineSubstitution}},
		{modeStrictTransTables | modeNoZeroInDate | modeErrorForDivisionByZero, []string{StrictTransTables, NoZeroInDate, ErrorForDivisionByZero}},

		{uint64(0), []string{}},
		{int(0), []string{}},

		{int8(4), []string{ANSIQuotes}},
		{int16(4), []string{ANSIQuotes}},
		{int32(4), []string{ANSIQuotes}},
		{uint8(4), []string{ANSIQuotes}},
		{uint16(4), []string{ANSIQuotes}},
		{uint32(4), []string{ANSIQuotes}},

		{"TRADITIONAL", []string{"TRADITIONAL"}},
		{"ANSI", []string{"ANSI"}},
		{"STRICT_TRANS_TABLES,NO_ZERO_DATE", []string{"STRICT_TRANS_TABLES,NO_ZERO_DATE"}},
		{"", []string{}},

		{uint64(9999999999), []string{"9999999999"}},
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
