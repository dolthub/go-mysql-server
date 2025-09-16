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
