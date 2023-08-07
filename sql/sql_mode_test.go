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

	"github.com/stretchr/testify/require"
)

func TestSqlMode(t *testing.T) {
	// Test that ANSI mode includes ANSI_QUOTES mode
	sqlMode := NewSqlModeFromString("only_full_group_by,ansi")
	require.True(t, sqlMode.AnsiQuotes())
	require.True(t, sqlMode.ModeEnabled("ansi"))
	require.True(t, sqlMode.ModeEnabled("ANSI"))
	require.True(t, sqlMode.ModeEnabled("ONLY_FULL_GROUP_BY"))
	require.False(t, sqlMode.ModeEnabled("fake_mode"))
	require.True(t, sqlMode.ParserOptions().AnsiQuotes)

	sqlMode = NewSqlModeFromString("AnSi_quotEs")
	require.True(t, sqlMode.AnsiQuotes())
	require.True(t, sqlMode.ModeEnabled("ansi_quotes"))
	require.True(t, sqlMode.ModeEnabled("ANSI_quoTes"))
	require.False(t, sqlMode.ModeEnabled("fake_mode"))
	require.True(t, sqlMode.ParserOptions().AnsiQuotes)

	// Test when SQL_MODE does not include ANSI_QUOTES
	sqlMode = NewSqlModeFromString("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES")
	require.False(t, sqlMode.AnsiQuotes())
	require.True(t, sqlMode.ModeEnabled("STRICT_TRANS_TABLES"))
	require.False(t, sqlMode.ModeEnabled("ansi_quotes"))
	require.False(t, sqlMode.ParserOptions().AnsiQuotes)
}
