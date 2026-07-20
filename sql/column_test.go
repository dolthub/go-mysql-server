// Copyright 2026 Dolthub, Inc.
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

func TestHiddenSystemColumnName(t *testing.T) {
	require.Equal(t, "!hidden!idx1!0!0", HiddenSystemColumnName("idx1", 0))
	require.Equal(t, "!hidden!idx1!2!0", HiddenSystemColumnName("idx1", 2))
	// Index names are lowercased, matching MySQL's case-insensitive identifier semantics.
	require.Equal(t, "!hidden!idx1!0!0", HiddenSystemColumnName("IDX1", 0))
}

func TestIsHiddenSystemColumnForIndex(t *testing.T) {
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!0!0", "idx1"))
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!2!0", "idx1"))
	// Case-insensitive on both the column name and the index name.
	require.True(t, IsHiddenSystemColumnForIndex("!HIDDEN!IDX1!0!0", "idx1"))
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!0!0", "IDX1"))

	// Must not match a different index's hidden columns, even with a name that's a prefix of
	// this one (e.g. "idx1" vs "idx10")
	require.False(t, IsHiddenSystemColumnForIndex("!hidden!idx2!0!0", "idx1"))
	require.False(t, IsHiddenSystemColumnForIndex("!hidden!idx10!0!0", "idx1"))
	require.False(t, IsHiddenSystemColumnForIndex("c1", "idx1"))
}
