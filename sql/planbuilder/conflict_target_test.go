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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConflictTargetMatches verifies order-independent conflict-target matching.
func TestConflictTargetMatches(t *testing.T) {
	require.True(t, conflictTargetMatches([]string{"id"}, []string{"table.id"}))
	require.True(t, conflictTargetMatches([]string{"a", "b"}, []string{"table.b", "table.a"}))
	require.False(t, conflictTargetMatches([]string{"a"}, []string{"table.a", "table.b"}))
	require.False(t, conflictTargetMatches([]string{"missing"}, []string{"table.id"}))
	require.False(t, conflictTargetMatches(nil, []string{"table.id"}))
}
