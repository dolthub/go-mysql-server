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

func TestColSetIterStopsOnBreak(t *testing.T) {
	cols := NewColSet(1, 2, 3)
	var seen []ColumnId
	for _, id := range cols.Iter() {
		seen = append(seen, id)
		if id == 2 {
			break
		}
	}
	require.Equal(t, []ColumnId{1, 2}, seen)
}
