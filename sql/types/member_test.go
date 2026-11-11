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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemberPositions_IsIdentity(t *testing.T) {
	from := []string{"a", "b"}
	tests := []struct {
		name     string
		to       []string
		expected bool
	}{
		{name: "same members", to: []string{"a", "b"}, expected: true},
		{name: "member added at the end", to: []string{"a", "b", "c"}, expected: true},
		{name: "members reordered", to: []string{"b", "a"}, expected: false},
		{name: "member added at the start", to: []string{"c", "a", "b"}, expected: false},
		{name: "member removed", to: []string{"a"}, expected: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, newMemberPositions(from, test.to).IsIdentity())
		})
	}
}
