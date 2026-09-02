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

package strings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuote(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		expected string
	}{
		{"plain", "hello", "\"hello\""},
		{"double quote", "a\"b", "\"a\\\"b\""},
		{"backslash", "a\\b", "\"a\\\\b\""},
		{"short escape", "a\nb", "\"a\\nb\""},
		{"vertical tab", "a\u000bb", "\"a\\u000bb\""},
		{"nul", "\u0000", "\"\\u0000\""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, Quote(tc.in))
		})
	}
}
