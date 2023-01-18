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

package expression

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPatternToRegex(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{`__`, `(?s)^..$`},
		{`_%_`, `(?s)^..*.$`},
		{`%_`, `(?s)^.*.$`},
		{`_%`, `(?s)^..*$`},
		{`a_b`, `(?s)^a.b$`},
		{`a%b`, `(?s)^a.*b$`},
		{`a.%b`, `(?s)^a\..*b$`},
		{`a\%b`, `(?s)^a%b$`},
		{`a\_b`, `(?s)^a_b$`},
		{`a\\b`, `(?s)^a\\b$`},
		{`a\\\_b`, `(?s)^a\\_b$`},
		{`(ab)`, `(?s)^\(ab\)$`},
		{`$`, `(?s)^\$$`},
		{`$$`, `(?s)^\$\$$`},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.out, patternToGoRegex(tt.in))
		})
	}
}

func TestCustomPatternToRegex(t *testing.T) {
	testCases := []struct {
		in, out, escape string
	}{
		{`a%`, `(?s)^%$`, `a`},
		{`a_`, `(?s)^_$`, `a`},
		{`\_`, `(?s)^_$`, `a`},
		{`\_`, `(?s)^_$`, `\`},
		{`a%a%`, `(?s)^%%$`, `a`},
		{`a%a_`, `(?s)^%_$`, `a`},
		{`$%`, `(?s)^%$`, `$`},
		{`$%$%`, `(?s)^%%$`, `$`},
		{`$$`, `(?s)^\$$`, `$`},
		{`$\`, `(?s)^\\$`, `$`},
		{`\$`, `(?s)^\$$`, `$`},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.out, patternToGoRegexWithEscape(tt.in, tt.escape))
		})
	}
}

func TestLike(t *testing.T) {
	testCases := []struct {
		pattern, value, escape string
		ok                     bool
		collation              sql.CollationID
	}{
		{"a__", "abc", "", true, sql.Collation_Default},
		{"a__", "abcd", "", false, sql.Collation_Default},
		{"a%b", "acb", "", true, sql.Collation_Default},
		{"a%b", "acdkeflskjfdklb", "", true, sql.Collation_Default},
		{"a%b", "ab", "", true, sql.Collation_Default},
		{"a%b", "a", "", false, sql.Collation_Default},
		{"a_b", "ab", "", false, sql.Collation_Default},
		{"aa:%", "aa:bb:cc:dd:ee:ff", "", true, sql.Collation_Default},
		{"aa:%", "AA:BB:CC:DD:EE:FF", "", false, sql.Collation_Default},
		{"aa:%", "AA:BB:CC:DD:EE:FF", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AaAbCc", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AaAbBcCbCc", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AbbbbC", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_n%_%z", "aBcDeFgHiJkLmNoPqRsTuVwXyZ", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\%b`, "acb", "", false, sql.Collation_Default},
		{`a\%b`, "a%b", "", true, sql.Collation_Default},
		{`a\%b`, "A%B", "", false, sql.Collation_Default},
		{`a\%b`, "A%B", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a$%b", "acb", "$", false, sql.Collation_Default},
		{"a$%b", "a%b", "$", true, sql.Collation_Default},
		{"a$%b", "A%B", "$", false, sql.Collation_Default},
		{"a$%b", "A%B", "$", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`a`, "a", "", true, sql.Collation_Default},
		{`ab`, "a", "", false, sql.Collation_Default},
		{`a\b`, "a", "", false, sql.Collation_Default},
		{`a\\b`, "a", "", false, sql.Collation_Default},
		{`a\\\b`, "a", "", false, sql.Collation_Default},
		{`a`, "a", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`ab`, "a", "", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\b`, "a", "", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\\b`, "a", "", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\\\b`, "a", "", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`A%%%%`, "abc", "", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`A%%%%bc`, "abc", "", true, sql.Collation_utf8mb4_0900_ai_ci},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%q LIKE %q", tt.value, tt.pattern), func(t *testing.T) {
			var escape sql.Expression
			if tt.escape != "" {
				escape = NewLiteral(tt.escape, types.LongText)
			}
			f := NewLike(
				NewGetField(0, types.CreateText(tt.collation), "", false),
				NewGetField(1, types.CreateText(tt.collation), "", false),
				escape,
			)
			value, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(
				tt.value,
				tt.pattern,
			))
			require.NoError(t, err)
			require.Equal(t, tt.ok, value)
		})
	}
}
