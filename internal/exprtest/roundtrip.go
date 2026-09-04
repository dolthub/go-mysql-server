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

package exprtest

import (
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
)

// AssertStringRoundTrip verifies that an expression string parses as exactly one SELECT expression and that the
// parser assigns the complete input to that expression. Comparison ignores keyword casing and insignificant spaces.
func AssertStringRoundTrip(t testing.TB, expected string) {
	t.Helper()
	statement, err := sqlparser.Parse("SELECT " + expected)
	require.NoError(t, err)

	selectStatement, ok := statement.(*sqlparser.Select)
	require.Truef(t, ok, "expected SELECT statement, found %T", statement)
	require.Len(t, selectStatement.SelectExprs, 1)

	parsed := formatSelectExpression(selectStatement.SelectExprs[0])
	require.Equal(t, normalize(expected), normalize(parsed), "parsed expression: %s", parsed)
}

var asKeyword = regexp.MustCompile(`(?i)\s+as\s+`)

func normalize(expression string) string {
	expression = asKeyword.ReplaceAllString(expression, " ")
	var result []rune
	var stringQuote rune
	var previous rune
	for _, r := range expression {
		if stringQuote != 0 {
			// Vitess escapes double quotes when it formats a single-quoted string. Both spellings have the same value.
			if r == '"' && previous == '\\' && len(result) > 0 {
				result = result[:len(result)-1]
			}
			result = append(result, r)
			if r == stringQuote && previous != '\\' {
				stringQuote = 0
			}
			previous = r
			continue
		}
		switch {
		case r == '\'' || r == '"':
			stringQuote = r
			result = append(result, r)
		case r == '`' || unicode.IsSpace(r):
			// Identifier quoting and whitespace are formatting choices and do not change the parsed expression.
		default:
			result = append(result, unicode.ToLower(r))
		}
		previous = r
	}
	return string(result)
}

func formatSelectExpression(expression sqlparser.SelectExpr) string {
	aliased, ok := expression.(*sqlparser.AliasedExpr)
	if !ok {
		return sqlparser.String(expression)
	}
	groupConcat, ok := aliased.Expr.(*sqlparser.GroupConcatExpr)
	if !ok || groupConcat.Separator.DefaultSeparator {
		return sqlparser.String(expression)
	}

	copy := *groupConcat
	copy.Separator.SeparatorString = strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(copy.Separator.SeparatorString)
	return sqlparser.String(&copy)
}
