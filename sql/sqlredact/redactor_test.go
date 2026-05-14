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

package sqlredact

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactSQLForTrace_BasicSelect(t *testing.T) {
	sql := "SELECT a, b, c FROM t WHERE x = 1234 AND y = 1234 AND z = 'apple'"
	got, m, err := RedactSQLForTrace(sql)
	require.NoError(t, err)

	// Literal values must be replaced with v-tokens.
	require.Containsf(t, got, ":v1", "expected literal token :v1 in: %q", got)
	// Same value 1234 dedupes — token appears twice in the output.
	require.Equalf(t, 2, strings.Count(got, ":v1"),
		"expected the duplicated 1234 to dedupe to one token (twice in output), got: %q", got)
	// 'apple' is a different value.
	require.Containsf(t, got, "'v2'", "expected 'apple' to map to 'v2' string-literal, got: %q", got)
	// Identifiers come back backtick-quoted.
	require.Containsf(t, got, "`n1`", "expected first identifier as `n1`, got: %q", got)
	require.Lenf(t, m.idents, 7, "expected 7 idents (a,b,c,t,x,y,z), got %v", m.idents)
	require.Lenf(t, m.values, 2, "expected 2 values (1234, apple), got %v", m.values)
}

func TestRedactSQLForTrace_QualifiedNames(t *testing.T) {
	sql := "SELECT u.name, u.email FROM users AS u WHERE u.id = 5"
	got, m, err := RedactSQLForTrace(sql)
	require.NoError(t, err)

	// Same lexeme "u" appears multiple times — must always map to the
	// same token. Pick whichever token "u" got; assert it's repeated.
	uTok := m.idents["u"]
	require.NotEmptyf(t, uTok, "alias 'u' missing from idents map: %v", m.idents)
	// Backtick-quoted form appears 4 times in the input (u.name, u.email,
	// AS u, u.id).
	require.GreaterOrEqualf(t, strings.Count(got, "`"+uTok+"`"), 4,
		"expected `%s` at least 4 times for repeated 'u', got: %q", uTok, got)
	for _, leaky := range []string{"users", "name", "email"} {
		require.NotContainsf(t, got, leaky, "identifier %q leaked: %q", leaky, got)
	}
}

func TestRedactSQLForTrace_DBQualifiedTable(t *testing.T) {
	sql := "SELECT job FROM `prometheus::bfh6nkyxwj7cwf`.`up`"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)

	require.NotContainsf(t, got, "bfh6nkyxwj7cwf", "UID leaked through redaction: %q", got)
	require.NotContainsf(t, got, "prometheus", "datasource type leaked through redaction: %q", got)
	// "up" is short and could appear inside a keyword — sanity check the
	// quoted form specifically.
	require.NotContainsf(t, got, "`up`", "table name leaked: %q", got)
}

func TestRedactSQLForTrace_INTuple(t *testing.T) {
	sql := "SELECT a FROM t WHERE x IN (1, 2, 3)"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)

	// None of the original integers should appear as raw digits (in
	// either spaced or paren-adjacent form).
	for _, lit := range []string{" 1 ", " 2 ", " 3 ", "(1", "(2", "(3"} {
		require.NotContainsf(t, got, lit, "raw integer leaked %q: %q", lit, got)
	}
}

func TestRedactSQLForTrace_LongValueDedupes(t *testing.T) {
	long := strings.Repeat("x", 300)
	sql := "SELECT a FROM t WHERE n = '" + long + "' AND m = '" + long + "'"
	_, m, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.Lenf(t, m.values, 1, "expected long value to dedupe, got: %v", m.values)
}

func TestRedactSQLForTrace_ParseErrorFallsBack(t *testing.T) {
	got, m, err := RedactSQLForTrace("SELECT secret_col FROM secret_tbl WHERE")
	require.Error(t, err)
	require.Equal(t, UnparseableMarker, got)
	require.NotNil(t, m, "mapping must be non-nil even on error")
	require.Empty(t, m.idents)
	require.Empty(t, m.values)
}

func TestRedactSQLForTrace_NonReservedKeywordAsIdentifier(t *testing.T) {
	// `name`, `data`, `user`, `time` are all non-reserved keywords in
	// the vitess grammar — the lexer emits a keyword token type for
	// them, but they are valid as bare column or table names. The
	// redactor must catch them via the identifier set built from the
	// AST.
	cases := []string{
		"SELECT name FROM users",
		"SELECT u.name, u.data FROM users AS u",
		"SELECT * FROM `time`",
		"SELECT user FROM accounts",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		require.NoErrorf(t, err, "error on %q", sql)
		// None of the original identifiers should appear in the
		// backtick-wrapped form — that would be the leak shape.
		for _, leaky := range []string{"`name`", "`data`", "`user`", "`time`", "`users`", "`accounts`"} {
			require.NotContainsf(t, got, leaky, "non-reserved keyword identifier %q leaked from %q: %q", leaky, sql, got)
		}
	}
}

func TestRedactSQLForTrace_Stability(t *testing.T) {
	sql := "SELECT a, b FROM t WHERE x = 1 AND y = 1"
	a, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	b, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.Equalf(t, a, b, "redaction not stable across calls")
}

func TestRedactSQLForTrace_MarginAndInlineCommentsDropped(t *testing.T) {
	cases := []string{
		"/* user_id='alice@example.com' */ SELECT 1 FROM t",
		"SELECT 1 FROM t /* trailing alice */",
		"SELECT /* inline alice */ 1 FROM t",
		"SELECT 1 FROM t -- alice line comment",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		require.NoErrorf(t, err, "error on %q", sql)
		require.NotContainsf(t, got, "alice", "comment leaked from %q: %q", sql, got)
	}
}

func TestRedactSQLForTrace_Insert(t *testing.T) {
	sql := "INSERT INTO users (name, email) VALUES ('alice', 'a@x')"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)

	for _, leaky := range []string{"alice", "a@x", "users", "email"} {
		require.NotContainsf(t, got, leaky, "INSERT leaked %q: %q", leaky, got)
	}
	// `name` is also a non-reserved keyword; sanity check the
	// backtick form specifically since unquoted "name" overlaps with
	// keyword text.
	require.NotContainsf(t, got, "`name`", "column 'name' leaked verbatim: %q", got)
}

func TestRedactSQLForTrace_SelectExprAlias(t *testing.T) {
	sql := "SELECT email AS user_email_addr FROM users"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	for _, leaky := range []string{"user_email_addr", "email", "users"} {
		require.NotContainsf(t, got, leaky, "identifier %q leaked: %q", leaky, got)
	}
}

func TestRedactSQLForTrace_CTEColumnRename(t *testing.T) {
	sql := "WITH x (sensitive_renamed_col) AS (SELECT a FROM t) SELECT * FROM x"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.NotContainsf(t, got, "sensitive_renamed_col", "CTE column rename leaked: %q", got)
}

func TestRedactSQLForTrace_JoinUsingColumns(t *testing.T) {
	sql := "SELECT * FROM a JOIN b USING (sensitive_join_col)"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.NotContainsf(t, got, "sensitive_join_col", "USING column leaked: %q", got)
}

func TestRedactSQLForTrace_LikeAndRegexpPatterns(t *testing.T) {
	cases := []string{
		"SELECT * FROM t WHERE name LIKE '%alice@example.com%'",
		"SELECT * FROM t WHERE name REGEXP '^alice@.*'",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		require.NoErrorf(t, err, "error on %q", sql)
		require.NotContainsf(t, got, "alice", "pattern literal leaked for %q: %q", sql, got)
	}
}

func TestRedactSQLForTrace_TableFunctionLeakedNoMore(t *testing.T) {
	// The earlier AST walker leaked the table-function NAME and
	// alias because TableFuncExpr is a value-receiver SQLNode. The
	// lexer-based redactor doesn't have that gap — both `my_func`
	// and `sub` are ID tokens.
	sql := "SELECT * FROM my_func('arg1', 'arg2') AS sub"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	for _, leaky := range []string{"my_func", "sub", "arg1", "arg2"} {
		require.NotContainsf(t, got, leaky, "identifier or value %q leaked: %q", leaky, got)
	}
}

func TestRedactSQLForTrace_OperatorsPassThrough(t *testing.T) {
	// Multi-char operators (!=, <=, >=, <>, <<, >>, <=>, ->, ->>) come
	// out of the lexer with empty val and a typ ≥ 256. emitStructural
	// must restore them via the symbolOps map.
	cases := map[string]string{
		"SELECT 1 WHERE a != b":  "!=",
		"SELECT 1 WHERE a <> b":  "!=", // <> shares NE token with !=
		"SELECT 1 WHERE a <= b":  "<=",
		"SELECT 1 WHERE a >= b":  ">=",
		"SELECT 1 WHERE a << b":  "<<",
		"SELECT 1 WHERE a >> b":  ">>",
		"SELECT 1 WHERE a <=> b": "<=>",
	}
	for sql, op := range cases {
		got, _, err := RedactSQLForTrace(sql)
		require.NoErrorf(t, err, "error on %q", sql)
		require.Containsf(t, got, op, "operator %q lost from %q -> %q", op, sql, got)
	}
}

func TestRedactSQLForTrace_SubqueryRedacts(t *testing.T) {
	sql := "SELECT * FROM t WHERE id IN (SELECT id FROM secret_table)"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.NotContainsf(t, got, "secret_table", "subquery table name leaked: %q", got)
}

func TestRedactSQLForTrace_UpdateSetClause(t *testing.T) {
	sql := "UPDATE t SET secret_col = 'leaky_value' WHERE c = 1"
	got, _, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	require.NotContainsf(t, got, "secret_col", "UPDATE column name leaked: %q", got)
	require.NotContainsf(t, got, "leaky_value", "UPDATE value leaked: %q", got)
}

func TestRedactSQLForTrace_HexAndBitLiterals(t *testing.T) {
	sql := "SELECT * FROM t WHERE a = 0xCAFE AND b = X'CAFE' AND c = B'10101'"
	got, m, err := RedactSQLForTrace(sql)
	require.NoError(t, err)
	for _, leaky := range []string{"0xCAFE", "CAFE", "10101"} {
		require.NotContainsf(t, got, leaky, "hex/bit literal %q leaked: %q", leaky, got)
	}
	require.GreaterOrEqualf(t, len(m.values), 2, "expected hex/bit literals in mapping, got: %v", m.values)
}

func TestMapping_ConcurrentRedactIsRaceFree(t *testing.T) {
	// Models the pattern under which Mapping is actually used in
	// production: a parent goroutine populated the mapping during
	// parse, then many concurrent rowexec spans read tokens for
	// names that appeared in the SQL plus a few synthetic names that
	// only show up at exec time. Run under `go test -race` to
	// validate the internal mutex.
	m := NewMapping()
	const goroutines = 32
	const iterations = 500
	parsedNames := []string{"users", "orders", "items", "products", "events"}
	for _, n := range parsedNames {
		m.RedactIdent(n)
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Mostly hit pre-populated names (RLock fast path).
				_ = m.RedactIdent(parsedNames[i%len(parsedNames)])
				// Occasionally mint a synthetic name (Lock slow path).
				if i%50 == 0 {
					_ = m.RedactIdent(fmt.Sprintf("synthetic_g%d_i%d", g, i))
				}
				// And a value lookup.
				_ = m.RedactValue("42")
			}
		}(g)
	}
	wg.Wait()

	// Pre-populated identifiers must still map to the same tokens
	// they were assigned at the start (no clobbering by concurrent
	// mints).
	for i, n := range parsedNames {
		want := "n" + strconv.Itoa(i+1)
		require.Equalf(t, want, m.RedactIdent(n), "parsed name %q mint instability", n)
	}
}

func TestMapping_TokensRepeat(t *testing.T) {
	m := NewMapping()
	require.Equal(t, "n1", m.RedactIdent("foo"))
	require.Equal(t, "n1", m.RedactIdent("foo"), "repeat lookup must return same token")
	require.Equal(t, "n2", m.RedactIdent("bar"))
	// Same surface form in the value namespace doesn't collide with
	// the identifier namespace.
	require.Equal(t, "v1", m.RedactValue("foo"))
}

func TestMapping_NilSafe(t *testing.T) {
	var m *Mapping
	require.Equal(t, "foo", m.RedactIdent("foo"), "nil mapping should pass through")
	require.Equal(t, "foo", m.RedactValue("foo"), "nil mapping should pass through value")
}
