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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceRedaction_DefaultDisabled(t *testing.T) {
	ctx := NewContext(context.Background())
	require.False(t, ctx.TraceRedactionEnabled(), "redaction must be opt-in (default off)")
}

func TestTraceRedaction_WithTraceRedactionTrue_OptsIn(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	require.True(t, ctx.TraceRedactionEnabled(), "WithTraceRedaction(true) should enable redaction")
}

func TestTraceRedaction_WithTraceRedactionFalse_StaysDisabled(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(false))
	require.False(t, ctx.TraceRedactionEnabled(), "WithTraceRedaction(false) keeps the default-off state")
}

func TestTraceRedaction_NilContext(t *testing.T) {
	var ctx *Context
	require.False(t, ctx.TraceRedactionEnabled(), "nil context should report redaction disabled")
	require.Equal(t, "foo", ctx.RedactNameForTrace("foo"), "nil context name redaction should pass through")
	require.Equal(t, "SELECT 1", ctx.RedactQueryForTrace("SELECT 1"), "nil context query redaction should pass through")
}

func TestTraceRedaction_DefaultPassesThrough(t *testing.T) {
	// With redaction off (the default), all helpers return their
	// input unchanged and no mapping is allocated.
	ctx := NewContext(context.Background())
	const q = "SELECT a FROM t WHERE x = 1"
	require.Equal(t, q, ctx.RedactQueryForTrace(q), "default-off RedactQueryForTrace must pass through")
	require.Equal(t, "users", ctx.RedactNameForTrace("users"), "default-off RedactNameForTrace must pass through")
	require.Nil(t, ctx.RedactionMapping(), "default-off context must not allocate a mapping")
}

func TestTraceRedaction_RedactQueryForTracePopulatesMapping(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	got := ctx.RedactQueryForTrace("SELECT a FROM t WHERE x = 1")
	require.NotContains(t, got, "FROM t", "table name not redacted")
	require.NotContains(t, got, "from t ", "table name not redacted (lowercased form)")
	require.NotNil(t, ctx.RedactionMapping(), "mapping must be populated when enabled")
}

func TestTraceRedaction_NameUsesParsedMapping(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	_ = ctx.RedactQueryForTrace("SELECT a FROM users WHERE id = 1")
	tok := ctx.RedactNameForTrace("users")
	require.NotEqual(t, "users", tok, "expected redacted token for 'users', got original")
	require.Equal(t, tok, ctx.RedactNameForTrace("users"), "name redaction must be stable")
}

func TestTraceRedaction_NameWithoutParsePhase(t *testing.T) {
	// rowexec spans may fire on contexts where no SQL was parsed.
	// Name redaction should still mint a stable token.
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	a := ctx.RedactNameForTrace("orphan_table")
	require.NotEqual(t, "orphan_table", a, "expected a redacted token, got original")
	require.Equal(t, a, ctx.RedactNameForTrace("orphan_table"), "token must be stable across calls")
}

type stubStringer string

func (s stubStringer) String() string { return string(s) }

func TestTraceRedaction_RedactStringerForTrace(t *testing.T) {
	enabled := NewContext(context.Background(), WithTraceRedaction(true))
	require.Containsf(t,
		enabled.RedactStringerForTrace(stubStringer("user_col + 5")),
		"redacted",
		"enabled context should mask SQL fragment")

	disabledByDefault := NewContext(context.Background())
	const original = "user_col + 5"
	require.Equal(t, original, disabledByDefault.RedactStringerForTrace(stubStringer(original)),
		"default-off context should return original Stringer text")

	var nilCtx *Context
	require.Equal(t, original, nilCtx.RedactStringerForTrace(stubStringer(original)),
		"nil context should return original Stringer text")
}

func TestTraceRedaction_UnparseableQuery(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	require.Truef(t,
		strings.Contains(ctx.RedactQueryForTrace("not even close to sql ;;"), "unparseable"),
		"expected unparseable marker for invalid SQL")
}
