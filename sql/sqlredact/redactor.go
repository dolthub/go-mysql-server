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

// Package sqlredact rewrites SQL queries into a form safe to attach to
// trace span attributes. Identifiers (table, column, schema, alias
// names) and literal values are replaced with stable, low-entropy
// tokens (n1, n2, ..., v1, v2, ...) that repeat across queries of the
// same shape so storage compresses well.
//
// The implementation combines two passes over the same SQL:
//
//  1. Parse to AST, then walk the AST to collect every TableIdent
//     and ColIdent string. The grammar is the authority on which
//     lexemes are identifiers — this is necessary because vitess
//     treats hundreds of words (e.g. NAME, USER, DATA, STATUS) as
//     "non-reserved keywords" that the lexer emits with a keyword
//     token type, even though they may appear in identifier
//     positions in real queries.
//
//  2. Lex the SQL and emit each token. ID tokens redact as
//     identifiers; literal tokens (STRING/INTEGRAL/FLOAT/HEX/HEXNUM/
//     BIT_LITERAL) redact as values; COMMENT tokens drop;
//     placeholder tokens (VALUE_ARG/LIST_ARG) pass through; any
//     other token (keyword, punctuation, multi-character operator)
//     emits structurally — UNLESS its val is in the identifier set
//     from step (1), in which case it redacts as an identifier
//     (catches the "non-reserved keyword used as a column name"
//     case).
//
// Coverage is bounded by the grammar's notion of identifier rather
// than by an evolving list of AST node fields: any future AST shape
// that holds a TableIdent or ColIdent gets covered automatically as
// long as it participates in Walk. Combined with the closed set of
// lexer literal-token types, the redactor cannot silently leak
// customer data through new grammar additions.
//
// On parse failure the redacted string is UnparseableMarker. We
// could fall back to lex-only redaction in that case but it would
// leak non-reserved keyword identifiers, so we prefer to publish
// nothing rather than leak partial data.
//
// This is distinct from sqlparser.Normalize / RedactSQLQuery, which
// is a query-plan-cache primitive that parameterizes literals only,
// dedupes only inside SELECT, and skips long values for CPU. The
// redactor here treats the trace surface as a privacy boundary:
// every high-cardinality token is rewritten, always dedupes, drops
// comments, and falls back to UnparseableMarker on parse errors.
package sqlredact

import (
	"errors"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// UnparseableMarker is the value substituted when the input cannot be
// parsed (or lexed). Callers should treat any returned redacted
// string as opaque — never re-parse it or display it as a "fixed"
// version of the user's SQL.
const UnparseableMarker = "<unparseable>"

// ErrLexFailed is returned when the lexer hits an irrecoverable error
// before reaching EOF.
var ErrLexFailed = errors.New("sqlredact: lex failed")

// RedactSQLForTrace tokenizes sql and returns a copy in which every
// high-cardinality token (identifiers and literals) has been replaced
// by a stable token in the returned Mapping. Comments are dropped.
// Keywords, punctuation, and multi-character operators pass through.
//
// The Mapping records the substitutions so callers can redact
// related downstream attributes (resolved table names emitted from
// rowexec spans, etc.) consistently.
//
// On parse or lex failure the redacted string is UnparseableMarker,
// the Mapping is empty, and a non-nil error is returned. Callers
// should always use the returned redacted string (never the input)
// when publishing to traces, regardless of error.
func RedactSQLForTrace(sql string) (string, *Mapping, error) {
	m := NewMapping()
	out, err := RedactSQLForTraceInto(sql, m)
	return out, m, err
}

// RedactSQLForTraceInto is the same as RedactSQLForTrace but writes
// substitutions into the caller-provided Mapping rather than
// allocating a new one. Used by sql.Context's redact helpers so that
// the same Mapping pointer is shared across all in-flight callers
// (parent span, datasource.resolved events, GMS planbuilder spans,
// rowexec spans) — the underlying maps' internal mutex covers the
// concurrent reads.
//
// Pre-existing entries in m are preserved. Tokens already present
// for a given original lexeme are reused; new lexemes mint fresh
// tokens with counters continuing from m's current state.
func RedactSQLForTraceInto(sql string, m *Mapping) (string, error) {
	if m == nil {
		m = NewMapping()
	}
	stmt, parseErr := sqlparser.Parse(sql)
	if parseErr != nil {
		return UnparseableMarker, parseErr
	}
	identSet := collectIdents(stmt)

	var out strings.Builder
	out.Grow(len(sql))

	tk := sqlparser.NewStringTokenizer(sql)
	first := true
	for {
		typ, val := tk.Scan()
		if typ == 0 {
			break
		}
		if typ == sqlparser.LEX_ERROR {
			return UnparseableMarker, ErrLexFailed
		}
		if typ == sqlparser.COMMENT {
			continue
		}
		if !first {
			out.WriteByte(' ')
		}
		first = false
		emitToken(&out, typ, val, m, identSet)
	}
	return out.String(), nil
}

// collectIdents walks the AST and collects every TableIdent and
// ColIdent value. These are the only identifier types in the vitess
// AST — every place the grammar accepts a user-supplied name (table,
// column, alias, schema, CTE column rename, USING list, etc.) ends
// up with one of these two types in the resulting AST.
func collectIdents(stmt sqlparser.Statement) map[string]struct{} {
	idents := map[string]struct{}{}
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		switch v := n.(type) {
		case sqlparser.TableIdent:
			if !v.IsEmpty() {
				idents[v.String()] = struct{}{}
			}
		case sqlparser.ColIdent:
			if !v.IsEmpty() {
				idents[v.String()] = struct{}{}
			}
		}
		return true, nil
	}, stmt)
	return idents
}

// emitToken writes a single redacted token to out. The branching
// matches the categorization of token types into:
//   - identifier (ID)
//   - literal value (STRING, INTEGRAL, FLOAT, HEX, HEXNUM, BIT_LITERAL)
//   - already-bound placeholder (VALUE_ARG, LIST_ARG)
//   - structural (everything else: keyword, punctuation, multi-char op)
//
// Keywords whose val matches a name in identSet are treated as
// identifiers — that's how non-reserved-keyword column names get
// covered.
func emitToken(out *strings.Builder, typ int, val []byte, m *Mapping, identSet map[string]struct{}) {
	switch typ {
	case sqlparser.ID:
		emitIdent(out, val, m)
	case sqlparser.STRING:
		out.WriteByte('\'')
		out.WriteString(m.RedactValue(string(val)))
		out.WriteByte('\'')
	case sqlparser.INTEGRAL, sqlparser.FLOAT, sqlparser.HEXNUM:
		out.WriteByte(':')
		out.WriteString(m.RedactValue(string(val)))
	case sqlparser.HEX:
		out.WriteString("X'")
		out.WriteString(m.RedactValue(string(val)))
		out.WriteByte('\'')
	case sqlparser.BIT_LITERAL:
		out.WriteString("B'")
		out.WriteString(m.RedactValue(string(val)))
		out.WriteByte('\'')
	case sqlparser.VALUE_ARG, sqlparser.LIST_ARG:
		out.Write(val)
	default:
		if len(val) > 0 {
			if _, ok := identSet[string(val)]; ok {
				// Non-reserved keyword used as an identifier.
				emitIdent(out, val, m)
				return
			}
		}
		emitStructural(out, typ, val)
	}
}

// emitIdent writes a backtick-quoted redacted identifier.
func emitIdent(out *strings.Builder, val []byte, m *Mapping) {
	out.WriteByte('`')
	out.WriteString(m.RedactIdent(string(val)))
	out.WriteByte('`')
}

// emitStructural writes a non-redacted token (keyword, punctuation,
// or multi-character operator). These are bounded by the SQL grammar
// and contain no customer data.
func emitStructural(out *strings.Builder, typ int, val []byte) {
	if len(val) > 0 {
		// Keyword text from the lexer (e.g. "SELECT", "AND", "true").
		out.Write(val)
		return
	}
	if typ < 256 {
		// Single-character operator (e.g. '=', '(', ',').
		out.WriteByte(byte(typ))
		return
	}
	// Multi-character symbol operator emitted with empty val.
	if s, ok := symbolOps[typ]; ok {
		out.WriteString(s)
		return
	}
	// Unknown structural token — fall back to a separator. This
	// branch is only reachable for typ ≥ 256 with empty val that we
	// don't recognize; a leak is impossible because val is empty.
	out.WriteByte(' ')
}

// symbolOps maps the multi-character symbol-operator token types
// emitted by the vitess lexer with empty val back to their textual
// representation. Sourced from token.go's Scan implementation.
var symbolOps = map[int]string{
	sqlparser.LE:                      "<=",
	sqlparser.GE:                      ">=",
	sqlparser.NE:                      "!=",
	sqlparser.SHIFT_LEFT:              "<<",
	sqlparser.SHIFT_RIGHT:             ">>",
	sqlparser.NULL_SAFE_EQUAL:         "<=>",
	sqlparser.JSON_EXTRACT_OP:         "->",
	sqlparser.JSON_UNQUOTE_EXTRACT_OP: "->>",
	// AND/OR also reach this branch when the input used the symbolic
	// `&&` / `||` forms; when the input used the keyword spellings,
	// val carries the keyword text and the symbolOps lookup is
	// bypassed by emitStructural's len(val)>0 guard.
	sqlparser.AND:    "&&",
	sqlparser.OR:     "||",
	sqlparser.CONCAT: "||",
}
