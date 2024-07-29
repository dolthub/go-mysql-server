// Copyright 2024 Dolthub, Inc.
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
	trace2 "runtime/trace"
	"strings"
	"unicode"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// GlobalParser is a temporary variable to expose Doltgres parser.
// It defaults to MysqlParser.
var GlobalParser Parser = NewMysqlParser()

type Parser interface {
	// ParseSimple takes only query string and returns the parsed statement.
	ParseSimple(query string) (ast.Statement, error)
	// Parse parses using default parser options of the ctx and returns the parsed statement
	// along with the query string and remainder string if it's multiple queries.
	Parse(ctx *Context, query string, multi bool) (ast.Statement, string, string, error)
	// ParseWithOptions parses using given parser options and returns the parsed statement
	// along with the query string and remainder string if it's multiple queries.
	ParseWithOptions(ctx context.Context, query string, delimiter rune, multi bool, options ast.ParserOptions) (ast.Statement, string, string, error)
	// ParseOneWithOptions parses the first query using specified parsing returns the parsed statement along with
	// the index of the start of the next query.
	ParseOneWithOptions(context.Context, string, ast.ParserOptions) (ast.Statement, int, error)
}

var _ Parser = &MysqlParser{}

// MysqlParser is a mysql syntax parser used as parser in the engine for Dolt.
type MysqlParser struct{}

// NewMysqlParser creates new MysqlParser
func NewMysqlParser() *MysqlParser {
	return &MysqlParser{}
}

// ParseSimple implements Parser interface.
func (m *MysqlParser) ParseSimple(query string) (ast.Statement, error) {
	return ast.Parse(query)
}

// Parse implements Parser interface.
func (m *MysqlParser) Parse(ctx *Context, query string, multi bool) (ast.Statement, string, string, error) {
	defer trace2.StartRegion(ctx, "Parse").End()
	return m.ParseWithOptions(ctx, query, ';', multi, LoadSqlMode(ctx).ParserOptions())
}

// ParseWithOptions implements Parser interface.
func (m *MysqlParser) ParseWithOptions(ctx context.Context, query string, delimiter rune, multi bool, options ast.ParserOptions) (stmt ast.Statement, parsed, remainder string, err error) {
	s := RemoveSpaceAndDelimiter(query, delimiter)
	parsed = s

	if !multi {
		stmt, err = ast.ParseWithOptions(ctx, s, options)
	} else {
		var ri int
		stmt, ri, err = ast.ParseOneWithOptions(ctx, s, options)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = RemoveSpaceAndDelimiter(parsed, delimiter)
			remainder = s[ri:]
		}
	}
	return
}

// ParseOneWithOptions implements Parser interface.
func (m *MysqlParser) ParseOneWithOptions(ctx context.Context, s string, options ast.ParserOptions) (ast.Statement, int, error) {
	return ast.ParseOneWithOptions(ctx, s, options)
}

// RemoveSpaceAndDelimiter removes space characters and given delimiter characters from the given query.
func RemoveSpaceAndDelimiter(query string, d rune) string {
	query = strings.TrimSpace(query)
	// trim spaces and empty statements
	return strings.TrimRightFunc(query, func(r rune) bool {
		return r == d || unicode.IsSpace(r)
	})
}
