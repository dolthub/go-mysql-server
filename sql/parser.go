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
	"strings"
	"unicode"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

type Parser interface {
	ParseSingleWithOptions(query string, options ast.ParserOptions) (ast.Statement, error)

	Parse(ctx *Context, query string, multi bool) (ast.Statement, string, string, error)
	ParseWithOptions(query string, delimiter rune, multi bool, options ast.ParserOptions) (ast.Statement, string, string, error)
	ParseOneWithOptions(string, ast.ParserOptions) (ast.Statement, int, error)
}

var _ Parser = &MysqlParser{}

type MysqlParser struct {
}

func (m *MysqlParser) ParseSingleWithOptions(query string, options ast.ParserOptions) (stmt ast.Statement, err error) {
	s := RemoveSpaceAndDelimiter(query, ';')
	stmt, err = ast.ParseWithOptions(s, options)
	return
}

func (m *MysqlParser) Parse(ctx *Context, query string, multi bool) (ast.Statement, string, string, error) {
	return m.ParseWithOptions(query, ';', multi, LoadSqlMode(ctx).ParserOptions())
}

func (m *MysqlParser) ParseWithOptions(query string, delimiter rune, multi bool, options ast.ParserOptions) (stmt ast.Statement, parsed, remainder string, err error) {
	s := RemoveSpaceAndDelimiter(query, delimiter)
	parsed = s

	if !multi {
		stmt, err = ast.ParseWithOptions(s, options)
	} else {
		var ri int
		stmt, ri, err = ast.ParseOneWithOptions(s, options)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = RemoveSpaceAndDelimiter(parsed, delimiter)
			remainder = s[ri:]
		}
	}
	return
}

func (m *MysqlParser) ParseOneWithOptions(s string, options ast.ParserOptions) (ast.Statement, int, error) {
	return ast.ParseOneWithOptions(s, options)
}

// RemoveSpaceAndDelimiter removes space characters and given delimiter characters from the given query.
func RemoveSpaceAndDelimiter(query string, d rune) string {
	query = strings.TrimSpace(query)
	// trim spaces and empty statements
	return strings.TrimRightFunc(query, func(r rune) bool {
		return r == d || unicode.IsSpace(r)
	})
}
