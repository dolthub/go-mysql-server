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
	"strings"
	"unicode"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// storedDefaultRewrites maps a lowercase function name to its stored default
// repair function. It is written only during init.
var storedDefaultRewrites = map[string]func(string) string{}

// RegisterStoredDefaultRewrite sets |rewrite| as the stored default repair for
// the lowercase function |name|, so that stored defaults calling it parse under
// the current grammar. |rewrite| must be idempotent. It panics if |name| is
// already registered, and it must be called from init.
func RegisterStoredDefaultRewrite(name string, rewrite func(string) string) {
	if _, ok := storedDefaultRewrites[name]; ok {
		panic("sql: RegisterStoredDefaultRewrite called twice for " + name)
	}
	storedDefaultRewrites[name] = rewrite
}

// RepairStoredDefault returns |text|, a stored column default, with the stored
// default repair of each function it calls applied. The parser's tokens decide
// what a call is, so a name inside a string literal is never repaired.
func RepairStoredDefault(text string) string {
	var names []string
	tokens := sqlparser.NewStringTokenizer(text)
	var prev []byte
	prevIsWord := false
	for {
		typ, val := tokens.Scan()
		if typ == 0 || typ == sqlparser.LEX_ERROR {
			break
		}
		if typ == '(' && prevIsWord {
			names = append(names, strings.ToLower(string(prev)))
		}
		prevIsWord = isWordToken(typ, val)
		prev = val
	}
	for _, name := range names {
		if repair, ok := storedDefaultRewrites[name]; ok {
			text = repair(text)
		}
	}
	return text
}

// isWordToken returns whether a token of type |typ| with value |val| is a word,
// such as an identifier or a keyword. Literals and punctuation are not words.
func isWordToken(typ int, val []byte) bool {
	if typ < 256 {
		// Punctuation is returned as its byte value.
		return false
	}
	switch typ {
	case sqlparser.STRING, sqlparser.INTEGRAL, sqlparser.FLOAT, sqlparser.HEX,
		sqlparser.HEXNUM, sqlparser.BIT_LITERAL:
		return false
	}
	return len(val) > 0 && (unicode.IsLetter(rune(val[0])) || val[0] == '_')
}
