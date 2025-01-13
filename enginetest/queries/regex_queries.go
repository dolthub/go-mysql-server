// Copyright 2023 Dolthub, Inc.
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

//go:build !race

// Running tests with `-race` will cause issues with our regex implementation. Memory usage skyrockets, and execution
// speed grinds to a halt as the pagefile/swap gets involved. Therefore, we do not run any regex tests when using the
// `-race` flag.

package queries

import (
	"gopkg.in/src-d/go-errors.v1"

	regex "github.com/dolthub/go-icu-regex"

	"github.com/dolthub/go-mysql-server/sql"
)

type RegexTest struct {
	Query       string
	Expected    []sql.UntypedSqlRow
	ExpectedErr *errors.Kind
}

var RegexTests = []RegexTest{
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING');",
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING', 'c');",
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING', 'i');",
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING', 'ci');",
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING', 'ic');",
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING' COLLATE utf8mb4_0900_ai_ci);",
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    "SELECT REGEXP_LIKE('testing', 'TESTING' COLLATE utf8mb4_0900_as_cs);",
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query: "SELECT REGEXP_LIKE('testing' COLLATE utf8mb4_0900_ai_ci, 'TESTING') FROM mytable;",
		Expected: []sql.UntypedSqlRow{
			{1},
			{1},
			{1},
		},
	},
	{
		Query: "SELECT i, s, REGEXP_LIKE(s, '[a-z]+d row') FROM mytable;",
		Expected: []sql.UntypedSqlRow{
			{1, "first row", 0},
			{2, "second row", 1},
			{3, "third row", 1},
		},
	},
	{
		Query:    `SELECT REGEXP_REPLACE("0123456789", "[0-4]", "X")`,
		Expected: []sql.UntypedSqlRow{{"XXXXX56789"}},
	},
	{
		Query:    `SELECT REGEXP_REPLACE("0123456789", "[0-4]", "X", 2)`,
		Expected: []sql.UntypedSqlRow{{"0XXXX56789"}},
	},
	{
		Query:    `SELECT REGEXP_REPLACE("0123456789", "[0-4]", "X", 2, 2)`,
		Expected: []sql.UntypedSqlRow{{"01X3456789"}},
	},
	{
		Query:    `SELECT REGEXP_REPLACE("TEST test TEST", "[a-z]", "X", 1, 0, "i")`,
		Expected: []sql.UntypedSqlRow{{"XXXX XXXX XXXX"}},
	},
	{
		Query:    `SELECT REGEXP_REPLACE("TEST test TEST", "[a-z]", "X", 1, 0, "c")`,
		Expected: []sql.UntypedSqlRow{{"TEST XXXX TEST"}},
	},
	{
		Query:    `SELECT REGEXP_REPLACE(CONCAT("abc123"), "[0-4]", "X")`,
		Expected: []sql.UntypedSqlRow{{"abcXXX"}},
	},
	{
		Query: `SELECT * FROM mytable WHERE s LIKE REGEXP_REPLACE("123456%r1o2w", "[0-9]", "")`,
		Expected: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: `SELECT REGEXP_REPLACE(s, "[a-z]", "X") from mytable`,
		Expected: []sql.UntypedSqlRow{
			{"XXXXX XXX"},
			{"XXXXXX XXX"},
			{"XXXXX XXX"},
		},
	},
	{
		Query:    `SELECT 20 REGEXP '^[-]?2[0-9]+$'`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT 30 REGEXP '^[-]?2[0-9]+$'`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(?P<foo_123");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(?P<1>a)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(?P<!>a)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(?P<foo!>a)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aa", "(?P<foo_123>a)(?P=foo_123");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aa", "(?P<foo_123>a)(?P=1)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aa", "(?P<foo_123>a)(?P=!)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aa", "(?P<foo_123>a)(?P=foo_124");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("a", "(?P<foo_123>a)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aa", "(?P<foo_123>a)(?P=foo_123)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("a", "\\1");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("\1", "[\\1]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("a", "\\141");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("\000", "\000");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\000", "[\000a]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\000", "[a\000]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\000", "[^a\000]");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\a\b\f\n\r\t\v", "\a[\b]\f\n\r\t\v");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\a\b\f\n\r\t\v", "[\a][\b][\f][\n][\r][\t][\v]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "\u");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\377", "\xff");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\377", "\x00ffffffffffffff");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\017", "\x00f");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\376", "\x00fe");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("SRC=eval.c g.c blah blah blah \\\\\n\tapes.c", "^\w+=(\\[\000-\277]|[^\n\\\\])*");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("acb", "a.b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\nb", "a.b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("acc\nccb", "a.*b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("acc\nccb", "a.{4,5}b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\rb", "a.b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\nb", "(?s)a.b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("acc\nccb", "(?s)a.*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("acc\nccb", "(?s)a.{4,5}b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\nb", "(?s)a.b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("", ")");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xbc", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axc", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abx", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xabcy", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ababc", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abq", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcc", "^abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabc", "abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axc", "a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axyzc", "a.*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axyzd", "a.*c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aac", "a[b-d]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "a[-b]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "a[\\-b]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a[]b");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a[");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a\\");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "abc)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(abc");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]", "a]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]b", "a[]]b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]b", "a[\\]]b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aed", "a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adc", "a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-c", "a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]c", "a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adc", "a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "\\ba\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("-a", "\\ba\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("-a-", "\\ba\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xy", "\\by\\b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("yz", "\\by\\b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "\\by\\b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "x\\b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "x\\B");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "\\Bz");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "z\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "\\Bx");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "\\Ba\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("-a", "\\Ba\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("-a-", "\\Ba\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xy", "\\By\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("yz", "\\By\\B");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xy", "\\By\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("yz", "\\by\\B");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xyz", "\\By\\B");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("def", "()ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("b", "$b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a(b", "a\\(b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a((b", "a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\\b", "a\\\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "((a))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(a)b(c)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabbabc", "a+b+c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)?");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", ")(");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("cde", "[^ab]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "a*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("e", "a|b|c|d|e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ef", "(a|b|c|d|e)f");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcdefg", "abcd*efg");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xabyabbbz", "ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xayabbbz", "ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("hij", "[abhgefdc]ij");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "^(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcdef", "(abc|)ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "(a|b)c*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(ab|ab*)bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a([bc]*)c*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]*)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]+)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]*)(c+d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adcdcde", "a[bcd]*dcdcde");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adcdcde", "a[bcd]+dcdcde");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(ab|a)b*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "((a)(b)c)(d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("alpha", "[a-zA-Z_][a-zA-Z0-9_]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abh", "^a(bc+|b[eh])g|.h$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("effgz", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ij", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("effg", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("bcdd", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("reffgz", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a", "(((((((((a)))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("uh-uh", "multiple words of text");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("multiple words, yeah", "multiple words");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "(.*)c(.*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("(a, b)", "\\((.*), (.*)\\)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "[k]");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ac", "a[-]?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "(abc)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "([a-c]*)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "^(.+)?B");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaaaa", "(a+).\\1$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaaa", "^(a+).\\1$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "(abc)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "([a-c]+)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aa", "(a)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aa", "(a+)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aa", "(a+)+\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aba", "(a).+\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aba", "(a)ba*\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaa", "(aa|a)a\\1$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaa", "(a|aa)a\\1$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaa", "(a+)a\\1$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "([abc]*)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a)(b)c|ab");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aaax", "(a)+x");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aacx", "([ac])+x");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("d:msgs/tdir/sub1/trial/away.cpp", "([^/]*/)*sub1/");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("track1.title:TBlah blah blah", "([^.]*)\\.([^:]*):[T ]+(.*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abNNxyzN", "([^N]*N)+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abNNxyz", "([^N]*N)+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcx", "([abc]*)x");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "([abc]*)x");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcx", "([xyz]*)x");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aac", "(a)+b|aac");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("aaaa", "(?P<i d>aaa)a");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aaaa", "(?P<id>aaa)a");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aaaa", "(?P<id>aa)(?P=id)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("aaaa", "(?P<id>aa)(?P=xd)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("a", "\\1");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("abcdefghijklk9", "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)\\\119");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xbc", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axc", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abx", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xabcy", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ababc", "abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab{0,}bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abq", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abq", "ab{1,}bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab+bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab{1,}bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab{1,3}bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab{3,4}bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab{4,5}bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab{0,1}bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbbc", "ab?bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab{0,1}c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcc", "^abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabc", "^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabc", "abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axc", "a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axyzc", "a.*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("axyzd", "a.*c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aac", "a[b-d]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "a[-b]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-", "a[b-]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a[b-a]");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a[]b");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a[");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]", "a]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]b", "a[]]b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aed", "a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abd", "a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adc", "a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a-c", "a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a]c", "a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adc", "a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("def", "()ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "*a");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(*)b");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("b", "$b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a\\");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("a(b", "a\\(b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a((b", "a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\\b", "a\\\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "abc)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(abc");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "((a))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(a)b(c)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabbabc", "a+b+c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aabbabc", "a{1,}b{1,}c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "a**");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "a.+?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b){0,}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b){1,}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b)?");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "(a+|b){0,1}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", ")(");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("cde", "[^ab]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "a*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abbbcd", "([abc])*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "([abc])*bcd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("e", "a|b|c|d|e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ef", "(a|b|c|d|e)f");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcdefg", "abcd*efg");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xabyabbbz", "ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xayabbbz", "ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("hij", "[abhgefdc]ij");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "^(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcdef", "(abc|)ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "(a|b)c*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(ab|ab*)bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "a([bc]*)c*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]*)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]+)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "a([bc]*)(c+d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adcdcde", "a[bcd]*dcdcde");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("adcdcde", "a[bcd]+dcdcde");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "(ab|a)b*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcd", "((a)(b)c)(d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("alpha", "[a-zA-Z_][a-zA-Z0-9_]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abh", "^a(bc+|b[eh])g|.h$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("effgz", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ij", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("effg", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("bcdd", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("reffgz", "(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a", "((((((((((a))))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("aa", "((((((((((a))))))))))\\10");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "((((((((((a))))))))))\\41");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(?i)((((((((((a))))))))))\\41");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("a", "(((((((((a)))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("uh-uh", "multiple words of text");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("multiple words, yeah", "multiple words");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcde", "(.*)c(.*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("(a, b)", "\\((.*), (.*)\\)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "[k]");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ac", "a[-]?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "(abc)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcabc", "([a-c]*)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("XBC", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AXC", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABX", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("XABCY", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABABC", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBC", "(?i)ab*bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab*?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab{0,}?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBC", "(?i)ab+?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABQ", "(?i)ab+bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABQ", "(?i)ab{1,}bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab+bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab{1,}?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab{1,3}?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab{3,4}?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab{4,5}?bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBC", "(?i)ab??bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab??bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab{0,1}?bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBBC", "(?i)ab??bc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab??c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab{0,1}?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)^abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCC", "(?i)^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCC", "(?i)^abc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AABC", "(?i)^abc$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AABC", "(?i)abc$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)^");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AXC", "(?i)a.c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AXYZC", "(?i)a.*?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AXYZD", "(?i)a.*c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABD", "(?i)a[bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABD", "(?i)a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ACE", "(?i)a[b-d]e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AAC", "(?i)a[b-d]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A-", "(?i)a[-b]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A-", "(?i)a[b-]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)a[b-a]");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)a[]b");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)a[");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("A]", "(?i)a]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A]B", "(?i)a[]]b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AED", "(?i)a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABD", "(?i)a[^bc]d");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ADC", "(?i)a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A-C", "(?i)a[^-b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A]C", "(?i)a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ADC", "(?i)a[^]b]c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)ab|cd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("DEF", "(?i)()ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)*a");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)(*)b");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("B", "(?i)$b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)a\\");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("A(B", "(?i)a\\(b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A((B", "(?i)a\\(*b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A\\B", "(?i)a\\\\b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)abc)");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)(abc");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)((a))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)(a)b(c)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AABBABC", "(?i)a+b+c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AABBABC", "(?i)a{1,}b{1,}c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i)a**");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCABC", "(?i)a.+?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCABC", "(?i)a.*?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCABC", "(?i)a.{0,5}?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b)*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b){0,}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b)+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b){1,}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b)?");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b){0,1}");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)(a+|b){0,1}?");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("-", "(?i))(");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("CDE", "(?i)[^ab]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "(?i)abc");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "(?i)a*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABBBCD", "(?i)([abc])*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)([abc])*bcd");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("E", "(?i)a|b|c|d|e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("EF", "(?i)(a|b|c|d|e)f");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCDEFG", "(?i)abcd*efg");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("XABYABBBZ", "(?i)ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("XAYABBBZ", "(?i)ab*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCDE", "(?i)(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("HIJ", "(?i)[abhgefdc]ij");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCDE", "(?i)^(ab|cd)e");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCDEF", "(?i)(abc|)ef");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)(a|b)c*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)(ab|ab*)bc");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)a([bc]*)c*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)a([bc]*)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)a([bc]+)(c*d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)a([bc]*)(c+d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ADCDCDE", "(?i)a[bcd]*dcdcde");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ADCDCDE", "(?i)a[bcd]+dcdcde");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABC", "(?i)(ab|a)b*c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCD", "(?i)((a)(b)c)(d)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ALPHA", "(?i)[a-zA-Z_][a-zA-Z0-9_]*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABH", "(?i)^a(bc+|b[eh])g|.h$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("EFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("IJ", "(?i)(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("EFFG", "(?i)(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("BCDD", "(?i)(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("REFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A", "(?i)((((((((((a))))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AA", "(?i)((((((((((a))))))))))\\10");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A", "(?i)(((((((((a)))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("A", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a))))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("C", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a|b|c))))))))))");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("UH-UH", "(?i)multiple words of text");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("MULTIPLE WORDS, YEAH", "(?i)multiple words");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCDE", "(?i)(.*)c(.*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("(A, B)", "(?i)\\((.*), (.*)\\)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "(?i)[k]");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AC", "(?i)a[-]?c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCABC", "(?i)(abc)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ABCABC", "(?i)([a-c]*)\\1");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abad", "a(?!b).");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abad", "a(?=d).");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abad", "a(?=c|d).");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a(?:b|c|d)(.)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a(?:b|c|d)*(.)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a(?:b|c|d)+?(.)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ace", "a(?:b|(c|e){1,2}?|d)+?(.)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("AB", "^(.+)?B");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a:bc-:de:f", "(?<!-):(.*?)(?<!-):");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a:bc\\:de:f", "(?<!\\\\):(.*?)(?<!\\\\):");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a'bc?'de'f", "(?<!\\?)'(.*?)(?<!\\?)'");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("w", "w(?# comment");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("wxyz", "w(?# comment 1)xy(?# comment 2)z");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("W", "(?i)w");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\nb", "a.b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a\nb", "(?s)a.b");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("--ab_cd0123--", "\\w+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("--ab_cd0123--", "[\\w]+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("1234abc5678", "\\D+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("1234abc5678", "[\\D]+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("123abc", "[\\da-fA-F]+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("-", "[\\d-x]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE(" testing!1972", "([\s]*)([\S]*)([\s]*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE(" testing!1972", "(\s*)(\S*)(\s*)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\377", "\xff");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\377", "\x00ff");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\t\n\v\r\f\a", "\t\n\v\r\f\a");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("\t\n\v\r\f\b", "[\t][\n][\v][\r][\f][\b]");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("smil", "(([a-z]+):)?([a-z]+)$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "((.)\1+)");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc\nabd", ".*d");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:       `SELECT REGEXP_LIKE("", "(");`,
		ExpectedErr: regex.ErrInvalidRegex,
	},
	{
		Query:    `SELECT REGEXP_LIKE("!", "[\41]");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("x", "(x?)?");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("foo", "(?x) foo ");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abcdefdof", "(?<!abc)(d.f)");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("laser_beam", "[\w-]+");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xx:", ".*?\S *:");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a   10", "a[ ]*?\ (\d+).*");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a    10", "a[ ]*?\ (\d+).*");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("xx\nx\n", "(?ms).*?x\s*\Z(.*)");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("MMM", "(?i)M+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("MMM", "(?i)m+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("MMM", "(?i)[M]+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("MMM", "(?i)[m]+");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("", "^*");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("one\ntwo\nthree\n", "^.*?$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("a>b", "a[^>]*?b");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("foo", "^a*?$");`,
		Expected: []sql.UntypedSqlRow{{0}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("ab", "^((a)c)?(ab)$");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^([ab]*?)(?=(b)?)c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^([ab]*?)(?!(b))c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
	{
		Query:    `SELECT REGEXP_LIKE("abc", "^([ab]*?)(?<!(a))c");`,
		Expected: []sql.UntypedSqlRow{{1}},
	},
}
