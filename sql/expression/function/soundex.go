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

package function

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Soundex is a function that returns the soundex of a string. Two strings that
// sound almost the same should have identical soundex strings. A standard
// soundex string is four characters long, but the SOUNDEX() function returns
// an arbitrarily long string.
type Soundex struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Soundex)(nil)
var _ sql.CollationCoercible = (*Soundex)(nil)

// NewSoundex creates a new Soundex expression.
func NewSoundex(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Soundex{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (s *Soundex) FunctionName() string {
	return "soundex"
}

// Description implements sql.FunctionExpression
func (s *Soundex) Description() string {
	return "returns the soundex of a string."
}

// Eval implements the Expression interface.
func (s *Soundex) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, _, err = types.LongText.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	// Handle Dolt's TextStorage wrapper that doesn't convert to plain string
	v, err = sql.UnwrapAny(ctx, v)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	var last rune
	for _, c := range v.(string) {
		c = soundexToUpper(c)
		if last == 0 && !soundexIsAlpha(c) {
			continue
		}
		code := s.code(c)
		if last == 0 {
			b.WriteRune(c)
			last = code
			continue
		}
		if code == '0' || code == last {
			continue
		}
		b.WriteRune(code)
		last = code
	}
	if b.Len() == 0 {
		return "0000", nil
	}
	for i := len([]rune(b.String())); i < 4; i++ {
		b.WriteRune('0')
	}
	return b.String(), nil
}

// soundexToUpper mirrors MySQL's soundex_toupper (sql/item_strfunc.cc), which uppercases
// ASCII 'a'-'z' and nothing else. The first letter of the input is emitted verbatim after
// this conversion, so using unicode-wide case folding here changes the returned string:
// SOUNDEX('é') gave 'É000' where MySQL gives 'é000' (dolthub/dolt#11546). It also matters
// for the codes, because Go's case folding maps some non-ASCII letters onto ASCII ones --
// U+017F LATIN SMALL LETTER LONG S becomes 'S' and would then be coded '2' rather than the
// '0' MySQL assigns to anything outside A-Z.
func soundexToUpper(c rune) rune {
	if c >= 'a' && c <= 'z' {
		return c - 'a' + 'A'
	}
	return c
}

// soundexIsAlpha mirrors MySQL's my_uni_isalpha (sql/item_strfunc.cc), which decides what
// SOUNDEX treats as a letter. It is deliberately coarser than unicode.IsLetter: MySQL
// counts every code point at or above U+00C0 as a letter, on the reasoning quoted in its
// own source that "characters between 'z' and U+00C0 are controls and punctuations".
// U+00D7 MULTIPLICATION SIGN and U+00F7 DIVISION SIGN sit above that line, so MySQL keeps
// them as the leading letter where unicode.IsLetter skipped them as garbage.
func soundexIsAlpha(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0xC0
}

func (s *Soundex) code(c rune) rune {
	switch c {
	case 'B', 'F', 'P', 'V':
		return '1'
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return '2'
	case 'D', 'T':
		return '3'
	case 'L':
		return '4'
	case 'M', 'N':
		return '5'
	case 'R':
		return '6'
	}
	return '0'
}

func (s *Soundex) String() string {
	return fmt.Sprintf("%s(%s)", s.FunctionName(), s.Child)
}

// WithChildren implements the Expression interface.
func (s *Soundex) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSoundex(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (s *Soundex) Type(ctx *sql.Context) sql.Type {
	return types.LongText
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Soundex) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}
