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
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql/encodings"
)

// ucaCollations records whether each collation ID uses Unicode Collation
// Algorithm ignorable-character rules.
var ucaCollations [len(collationArray)]bool

// WeightScanner yields the collation weights of a string in comparison order.
//
// Unicode Collation Algorithm (UCA) collations assign zero weight to ignorable
// characters such as U+200B ZERO WIDTH SPACE and skip them during string
// comparison and hashing, as documented in [Unicode Character Sets]. Non-UCA
// collations yield one weight per character without skipping zero weights.
//
// [Unicode Character Sets]: https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html
type WeightScanner struct {
	// str holds the remaining unconsumed bytes of the input string.
	str string
	// sorter maps each decoded rune to its collation sort weight.
	sorter CollationSorter
	// isBinary reports whether the input uses raw byte semantics.
	isBinary bool
	// skipIgnorable reports whether zero-weight runes are skipped.
	skipIgnorable bool
}

// NewWeightScanner returns a WeightScanner over |str| using |collation|.
func NewWeightScanner(collation CollationID, str string) WeightScanner {
	return WeightScanner{
		str:           str,
		sorter:        collationArray[collation].Sorter,
		isBinary:      collation == Collation_binary,
		skipIgnorable: ucaCollations[collation],
	}
}

// Next returns the next collation weight in the scanned string.
//
// Next returns (0, false, nil) once all bytes of the string are consumed, or a
// non-nil error if the string contains an invalid UTF-8 byte sequence.
func (ws *WeightScanner) Next() (int32, bool, error) {
	for len(ws.str) > 0 {
		if ws.isBinary {
			b := ws.str[0]
			ws.str = ws.str[1:]
			return int32(b), true, nil
		}
		r, read := utf8.DecodeRuneInString(ws.str)
		if r == utf8.RuneError && read == 1 {
			return 0, false, ErrCollationMalformedString.New("scanning")
		}
		ws.str = ws.str[read:]
		w := ws.sorter(r)
		if ws.skipIgnorable && w == encodings.IgnorableWeight {
			continue
		}
		return w, true, nil
	}
	return 0, false, nil
}
