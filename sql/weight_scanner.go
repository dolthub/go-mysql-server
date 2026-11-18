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
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql/encodings"
)

// WeightScanner yields the collation weights of a string in comparison order.
//
// Unicode Collation Algorithm (UCA) collations such as utf8mb4_unicode_ci
// ignore ignorable characters in comparisons, whereas non-UCA collations such
// as utf8mb4_general_ci do not, as documented in [Unicode Character Sets].
// Collations with Collation.IsUCA skip runes whose sort weight is
// [encodings.IgnorableWeight], while non-UCA collations yield one weight per
// character without skipping zero weights.
//
// [Unicode Character Sets]: https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html
type WeightScanner struct {
	// collation points to the collation metadata and rune sorter.
	collation *Collation
	// str holds the remaining unconsumed bytes of the input string.
	str string
}

// NewWeightScanner returns a WeightScanner over |str| using |collation|.
func NewWeightScanner(collation CollationID, str string) WeightScanner {
	return WeightScanner{
		collation: &collationArray[collation],
		str:       str,
	}
}

// Compare compares the remaining weights of the scanner and |other|, returning
// -1, 0, or 1. Both scanners must use the same collation.
func (ws *WeightScanner) Compare(other *WeightScanner) int {
	if ws.collation.ID == Collation_binary {
		res := strings.Compare(ws.str, other.str)
		ws.str = ""
		other.str = ""
		return res
	}
	for {
		if len(ws.str) > 0 && len(other.str) > 0 {
			lr, lread := utf8.DecodeRuneInString(ws.str)
			rr, rread := utf8.DecodeRuneInString(other.str)
			if lr == rr {
				ws.str = ws.str[lread:]
				other.str = other.str[rread:]
				continue
			}
			lw := ws.collation.Sorter(lr)
			rw := other.collation.Sorter(rr)
			if !ws.collation.IsUCA || (lw != encodings.IgnorableWeight && rw != encodings.IgnorableWeight) {
				ws.str = ws.str[lread:]
				other.str = other.str[rread:]
				if lw < rw {
					return -1
				}
				if lw > rw {
					return 1
				}
				continue
			}
		}
		lw, lOk := ws.Next()
		rw, rOk := other.Next()
		if !lOk || !rOk {
			if !lOk && !rOk {
				return 0
			}
			if !lOk {
				return -1
			}
			return 1
		}
		if lw < rw {
			return -1
		}
		if lw > rw {
			return 1
		}
	}
}

// Next returns the next collation weight in the scanned string.
//
// Next returns (0, false) once all bytes of the string are consumed. Invalid
// UTF-8 bytes decode as [utf8.RuneError] and advance one byte.
func (ws *WeightScanner) Next() (int32, bool) {
	for len(ws.str) > 0 {
		if ws.collation.ID == Collation_binary {
			b := ws.str[0]
			ws.str = ws.str[1:]
			return int32(b), true
		}
		r, read := utf8.DecodeRuneInString(ws.str)
		ws.str = ws.str[read:]
		w := ws.collation.Sorter(r)
		if ws.collation.IsUCA && w == encodings.IgnorableWeight {
			continue
		}
		return w, true
	}
	return 0, false
}
