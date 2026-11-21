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
	"cmp"
	"strings"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql/encodings"
)

// WeightScanner yields the collation weights of a string in comparison order.
//
// Per [Unicode Character Sets], UCA collations ([Collation.IsUCA]) skip runes
// whose weight is [encodings.UCAIgnorableWeight], whereas non-UCA collations
// yield every character's weight including zero.
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
// -1, 0, or 1 without allocating a weight buffer. Both scanners must use the
// same collation.
func (ws *WeightScanner) Compare(other *WeightScanner) int {
	if ws.str == other.str {
		ws.str, other.str = "", ""
		return 0
	}
	if ws.collation.ID == Collation_binary {
		res := strings.Compare(ws.str, other.str)
		ws.str, other.str = "", ""
		return res
	}

	n := runeAlignedPrefixLen(ws.str, other.str)
	ws.str, other.str = ws.str[n:], other.str[n:]

	sorter := ws.collation.Sorter
	for {
		if len(ws.str) > 0 && len(other.str) > 0 {
			lr, lSize := utf8.DecodeRuneInString(ws.str)
			rr, rSize := utf8.DecodeRuneInString(other.str)
			if lr == rr {
				ws.str, other.str = ws.str[lSize:], other.str[rSize:]
				continue
			}
			lw, rw := sorter(lr), sorter(rr)
			if !ws.isIgnorable(lw) && !ws.isIgnorable(rw) {
				ws.str, other.str = ws.str[lSize:], other.str[rSize:]
				if lw != rw {
					return cmp.Compare(lw, rw)
				}
				continue
			}
		}
		// Next skips ignorable runes and reports exhaustion, neither of which
		// the single-rune check above handles.
		lw, lOk := ws.Next()
		rw, rOk := other.Next()
		if !lOk || !rOk {
			return compareRemaining(lOk, rOk)
		}
		if lw != rw {
			return cmp.Compare(lw, rw)
		}
	}
}

// runeAlignedPrefixLen returns the length of the longest common byte prefix of
// |a| and |b|, shortened so it never ends inside a rune.
func runeAlignedPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i+8 <= n && a[i:i+8] == b[i:i+8] {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	if i > 0 && a[i-1] >= utf8.RuneSelf {
		i = runeBoundaryAtOrBefore(a, i)
	}
	return i
}

// runeBoundaryAtOrBefore returns |i|, or the start of the rune that |i| splits,
// so the tails decode the same leading runes a full-string scan would.
func runeBoundaryAtOrBefore(s string, i int) int {
	start := i - 1
	for start > 0 && i-start < utf8.UTFMax && !utf8.RuneStart(s[start]) {
		start--
	}
	if utf8.FullRuneInString(s[start:i]) {
		return i
	}
	return start
}

// compareRemaining orders two scanners by which one still has weights.
func compareRemaining(lMore, rMore bool) int {
	switch {
	case lMore == rMore:
		return 0
	case lMore:
		return 1
	default:
		return -1
	}
}

// Next returns the next collation weight in the scanned string and reports
// whether a weight was produced.
//
// Next returns (0, false) once all bytes are consumed. Invalid UTF-8 bytes
// decode as [utf8.RuneError] and advance one byte so malformed input yields
// deterministic, transitive sort weights instead of failing comparison.
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
		if ws.isIgnorable(w) {
			continue
		}
		return w, true
	}
	return 0, false
}

// isIgnorable reports whether this collation skips runes of weight |w|.
func (ws *WeightScanner) isIgnorable(w int32) bool {
	return ws.collation.IsUCA && w == encodings.UCAIgnorableWeight
}
