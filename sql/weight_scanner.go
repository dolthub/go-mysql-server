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
	"math"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	// ignorableWeight is the zero sort weight assigned to ignorable code points.
	ignorableWeight int32 = 0

	// ucaSortLength is the sort length value shared by UCA 4.0.0 and 5.2.0
	// collations.
	ucaSortLength = 8

	// mongolianVowelSeparator is U+180E, which is ignorable in UCA 4.0.0 and
	// non-ignorable in UCA 5.2.0.
	mongolianVowelSeparator rune = '\u180e'
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
		if ws.skipIgnorable && w == ignorableWeight {
			continue
		}
		return w, true, nil
	}
	return 0, false, nil
}

// configureUCACollation initializes UCA ignorable-character metadata and
// sorter wrapping for |id| when |id| uses UCA rules.
func configureUCACollation(id CollationID) {
	c := &collationArray[id]
	if c.Sorter == nil || c.IsBinary {
		return
	}
	if strings.Contains(c.Name, "_0900_") {
		ucaCollations[id] = true
		return
	}
	if c.SortLength != ucaSortLength {
		return
	}
	ucaCollations[id] = true
	switch {
	case strings.HasSuffix(c.Name, "_unicode_520_ci"):
		c.Sorter = ucaSorter(c.Sorter, isUCA520Ignorable)
	case strings.HasSuffix(c.Name, "_persian_ci"):
		c.Sorter = ucaSorter(c.Sorter, isUCA400PersianIgnorable)
	case strings.HasSuffix(c.Name, "_sinhala_ci"):
		c.Sorter = ucaSorter(c.Sorter, isUCA400SinhalaIgnorable)
	default:
		c.Sorter = ucaSorter(c.Sorter, isUCA400Ignorable)
	}
}

// ucaSorter returns a CollationSorter that maps runes matched by |isIgnorable|
// to ignorableWeight and shifts non-ignorable weights from |base| above zero.
func ucaSorter(base CollationSorter, isIgnorable func(rune) bool) CollationSorter {
	return func(r rune) int32 {
		if (r < ' ' || r > '~') && isIgnorable(r) {
			return ignorableWeight
		}
		w := base(r)
		if w < math.MaxInt32 {
			return w + 1
		}
		return w
	}
}

// isUCA520Ignorable reports whether |r| has zero weight under UCA 5.2.0.
func isUCA520Ignorable(r rune) bool {
	if r == mongolianVowelSeparator {
		return false
	}
	return isUCA400Ignorable(r) || inRuneRanges(r, uca520ExtraIgnorableRanges)
}

// isUCA400PersianIgnorable reports whether |r| has zero weight under the
// Persian UCA 4.0.0 tailoring.
func isUCA400PersianIgnorable(r rune) bool {
	return isUCA400Ignorable(r) && !inRuneRanges(r, uca400PersianNonIgnorableRanges)
}

// isUCA400SinhalaIgnorable reports whether |r| has zero weight under the
// Sinhala UCA 4.0.0 tailoring.
func isUCA400SinhalaIgnorable(r rune) bool {
	return isUCA400Ignorable(r) && !inRuneRanges(r, uca400SinhalaNonIgnorableRanges)
}

// isUCA400Ignorable reports whether |r| has zero weight under default UCA 4.0.0
// rules.
func isUCA400Ignorable(r rune) bool {
	return inRuneRanges(r, uca400IgnorableRanges)
}

// inRuneRanges reports whether |r| lies within any sorted inclusive interval in
// |ranges|.
func inRuneRanges(r rune, ranges [][2]rune) bool {
	_, ok := slices.BinarySearchFunc(ranges, r, func(iv [2]rune, target rune) int {
		if target < iv[0] {
			return 1
		}
		if target > iv[1] {
			return -1
		}
		return 0
	})
	return ok
}

// uca400IgnorableRanges holds the sorted inclusive code-point intervals that
// produce empty weight strings under default UCA 4.0.0 collations.
var uca400IgnorableRanges = [][2]rune{
	{'\u0000', '\u0008'}, {'\u000e', '\u001f'}, {'\u007f', '\u0084'}, {'\u0086', '\u009f'},
	{'\u0300', '\u0357'}, {'\u035d', '\u0362'}, {'\u0483', '\u0486'}, {'\u0488', '\u0489'},
	{'\u0591', '\u05a1'}, {'\u05a3', '\u05b9'}, {'\u05bb', '\u05bd'}, {'\u05bf', '\u05bf'},
	{'\u05c1', '\u05c2'}, {'\u05c4', '\u05c4'}, {'\u0600', '\u0603'}, {'\u0610', '\u0615'},
	{'\u064b', '\u0658'}, {'\u0670', '\u0670'}, {'\u06d6', '\u06e4'}, {'\u06e7', '\u06e8'},
	{'\u06ea', '\u06ed'}, {'\u070f', '\u070f'}, {'\u0711', '\u0711'}, {'\u0730', '\u074a'},
	{'\u0901', '\u0903'}, {'\u093c', '\u093c'}, {'\u0951', '\u0954'}, {'\u0981', '\u0983'},
	{'\u09bc', '\u09bc'}, {'\u0a01', '\u0a03'}, {'\u0a3c', '\u0a3c'}, {'\u0a70', '\u0a71'},
	{'\u0a81', '\u0a83'}, {'\u0abc', '\u0abc'}, {'\u0b01', '\u0b03'}, {'\u0b3c', '\u0b3c'},
	{'\u0b82', '\u0b82'}, {'\u0c01', '\u0c03'}, {'\u0c82', '\u0c83'}, {'\u0cbc', '\u0cbc'},
	{'\u0d02', '\u0d03'}, {'\u0d82', '\u0d83'}, {'\u0e47', '\u0e4b'}, {'\u0e4e', '\u0e4e'},
	{'\u0ec8', '\u0ecb'}, {'\u0f18', '\u0f19'}, {'\u0f35', '\u0f35'}, {'\u0f37', '\u0f37'},
	{'\u0f39', '\u0f39'}, {'\u0f7e', '\u0f7f'}, {'\u0f82', '\u0f83'}, {'\u0f86', '\u0f87'},
	{'\u0fc6', '\u0fc6'}, {'\u1036', '\u1038'}, {'\u17c6', '\u17d1'}, {'\u17d3', '\u17d3'},
	{'\u17dd', '\u17dd'}, {'\u180b', '\u180e'}, {'\u1939', '\u193b'}, {'\u200b', '\u200f'},
	{'\u202a', '\u202e'}, {'\u2060', '\u2063'}, {'\u206a', '\u206f'}, {'\u20d0', '\u20ea'},
	{'\u302a', '\u302f'}, {'\u3099', '\u309a'}, {'\ufb1e', '\ufb1e'}, {'\ufc5e', '\ufc63'},
	{'\ufcf2', '\ufcf4'}, {'\ufe00', '\ufe0f'}, {'\ufe20', '\ufe23'}, {'\ufe70', '\ufe74'},
	{'\ufe76', '\ufe7f'}, {'\ufeff', '\ufeff'}, {'\uff9e', '\uff9f'}, {'\ufff9', '\ufffb'},
}

// uca400PersianNonIgnorableRanges holds the UCA 4.0.0 ignorable code-point
// intervals given primary weights by the Persian tailoring.
var uca400PersianNonIgnorableRanges = [][2]rune{
	{'\u064b', '\u0650'}, {'\u0653', '\u0655'}, {'\u0670', '\u0670'}, {'\ufe70', '\ufe72'},
	{'\ufe74', '\ufe74'}, {'\ufe76', '\ufe7b'},
}

// uca400SinhalaNonIgnorableRanges holds the UCA 4.0.0 ignorable code-point
// intervals given primary weights by the Sinhala tailoring.
var uca400SinhalaNonIgnorableRanges = [][2]rune{
	{'\u0d82', '\u0d83'},
}

// uca520ExtraIgnorableRanges holds the additional sorted inclusive code-point
// intervals that become ignorable in UCA 5.2.0 beyond UCA 4.0.0.
var uca520ExtraIgnorableRanges = [][2]rune{
	{'\u0358', '\u035c'}, {'\u0487', '\u0487'}, {'\u05a2', '\u05a2'}, {'\u05ba', '\u05ba'},
	{'\u05c5', '\u05c5'}, {'\u05c7', '\u05c7'}, {'\u0616', '\u061a'}, {'\u0659', '\u065e'},
	{'\u07eb', '\u07f3'}, {'\u0818', '\u0819'}, {'\u081c', '\u082d'}, {'\u0900', '\u0900'},
	{'\u0e4c', '\u0e4d'}, {'\u0ecc', '\u0ecd'}, {'\u108d', '\u108d'}, {'\u135f', '\u135f'},
	{'\u1a74', '\u1a7c'}, {'\u1a7f', '\u1a7f'}, {'\u1b00', '\u1b04'}, {'\u1b34', '\u1b34'},
	{'\u1b6b', '\u1b73'}, {'\u1b80', '\u1b82'}, {'\u1c37', '\u1c37'}, {'\u1cd0', '\u1ce8'},
	{'\u1ced', '\u1ced'}, {'\u1cf2', '\u1cf2'}, {'\u1dc0', '\u1dc9'}, {'\u1dcb', '\u1dd1'},
	{'\u1dfd', '\u1dff'}, {'\u2064', '\u2064'}, {'\u20eb', '\u20f0'}, {'\u2cef', '\u2cf1'},
	{'\ua66f', '\ua672'}, {'\ua67c', '\ua67d'}, {'\ua6f0', '\ua6f1'}, {'\ua80b', '\ua80b'},
	{'\ua880', '\ua881'}, {'\ua8e0', '\ua8f1'}, {'\ua92b', '\ua92d'}, {'\ua980', '\ua983'},
	{'\ua9b3', '\ua9b3'}, {'\uaabf', '\uaabf'}, {'\uaac1', '\uaac1'}, {'\uabec', '\uabec'},
	{'\ufe24', '\ufe26'}, {'\U000101fd', '\U000101fd'}, {'\U00010a0d', '\U00010a0f'},
	{'\U00010a38', '\U00010a3a'}, {'\U00011080', '\U00011082'}, {'\U000110ba', '\U000110ba'},
	{'\U000110bd', '\U000110bd'}, {'\U0001d165', '\U0001d169'}, {'\U0001d16d', '\U0001d182'},
	{'\U0001d185', '\U0001d18b'}, {'\U0001d1aa', '\U0001d1ad'}, {'\U0001d242', '\U0001d244'},
	{'\U000e0001', '\U000e0001'}, {'\U000e0020', '\U000e007f'}, {'\U000e0100', '\U000e01ef'},
}
