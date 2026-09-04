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

import "strings"

var collationExpanders = [len(collationArray)]func(r rune) string{}

func init() {
	for id, collation := range collationArray {
		// TODO(#3844): Support multi-level collation weights so
		// case-sensitive and accent-sensitive collations can evaluate
		// expansions without equating distinct case or accent variants.
		if strings.Contains(collation.Name, "_bin") ||
			strings.HasSuffix(collation.Name, "_cs") ||
			strings.HasSuffix(collation.Name, "_as_ci") {
			continue
		}
		if strings.Contains(collation.Name, "_0900_ai_ci") ||
			strings.Contains(collation.Name, "_unicode_520_ci") {
			collationExpanders[id] = uca0900Expander
		} else if strings.Contains(collation.Name, "_unicode_ci") {
			collationExpanders[id] = unicodeExpander
		} else if strings.Contains(collation.Name, "_german2_ci") {
			collationExpanders[id] = german2Expander
		}
	}
	collationExpanders[0] = collationExpanders[Collation_Default]
}

// Expander returns the rune expansion function for the collation, or
// nil if the collation does not expand single characters into
// multi-weight sequences.
//
// Collation expansions allow single characters such as the German
// sharp S ('ß') or ligatures to match their multi-character
// equivalents (such as "ss") under Unicode collation rules.
//
// See Unicode Technical Standard no. 10, Section 3.1.3:
// [UTS 10 Expansions] and the MySQL Collation Implementation:
// [MySQL Collation Effect].
//
// [UTS 10 Expansions]: https://www.unicode.org/reports/tr10/#Expansions
// [MySQL Collation Effect]: https://dev.mysql.com/doc/refman/8.4/en/charset-collation-effect.html
func (c CollationID) Expander() func(r rune) string {
	if int(c) < len(collationExpanders) {
		return collationExpanders[c]
	}
	return nil
}

// unicodeExpander expands runes that map to multiple collation
// elements under standard Unicode Collation Algorithm (UCA)
// collations (such as utf8mb4_unicode_ci).
func unicodeExpander(r rune) string {
	switch r {
	case 'Ǆ', 'ǅ', 'ǆ', 'Ǳ', 'ǲ', 'ǳ':
		return "dz"
	case 'ﬀ':
		return "ff"
	case 'ﬃ':
		return "ffi"
	case 'ﬄ':
		return "ffl"
	case 'ﬁ':
		return "fi"
	case 'ﬂ':
		return "fl"
	case 'Ĳ', 'ĳ':
		return "ij"
	case 'Ǉ', 'ǈ', 'ǉ':
		return "lj"
	case 'Ǌ', 'ǋ', 'ǌ':
		return "nj"
	case 'Œ', 'œ':
		return "oe"
	case 'ß':
		return "ss"
	case 'ﬅ', 'ﬆ':
		return "st"
	case 'ƾ':
		return "ts"
	case 'ƍ':
		return "zw"
	default:
		return ""
	}
}

// uca0900Expander expands runes for Unicode 9.0.0+ collations such
// as utf8mb4_0900_ai_ci and utf8mb4_unicode_520_ci.
//
// Expands Latin digraphs 'æ' to "ae", 'ȸ' to "db", and
// 'ȹ' to "qp".
func uca0900Expander(r rune) string {
	switch r {
	case 'Æ', 'æ', 'Ǣ', 'ǣ', 'Ǽ', 'ǽ':
		return "ae"
	case 'ȸ':
		return "db"
	case 'ȹ':
		return "qp"
	default:
		return unicodeExpander(r)
	}
}

// german2Expander expands runes for German phonebook (DIN 5007-2)
// collations such as utf8mb4_german2_ci.
//
// German phonebook order expands umlauts to their vowel-plus-e
// equivalents ('ä' to "ae", 'ö' to "oe", 'ü' to "ue").
func german2Expander(r rune) string {
	switch r {
	case 'ä', 'Ä':
		return "ae"
	case 'ö', 'Ö':
		return "oe"
	case 'ü', 'Ü':
		return "ue"
	default:
		return unicodeExpander(r)
	}
}
