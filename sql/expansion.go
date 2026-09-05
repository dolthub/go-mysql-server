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

var collationExpanders = [len(collationArray)]func(r rune) []rune{}

// unicodeExpander expands runes for standard Unicode Collation Algorithm
// (UCA) collations (such as utf8mb4_unicode_ci and 0900 collations).
func unicodeExpander(r rune) []rune {
	switch r {
	case 'ß':
		return []rune{'s', 's'}
	case 'œ', 'Œ':
		return []rune{'o', 'e'}
	case 'ĳ', 'Ĳ':
		return []rune{'i', 'j'}
	case 'Ǆ', 'ǅ', 'ǆ', 'Ǳ', 'ǲ', 'ǳ':
		return []rune{'d', 'z'}
	case 'Ǉ', 'ǈ', 'ǉ':
		return []rune{'l', 'j'}
	case 'Ǌ', 'ǋ', 'ǌ':
		return []rune{'n', 'j'}
	case 'ﬀ':
		return []rune{'f', 'f'}
	case 'ﬁ':
		return []rune{'f', 'i'}
	case 'ﬂ':
		return []rune{'f', 'l'}
	case 'ﬃ':
		return []rune{'f', 'f', 'i'}
	case 'ﬄ':
		return []rune{'f', 'f', 'l'}
	case 'ﬅ', 'ﬆ':
		return []rune{'s', 't'}
	case '¼':
		return []rune{'1', '/', '4'}
	case '½':
		return []rune{'1', '/', '2'}
	case '¾':
		return []rune{'3', '/', '4'}
	default:
		return nil
	}
}

// german2Expander expands umlauts for German phonebook collations (such
// as utf8mb4_german2_ci) in addition to standard Unicode expansions.
func german2Expander(r rune) []rune {
	switch r {
	case 'ä', 'Ä':
		return []rune{'a', 'e'}
	case 'ö', 'Ö':
		return []rune{'o', 'e'}
	case 'ü', 'Ü':
		return []rune{'u', 'e'}
	default:
		return unicodeExpander(r)
	}
}

func init() {
	for id, collation := range collationArray {
		if strings.Contains(collation.Name, "_unicode_ci") ||
			strings.Contains(collation.Name, "_unicode_520_ci") ||
			strings.Contains(collation.Name, "_0900_") {
			collationExpanders[id] = unicodeExpander
		} else if strings.Contains(collation.Name, "_german2_ci") {
			collationExpanders[id] = german2Expander
		}
	}
	collationExpanders[0] = collationExpanders[Collation_Default]
}

// Expander returns this collation's rune expansion function, if any.
func (c CollationID) Expander() func(r rune) []rune {
	if int(c) < len(collationExpanders) {
		return collationExpanders[c]
	}
	return nil
}
