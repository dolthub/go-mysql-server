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

// WeightScanner iterates over the collation weights of a string under
// a specific collation.
//
// Single characters that expand to multiple collation weights (such
// as 'ß' expanding to "ss") yield their constituent weights
// sequentially, matching the weight-iteration model used by UCA
// collation scanners.
//
// See Unicode Technical Standard no. 10:
// [UTS 10 Collation Algorithm] and MySQL's Collation Architecture:
// [MySQL Collation Architecture].
//
// [UTS 10 Collation Algorithm]: https://www.unicode.org/reports/tr10/#Weight_Level_Defn
// [MySQL Collation Architecture]: https://dev.mysql.com/doc/refman/8.4/en/charset-collation-effect.html
type WeightScanner struct {
	encoder       encodings.Encoder
	getRuneWeight CollationSorter
	expander      func(r rune) string
	str           string
	expanded      [4]rune
	expandedLen   uint8
	expandedIdx   uint8
}

// NewWeightScanner constructs a [WeightScanner] for the given
// collation and string.
//
// The scanner decodes characters using the collation's character set
// encoder, looks up primary weights with the collation's sorter, and
// checks the collation's expansion table for multi-weight expansions.
func NewWeightScanner(collation CollationID, str string) WeightScanner {
	return WeightScanner{
		encoder:       collation.CharacterSet().Encoder(),
		getRuneWeight: collationArray[collation].Sorter,
		expander:      collation.Expander(),
		str:           str,
	}
}

// Next yields the next collation weight from the input stream.
//
// It returns (weight, true, nil) for each weight, (0, false, nil)
// once the string has been fully consumed, or (0, false, err) if an
// invalid byte sequence is encountered.
func (ws *WeightScanner) Next() (weight int32, ok bool, err error) {
	if ws.expandedIdx < ws.expandedLen {
		w := ws.getRuneWeight(ws.expanded[ws.expandedIdx])
		ws.expandedIdx++
		return w, true, nil
	}
	ws.expandedLen = 0
	ws.expandedIdx = 0

	if len(ws.str) == 0 {
		return 0, false, nil
	}

	var r rune
	var read int
	if ws.encoder != nil {
		r, read = ws.encoder.NextRune(ws.str)
	} else {
		r, read = utf8.DecodeRuneInString(ws.str)
	}
	if read == 0 || (r == utf8.RuneError && read == 1) {
		return 0, false, ErrCollationMalformedString.New("scanning")
	}
	ws.str = ws.str[read:]

	// TODO(#3844): Support multi-character contractions (e.g. Spanish
	// 'ch' or Hungarian 'dzs') via lookahead matching.
	if ws.expander != nil {
		if exp := ws.expander(r); exp != "" {
			for _, er := range exp {
				ws.expanded[ws.expandedLen] = er
				ws.expandedLen++
			}
			ws.expandedIdx = 1
			return ws.getRuneWeight(ws.expanded[0]), true, nil
		}
	}
	return ws.getRuneWeight(r), true, nil
}
