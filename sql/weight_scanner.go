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

// WeightScanner sequentially produces collation weights for a string
// under a specific collation, expanding multi-weight characters as
// needed.
type WeightScanner struct {
	encoder       encodings.Encoder
	getRuneWeight CollationSorter
	expander      func(r rune) []rune
	str           string
	expanded      []rune
	expandedIdx   int
}

// NewWeightScanner returns a new [WeightScanner] configured for the
// provided collation and input string.
func NewWeightScanner(collation CollationID, str string) WeightScanner {
	return WeightScanner{
		encoder:       collation.CharacterSet().Encoder(),
		getRuneWeight: collationArray[collation].Sorter,
		expander:      collation.Expander(),
		str:           str,
	}
}

// Next returns the next weight in the collation sequence. It returns
// false for ok when the input string has been fully consumed, or an
// error if a malformed byte sequence is encountered.
func (ws *WeightScanner) Next() (weight int32, ok bool, err error) {
	if ws.expandedIdx < len(ws.expanded) {
		w := ws.getRuneWeight(ws.expanded[ws.expandedIdx])
		ws.expandedIdx++
		return w, true, nil
	}
	ws.expanded = nil
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
	if read == 0 || read == utf8.RuneError {
		return 0, false, ErrCollationMalformedString.New("scanning")
	}
	ws.str = ws.str[read:]

	if ws.expander != nil {
		if exp := ws.expander(r); len(exp) > 0 {
			ws.expanded = exp
			ws.expandedIdx = 1
			return ws.getRuneWeight(exp[0]), true, nil
		}
	}
	return ws.getRuneWeight(r), true, nil
}
