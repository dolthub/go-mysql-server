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

package function

import (
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
)

// CharSetHandler measures character counts and calculates character
// offsets according to a character set or collation.
type CharSetHandler interface {
	// NumChars returns the number of characters in str. For binary
	// strings, it returns the number of bytes.
	NumChars(str string) (int, error)
	// CharPos returns the byte index in str where character n begins.
	// If n is greater than or equal to the number of characters, it
	// returns len(str).
	CharPos(str string, n int) (int, error)
}

// NewCharSetHandler returns a CharSetHandler for the
// given collation.
func NewCharSetHandler(collation sql.CollationID) CharSetHandler {
	if collation.CharacterSet() == sql.CharacterSet_binary {
		return binaryCharSetHandler{}
	}
	enc := collation.CharacterSet().Encoder()
	if enc == nil {
		enc = sql.CharacterSet_utf8mb4.Encoder()
	}
	return runeCharSetHandler{encoder: enc}
}

// binaryCharSetHandler measures strings byte-for-byte.
type binaryCharSetHandler struct{}

var _ CharSetHandler = binaryCharSetHandler{}

func (binaryCharSetHandler) NumChars(str string) (int, error) {
	return len(str), nil
}

func (binaryCharSetHandler) CharPos(str string, n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	if n >= len(str) {
		return len(str), nil
	}
	return n, nil
}

// runeCharSetHandler measures strings by code point.
type runeCharSetHandler struct {
	encoder encodings.Encoder
}

var _ CharSetHandler = runeCharSetHandler{}

func (h runeCharSetHandler) NumChars(str string) (int, error) {
	content := str
	contentLen := 0
	for len(content) > 0 {
		cr, cRead := h.encoder.NextRune(content)
		if cr == utf8.RuneError && cRead <= 1 {
			return 0, sql.ErrCollationMalformedString.New("checking length")
		}
		content = content[cRead:]
		contentLen++
	}
	return contentLen, nil
}

func (h runeCharSetHandler) CharPos(str string, n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	content := str
	byteOffset := 0
	for count := 0; count < n && len(content) > 0; count++ {
		cr, cRead := h.encoder.NextRune(content)
		if cr == utf8.RuneError && cRead <= 1 {
			return 0, sql.ErrCollationMalformedString.New("charpos")
		}
		content = content[cRead:]
		byteOffset += cRead
	}
	return byteOffset, nil
}
