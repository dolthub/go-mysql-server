// Copyright 2022 Dolthub, Inc.
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

package encodings

import "unicode/utf8"

// RangeMap is an implementation of Encoder. Almost all files that make use of RangeMap have been generated by the
// collation-extractor project: https://github.com/dolthub/collation-extractor
type RangeMap struct {
	inputEntries  [][]rangeMapEntry
	outputEntries [][]rangeMapEntry
	toUpper       map[rune]rune
	toLower       map[rune]rune
}

var _ Encoder = (*RangeMap)(nil)

// rangeMapEntry is an entry within a RangeMap, which represents a range of valid inputs along with the possible
// outputs, along with the multiplier for each byte position.
type rangeMapEntry struct {
	inputRange  rangeBounds
	outputRange rangeBounds
	inputMults  []int
	outputMults []int
}

// rangeBounds represents the minimum and maximum values for each section of this specific range. The byte at index 0
// represents the minimum, while the byte at index 1 represents the maximum.
type rangeBounds [][2]byte

// Decode implements the Encoder interface.
func (rm *RangeMap) Decode(str []byte) ([]byte, bool) {
	// There's no way of knowing how large the resulting string will be, but we can at least set it to the same size to
	// minimize allocations.
	decodedStr := make([]byte, 0, len(str))
	for len(str) > 0 {
		var decodedRune []byte
		decodedRuneLen := 1
		// The most common strings for most expected applications will find their result in the first loop, so the
		// performance here shouldn't be as bad as it may seem.
		for ; decodedRuneLen <= len(rm.inputEntries); decodedRuneLen++ {
			if decodedRuneLen > len(str) {
				return nil, false
			}
			var ok bool
			decodedRune, ok = rm.DecodeRune(str[:decodedRuneLen])
			if ok {
				break
			}
		}
		if decodedRuneLen > len(rm.inputEntries) {
			return nil, false
		}
		decodedStr = append(decodedStr, decodedRune...)
		str = str[decodedRuneLen:]
	}
	return decodedStr, true
}

// Encode implements the Encoder interface.
func (rm *RangeMap) Encode(str []byte) ([]byte, bool) {
	// There's no way of knowing how large the resulting string will be, but we can at least set it to the same size to
	// minimize allocations.
	encodedStr := make([]byte, 0, len(str))
	for len(str) > 0 {
		var encodedRune []byte
		encodedRuneLen := 1
		// The most common strings for most expected applications will find their result in the first loop, so the
		// performance here shouldn't be as bad as it may seem.
		for ; encodedRuneLen <= len(rm.inputEntries); encodedRuneLen++ {
			var ok bool
			encodedRune, ok = rm.EncodeRune(str[:encodedRuneLen])
			if ok {
				break
			}
		}
		if encodedRuneLen > len(rm.inputEntries) {
			return nil, false
		}
		encodedStr = append(encodedStr, encodedRune...)
		str = str[encodedRuneLen:]
	}
	return encodedStr, true
}

// EncodeReplaceUnknown implements the Encoder interface.
func (rm *RangeMap) EncodeReplaceUnknown(str []byte) []byte {
	// There's no way of knowing how large the resulting string will be, but we can at least set it to the same size to
	// minimize allocations.
	encodedStr := make([]byte, 0, len(str))
	for len(str) > 0 {
		var encodedRune []byte
		encodedRuneLen := 1
		// The most common strings for most expected applications will find their result in the first loop, so the
		// performance here shouldn't be as bad as it may seem.
		for ; encodedRuneLen <= len(rm.inputEntries) && encodedRuneLen <= len(str); encodedRuneLen++ {
			var ok bool
			encodedRune, ok = rm.EncodeRune(str[:encodedRuneLen])
			if ok {
				break
			}
		}
		if encodedRuneLen > len(rm.inputEntries) {
			// The rune is not valid in this character set, so we'll attempt to see if the rune is valid utf8.
			// If it is, then we want to replace the entire rune with a question mark. If it's not, then we'll
			// just replace the next byte.
			_, encodedRuneLen = utf8.DecodeRune(str)
			if encodedRuneLen == 0 {
				encodedRuneLen = 1
			}
			encodedRune = []byte{'?'}
		}
		// Since we do not terminate on invalid sequences, we may end up in a scenario where our count is misaligned, so
		// we need to catch such instances.
		if encodedRuneLen >= len(str) {
			encodedRuneLen = len(str)
		}
		if len(encodedRune) == 0 {
			encodedRune = []byte{'?'}
		}
		encodedStr = append(encodedStr, encodedRune...)
		str = str[encodedRuneLen:]
	}
	return encodedStr
}

// DecodeRune implements the Encoder interface.
func (rm *RangeMap) DecodeRune(r []byte) ([]byte, bool) {
	if len(r) > len(rm.inputEntries) {
		return nil, false
	}
	for _, entry := range rm.inputEntries[len(r)-1] {
		if entry.inputRange.contains(r) {
			outputData := make([]byte, len(entry.outputRange))
			increase := 0
			for i := len(entry.inputRange) - 1; i >= 0; i-- {
				increase += int(r[i]-entry.inputRange[i][0]) * entry.inputMults[i]
			}
			for i := 0; i < len(outputData); i++ {
				diff := increase / entry.outputMults[i]
				outputData[i] = entry.outputRange[i][0] + byte(diff)
				increase -= diff * entry.outputMults[i]
			}
			return outputData, true
		}
	}
	return nil, false
}

// EncodeRune implements the Encoder interface.
func (rm *RangeMap) EncodeRune(r []byte) ([]byte, bool) {
	if len(r) > len(rm.outputEntries) {
		return nil, false
	}
	for _, entry := range rm.outputEntries[len(r)-1] {
		if entry.outputRange.contains(r) {
			inputData := make([]byte, len(entry.inputRange))
			increase := 0
			for i := len(entry.outputRange) - 1; i >= 0; i-- {
				increase += int(r[i]-entry.outputRange[i][0]) * entry.outputMults[i]
			}
			for i := 0; i < len(inputData); i++ {
				diff := increase / entry.inputMults[i]
				inputData[i] = entry.inputRange[i][0] + byte(diff)
				increase -= diff * entry.inputMults[i]
			}
			return inputData, true
		}
	}
	return nil, false
}

// Uppercase implements the Encoder interface.
func (rm *RangeMap) Uppercase(str string) string {
	newStr := make([]byte, 0, len(str))
	// Range loops over strings automatically read the string as a series of runes, similar to utf8.DecodeRuneInString().
	// See: https://go.dev/doc/effective_go#for & https://pkg.go.dev/unicode/utf8#DecodeRuneInString
	for _, r := range str {
		// Wrapping a rune in a string will convert it to a sequence of bytes, which are then appended to the byte slice
		newStr = append(newStr, string(rm.UppercaseRune(r))...)
	}
	return BytesToString(newStr)
}

// Lowercase implements the Encoder interface.
func (rm *RangeMap) Lowercase(str string) string {
	newStr := make([]byte, 0, len(str))
	// Range loops over strings automatically read the string as a series of runes, similar to utf8.DecodeRuneInString().
	// See: https://go.dev/doc/effective_go#for & https://pkg.go.dev/unicode/utf8#DecodeRuneInString
	for _, r := range str {
		// Wrapping a rune in a string will convert it to a sequence of bytes, which are then appended to the byte slice
		newStr = append(newStr, string(rm.LowercaseRune(r))...)
	}
	return BytesToString(newStr)
}

// UppercaseRune implements the Encoder interface.
func (rm *RangeMap) UppercaseRune(r rune) rune {
	if uRune, ok := rm.toUpper[r]; ok {
		return uRune
	}
	return r
}

// LowercaseRune implements the Encoder interface.
func (rm *RangeMap) LowercaseRune(r rune) rune {
	if lRune, ok := rm.toLower[r]; ok {
		return lRune
	}
	return r
}

// NextRune implements the Encoder interface.
func (rm *RangeMap) NextRune(str string) (rune, int) {
	return utf8.DecodeRuneInString(str)
}

// IsReturnSafe implements the Encoder interface. All returns from RangeMap are safe to edit as they create a new byte
// slice.
func (rm *RangeMap) IsReturnSafe() bool {
	return true
}

// contains returns whether the data falls within the range bounds. Assumes that the length of the data matches the
// length of the range bounds.
func (r rangeBounds) contains(data []byte) bool {
	for i := 0; i < len(r); i++ {
		if r[i][0] > data[i] || r[i][1] < data[i] {
			return false
		}
	}
	return true
}
