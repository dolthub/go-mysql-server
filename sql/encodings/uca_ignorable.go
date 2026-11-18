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

package encodings

import (
	"math"
	"slices"
)

//go:generate go run ./generate -ignorable -out uca_ignorable.gen.go

// IgnorableWeight is the zero sort weight assigned to ignorable code points in
// Unicode Collation Algorithm (UCA) collations.
const IgnorableWeight int32 = 0

// UCASorter returns a rune weight function for |collationName| and reports
// whether |collationName| uses UCA ignorable-character rules.
//
// For UCA collations whose generated weight maps do not already assign
// IgnorableWeight to ignorable code points, UCASorter wraps |base| so ignorable
// runes return IgnorableWeight and non-ignorable weights shift above zero.
func UCASorter(collationName string, base func(rune) int32) (func(rune) int32, bool) {
	if base == nil {
		return nil, false
	}
	ranges, isUCA := ucaIgnorableRanges[collationName]
	if !isUCA {
		return base, false
	}
	if len(ranges) == 0 {
		return base, true
	}
	return func(r rune) int32 {
		if (r < ' ' || r > '~') && inRuneRanges(r, ranges) {
			return IgnorableWeight
		}
		w := base(r)
		if w < math.MaxInt32 {
			return w + 1
		}
		return w
	}, true
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
