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

package sets

import (
	"fmt"
	"math"
	"math/bits"
)

type BitSet uint64

const maxSetSize = 63

func NewBitSet(idxs ...uint64) (res BitSet) {
	for _, idx := range idxs {
		res = res.Add(idx)
	}
	return res
}

// Add returns a copy of the bitSet with the given element added.
func (s BitSet) Add(idx uint64) BitSet {
	// TODO: return error instead of panicking
	if idx > maxSetSize {
		panic(fmt.Sprintf("cannot insert %d into bitSet", idx))
	}
	return s | (1 << idx)
}

// Remove returns a copy of the bitSet with the given element removed.
func (s BitSet) Remove(idx uint64) BitSet {
	// TODO: return error instead of panicking
	if idx > maxSetSize {
		panic(fmt.Sprintf("%d is invalid index for bitSet", idx))
	}
	return s & ^(1 << idx)
}

// Contains returns whether a bitset contains a given element.
func (s BitSet) Contains(idx uint64) bool {
	// TODO: return error instead of panicking
	if idx > maxSetSize {
		panic(fmt.Sprintf("%d is invalid index for bitSet", idx))
	}
	return s&(1<<idx) != 0
}

// Union returns the set union of this set with the given set.
func (s BitSet) Union(o BitSet) BitSet {
	return s | o
}

// Intersection returns the set intersection of this set with the given set.
func (s BitSet) Intersection(o BitSet) BitSet {
	return s & o
}

// Difference returns the set difference of this set with the given set.
func (s BitSet) Difference(o BitSet) BitSet {
	return s & ^o
}

// Intersects returns true if this set and the given set intersect.
func (s BitSet) Intersects(o BitSet) bool {
	return s.Intersection(o) != 0
}

// IsSubsetOf returns true if this set is a subset of the given set.
func (s BitSet) IsSubsetOf(o BitSet) bool {
	return s.Union(o) == o
}

// IsSingleton returns true if the set has exactly one element.
func (s BitSet) IsSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// Next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s BitSet) Next(startVal uint64) (elem uint64, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + uint64(ntz), true
		}
	}
	return uint64(math.MaxInt64), false
}

// Size returns the number of elements in the set.
func (s BitSet) Size() int {
	return bits.OnesCount64(uint64(s))
}

func (s BitSet) String() string {
	var str string
	var i BitSet = 1
	cnt := 0
	for cnt < s.Size() {
		if (i & s) != 0 {
			str += "1"
			cnt++
		} else {
			str += "0"
		}
		i = i << 1
	}
	return str
}
