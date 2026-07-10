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

package memo

import (
	"fmt"
	"math"
	"math/bits"
)

type BitSet uint64

func newBitSet(idxs ...uint64) (res BitSet) {
	for _, idx := range idxs {
		res = res.add(idx)
	}
	return res
}

// add returns a copy of the bitSet with the given element added.
func (s BitSet) add(idx uint64) BitSet {
	if idx > maxSetSize {
		panic(fmt.Sprintf("cannot insert %d into bitSet", idx))
	}
	return s | (1 << idx)
}

// remove returns a copy of the bitSet with the given element removed.
func (s BitSet) remove(idx uint64) BitSet {
	if idx > maxSetSize {
		panic(fmt.Sprintf("%d is invalid index for bitSet", idx))
	}
	return s & ^(1 << idx)
}

// contains returns whether a bitset contains a given element.
func (s BitSet) contains(idx uint64) bool {
	if idx > maxSetSize {
		panic(fmt.Sprintf("%d is invalid index for bitSet", idx))
	}
	return s&(1<<idx) != 0
}

// union returns the set union of this set with the given set.
func (s BitSet) union(o BitSet) BitSet {
	return s | o
}

// intersection returns the set intersection of this set with the given set.
func (s BitSet) intersection(o BitSet) BitSet {
	return s & o
}

// difference returns the set difference of this set with the given set.
func (s BitSet) difference(o BitSet) BitSet {
	return s & ^o
}

// intersects returns true if this set and the given set intersect.
func (s BitSet) intersects(o BitSet) bool {
	return s.intersection(o) != 0
}

// isSubsetOf returns true if this set is a subset of the given set.
func (s BitSet) isSubsetOf(o BitSet) bool {
	return s.union(o) == o
}

// isSingleton returns true if the set has exactly one element.
func (s BitSet) isSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s BitSet) next(startVal uint64) (elem uint64, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + uint64(ntz), true
		}
	}
	return uint64(math.MaxInt64), false
}

// len returns the number of elements in the set.
func (s BitSet) len() int {
	return bits.OnesCount64(uint64(s))
}

func (s BitSet) String() string {
	var str string
	var i BitSet = 1
	cnt := 0
	for cnt < s.len() {
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
