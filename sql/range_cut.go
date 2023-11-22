// Copyright 2021 Dolthub, Inc.
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
	"fmt"
)

// RangeCut represents a position on the line of all possible values.
type RangeCut interface {
	// Compare returns an integer stating the relative position of the calling RangeCut to the given RangeCut.
	Compare(RangeCut, Type) (int, error)
	// String returns the RangeCut as a string for display purposes.
	String() string
	// TypeAsLowerBound returns the bound type if the calling RangeCut is the lower bound of a range.
	TypeAsLowerBound() RangeBoundType
	// TypeAsUpperBound returns the bound type if the calling RangeCut is the upper bound of a range.
	TypeAsUpperBound() RangeBoundType
}

// RangeBoundType is the bound of the RangeCut.
type RangeBoundType int

const (
	// Open bounds represent exclusion.
	Open RangeBoundType = iota
	// Closed bounds represent inclusion.
	Closed
)

// Inclusive returns whether the bound represents inclusion.
func (bt RangeBoundType) Inclusive() bool {
	return bt == Closed
}

// GetRangeCutKey returns the inner value from the given RangeCut.
func GetRangeCutKey(c RangeCut) interface{} {
	switch c := c.(type) {
	case Below:
		return c.Key
	case Above:
		return c.Key
	default:
		panic(fmt.Errorf("need to check the RangeCut type before calling GetRangeCutKey, used on `%T`", c))
	}
}

func RangeCutIsBinding(c RangeCut) bool {
	switch c.(type) {
	case Below, Above:
		return true
	case AboveAll, AboveNull, BelowNull:
		return false
	default:
		panic(fmt.Errorf("unknown range cut %v", c))
	}
}

// GetRangeCutMax returns the RangeCut with the highest value.
func GetRangeCutMax(typ Type, cuts ...RangeCut) (RangeCut, error) {
	i := 0
	var maxCut RangeCut
	for ; i < len(cuts); i++ {
		if cuts[i] != nil {
			maxCut = cuts[i]
			i++
			break
		}
	}
	for ; i < len(cuts); i++ {
		if cuts[i] == nil {
			continue
		}
		comp, err := maxCut.Compare(cuts[i], typ)
		if err != nil {
			return maxCut, err
		}
		if comp == -1 {
			maxCut = cuts[i]
		}
	}
	return maxCut, nil
}

// GetRangeCutMin returns the RangeCut with the lowest value.
func GetRangeCutMin(typ Type, cuts ...RangeCut) (RangeCut, error) {
	i := 0
	var minCut RangeCut
	for ; i < len(cuts); i++ {
		if cuts[i] != nil {
			minCut = cuts[i]
			i++
			break
		}
	}
	for ; i < len(cuts); i++ {
		if cuts[i] == nil {
			continue
		}
		comp, err := minCut.Compare(cuts[i], typ)
		if err != nil {
			return minCut, err
		}
		if comp == 1 {
			minCut = cuts[i]
		}
	}
	return minCut, nil
}

// Above represents the position immediately above the contained key.
type Above struct {
	Key interface{}
}

var _ RangeCut = Above{}

// Compare implements RangeCut.
func (a Above) Compare(c RangeCut, typ Type) (int, error) {
	switch c := c.(type) {
	case AboveAll:
		return -1, nil
	case AboveNull:
		return 1, nil
	case Above:
		return typ.Compare(a.Key, c.Key)
	case Below:
		cmp, err := typ.Compare(a.Key, c.Key)
		if err != nil {
			return 0, err
		}
		if cmp == -1 {
			return -1, nil
		}
		return 1, nil
	case BelowNull:
		return 1, nil
	default:
		panic(fmt.Errorf("unrecognized RangeCut type '%T'", c))
	}
}

// String implements RangeCut.
func (a Above) String() string {
	return fmt.Sprintf("Above[%v]", a.Key)
}

// TypeAsLowerBound implements RangeCut.
func (Above) TypeAsLowerBound() RangeBoundType {
	return Open
}

// TypeAsUpperBound implements RangeCut.
func (Above) TypeAsUpperBound() RangeBoundType {
	return Closed
}

// AboveAll represents the position beyond the maximum possible value.
type AboveAll struct{}

var _ RangeCut = AboveAll{}

// Compare implements RangeCut.
func (AboveAll) Compare(c RangeCut, typ Type) (int, error) {
	if _, ok := c.(AboveAll); ok {
		return 0, nil
	}
	return 1, nil
}

// String implements RangeCut.
func (AboveAll) String() string {
	return "AboveAll"
}

// TypeAsLowerBound implements RangeCut.
func (AboveAll) TypeAsLowerBound() RangeBoundType {
	return Open
}

// TypeAsUpperBound implements RangeCut.
func (AboveAll) TypeAsUpperBound() RangeBoundType {
	return Open
}

// Below represents the position immediately below the contained key.
type Below struct {
	Key interface{}
}

var _ RangeCut = Below{}

// Compare implements RangeCut.
func (b Below) Compare(c RangeCut, typ Type) (int, error) {
	switch c := c.(type) {
	case AboveAll:
		return -1, nil
	case AboveNull:
		return 1, nil
	case Below:
		return typ.Compare(b.Key, c.Key)
	case Above:
		cmp, err := typ.Compare(c.Key, b.Key)
		if err != nil {
			return 0, err
		}
		if cmp == -1 {
			return 1, nil
		}
		return -1, nil
	case BelowNull:
		return 1, nil
	default:
		panic(fmt.Errorf("unrecognized RangeCut type '%T'", c))
	}
}

// String implements RangeCut.
func (b Below) String() string {
	return fmt.Sprintf("Below[%v]", b.Key)
}

// TypeAsLowerBound implements RangeCut.
func (Below) TypeAsLowerBound() RangeBoundType {
	return Closed
}

// TypeAsUpperBound implements RangeCut.
func (Below) TypeAsUpperBound() RangeBoundType {
	return Open
}

// AboveNull represents the position just above NULL, lower than every possible value in the domain.
type AboveNull struct{}

var _ RangeCut = AboveNull{}

// Compare implements RangeCut.
func (AboveNull) Compare(c RangeCut, typ Type) (int, error) {
	if _, ok := c.(AboveNull); ok {
		return 0, nil
	}
	if _, ok := c.(BelowNull); ok {
		return 1, nil
	}
	return -1, nil
}

// String implements RangeCut.
func (AboveNull) String() string {
	return "AboveNull"
}

// TypeAsLowerBound implements RangeCut.
func (AboveNull) TypeAsLowerBound() RangeBoundType {
	return Open
}

// TypeAsUpperBound implements RangeCut.
func (AboveNull) TypeAsUpperBound() RangeBoundType {
	return Closed
}

// BelowNull represents the position below NULL, which sorts before |AboveNull|
// and every non-NULL value in the domain.
type BelowNull struct{}

var _ RangeCut = BelowNull{}

// Compare implements RangeCut.
func (BelowNull) Compare(c RangeCut, typ Type) (int, error) {
	// BelowNull overlaps with itself
	if _, ok := c.(BelowNull); ok {
		return 0, nil
	}
	return -1, nil
}

// String implements RangeCut.
func (BelowNull) String() string {
	return "BelowNull"
}

// TypeAsLowerBound implements RangeCut.
func (BelowNull) TypeAsLowerBound() RangeBoundType {
	return Closed
}

// TypeAsUpperBound implements RangeCut.
func (BelowNull) TypeAsUpperBound() RangeBoundType {
	return Open
}
