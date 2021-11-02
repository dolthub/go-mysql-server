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

// RangeCollection is a collection of ranges that represent different (non-overlapping) filter expressions.
type RangeCollection []Range

// Range is a collection of RangeColumns that are ordered by the column expressions as returned by their parent
// index. A single range represents a set of values intended for iteration by an integrator's index.
type Range []RangeColumn

// RangeColumn is a slice of RangeColumnExprs meant to represent a set of discrete, non-overlapping RangeColumnExprs.
type RangeColumn []RangeColumnExpr

// Intersect attempts to intersect the given RangeCollection with the calling RangeCollection. This ensures that each
// Range belonging to the same collection is treated as a union with respect to that same collection, rather than
// attempting to intersect ranges that are a part of the same collection.
func (ranges RangeCollection) Intersect(otherRanges RangeCollection) (RangeCollection, error) {
	var newRanges RangeCollection
	for _, rang := range ranges {
		for _, otherRange := range otherRanges {
			newRange, err := rang.Intersect(otherRange)
			if err != nil {
				return nil, err
			}
			if len(newRange) > 0 {
				newRanges = append(newRanges, newRange)
			}
		}
	}
	newRanges, err := SimplifyRanges(newRanges...)
	if err != nil {
		return nil, err
	}
	if len(newRanges) == 0 {
		return nil, nil
	}
	return newRanges, nil
}

// AsEmpty returns a Range full of empty RangeColumns with the same types as the calling Range.
func (rang Range) AsEmpty() Range {
	emptyRange := make(Range, len(rang))
	for i := range rang {
		emptyRange[i] = RangeColumn{EmptyRangeColumnExpr(rang[i][0].typ)}
	}
	return emptyRange
}

// RangesByColumnExpression returns the RangeColumn that belongs to the given column expression. If an index does not
// contain the given column expression then a nil is returned.
func (rang Range) RangesByColumnExpression(idx Index, colExpr string) RangeColumn {
	for i, idxColExpr := range idx.Expressions() {
		if idxColExpr == colExpr {
			if i < len(rang) {
				return rang[i]
			}
			break
		}
	}
	return nil
}

// Intersect attempts to intersect the given Range with the calling Range.
func (rang Range) Intersect(otherRange Range) (Range, error) {
	if len(rang) != len(otherRange) {
		return nil, nil
	}
	newRangeCollection := make(Range, len(rang))
	for i := range rang {
		intersectedRanges, ok, err := rang[i].TryIntersect(otherRange[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			return rang.AsEmpty(), nil
		}
		newRangeCollection[i] = intersectedRanges
	}
	return newRangeCollection, nil
}

// IsSubsetOf evaluates whether the calling Range is fully encompassed by the given Range.
func (rang Range) IsSubsetOf(otherRange Range) (bool, error) {
	if len(rang) != len(otherRange) {
		return false, nil
	}
	for i := range rang {
		ok, err := rang[i].IsSubsetOf(otherRange[i])
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// IsSupersetOf evaluates whether the calling Range fully encompasses the given Range.
func (rang Range) IsSupersetOf(otherRange Range) (bool, error) {
	return otherRange.IsSubsetOf(rang)
}

// Equals returns whether the calling RangeColumn is equal to the given RangeColumn.
func (rc RangeColumn) Equals(other RangeColumn) (bool, error) {
	if len(rc) != len(other) {
		return false, nil
	}
	for i := range rc {
		if ok, err := rc[i].Equals(other[i]); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// TryIntersect attempts to intersect the calling RangeColumn with the given RangeColumn. If the intersection fails or
// results in an empty RangeColumn then nil and false are returned.
func (rc RangeColumn) TryIntersect(other RangeColumn) (RangeColumn, bool, error) {
	var newRangeColumn []RangeColumnExpr
	var err error
	for _, rr := range rc {
		for _, or := range other {
			newRange, ok, err := rr.TryIntersect(or)
			if err != nil {
				return nil, false, err
			}
			if ok {
				newRangeColumn = append(newRangeColumn, newRange)
			}
		}
	}
	newRangeColumn, err = SimplifyRangeColumn(newRangeColumn...)
	if err != nil || len(newRangeColumn) == 0 {
		return nil, false, err
	}
	return newRangeColumn, true, nil
}

// IsSubsetOf evaluates whether the calling RangeColumn is fully encompassed by the given RangeColumn.
func (rc RangeColumn) IsSubsetOf(other RangeColumn) (bool, error) {
	for _, rce := range rc {
		isSubset := false
		for _, otherRCE := range other {
			if ok, err := rce.IsSubsetOf(otherRCE); err != nil {
				return false, err
			} else if ok {
				isSubset = true
				break
			}
		}
		if !isSubset {
			return false, nil
		}
	}
	return true, nil
}

// IsSupersetOf evaluates whether the calling RangeColumn fully encompasses the given RangeColumn.
func (rc RangeColumn) IsSupersetOf(other RangeColumn) (bool, error) {
	return other.IsSubsetOf(rc)
}

// IntersectRanges intersects each Range for each column expression. If a RangeColumnExpr ends up with no valid ranges
// then a nil is returned.
func IntersectRanges(ranges ...Range) Range {
	if len(ranges) == 0 {
		return nil
	}
	var rang Range
	i := 0
	for ; i < len(ranges); i++ {
		rc := ranges[i]
		if len(rc) == 0 {
			continue
		}
		rang = rc
		break
	}
	if len(rang) == 0 {
		return nil
	}
	i++

	for ; i < len(ranges); i++ {
		rc := ranges[i]
		if len(rc) == 0 {
			continue
		}
		newRange, err := rang.Intersect(rc)
		if err != nil || len(newRange) == 0 {
			return nil
		}
	}
	if len(rang) == 0 {
		return nil
	}
	return rang
}

// SimplifyRanges operates differently depending on whether the given RangeCollection represent a single RangeColumn or
// multiple RangeColumns, as they have different rules. If the collections contain a single RangeColumn then they are
// all unioned together. If the collections contain multiple RangeColumns then all RangeColumns that are a subset of
// another RangeColumn are removed.
//
// This difference is because a Range that contains multiple RangeColumns was constructed with an AND restriction
// between each column, therefore a union between only RangeColumnExprs would result in valid results according to the
// RangeColumn that are invalid according to the filter itself.
func SimplifyRanges(ranges ...Range) (RangeCollection, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	if len(ranges[0]) == 1 {
		var allRangeColExprs []RangeColumnExpr
		returnAllRange := true
		for _, rangeCollection := range ranges {
			if len(rangeCollection) != 1 {
				returnAllRange = false
				break
			}
			allRangeColExprs = append(allRangeColExprs, rangeCollection[0]...)
		}
		if returnAllRange {
			var err error
			allRangeColExprs, err = SimplifyRangeColumn(allRangeColExprs...)
			if err != nil {
				return nil, err
			}
			if len(allRangeColExprs) == 0 {
				return []Range{{RangeColumn{EmptyRangeColumnExpr(ranges[0][0][0].typ)}}}, nil
			}
			return []Range{{allRangeColExprs}}, nil
		}
	}

	var discreteRanges []Range
	for _, rang := range ranges {
		shouldAdd := true
		for dcIndex := 0; dcIndex < len(discreteRanges); dcIndex++ {
			discreteCollection := discreteRanges[dcIndex]
			if ok, err := discreteCollection.IsSupersetOf(rang); err != nil {
				return nil, err
			} else if ok {
				shouldAdd = false
				break
			} else if ok, err = rang.IsSupersetOf(discreteCollection); err != nil {
				return nil, err
			} else if ok {
				discreteRanges = append(discreteRanges[:dcIndex], discreteRanges[dcIndex+1:]...)
				dcIndex--
			}
		}
		if shouldAdd {
			discreteRanges = append(discreteRanges, rang)
		}
	}
	return discreteRanges, nil
}
