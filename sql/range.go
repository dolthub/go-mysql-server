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

import "strings"

// RangeCollection is a collection of ranges that represent different (non-overlapping) filter expressions.
type RangeCollection []Range

// Range is a collection of RangeColumns that are ordered by the column expressions as returned by their parent
// index. A single range represents a set of values intended for iteration by an integrator's index.
type Range []RangeColumnExpr

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
	newRanges, err := RemoveOverlappingRanges(newRanges...)
	if err != nil {
		return nil, err
	}
	if len(newRanges) == 0 {
		return nil, nil
	}
	return newRanges, nil
}

// String returns this RangeCollection as a string for display purposes.
func (ranges RangeCollection) String() string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, rang := range ranges {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rang.String())
	}
	sb.WriteByte(']')
	return sb.String()
}

// DebugString returns this RangeCollection as a string for debugging purposes.
func (ranges RangeCollection) DebugString() string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, rang := range ranges {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rang.DebugString())
	}
	sb.WriteByte(']')
	return sb.String()
}

// AsEmpty returns a Range full of empty RangeColumns with the same types as the calling Range.
func (rang Range) AsEmpty() Range {
	emptyRange := make(Range, len(rang))
	for i := range rang {
		emptyRange[i] = EmptyRangeColumnExpr(rang[i].typ)
	}
	return emptyRange
}

// Copy returns a duplicate of this Range.
func (rang Range) Copy() Range {
	newRange := make(Range, len(rang))
	for i, colExpr := range rang {
		newRange[i] = colExpr // RangeColumnExpr and all of its members are non-pointer types, so they're copied
	}
	return newRange
}

// ExpressionByColumnName returns the RangeColumnExpr that belongs to the given column expression. If an index does not
// contain the column expression then false is returned.
func (rang Range) ExpressionByColumnName(idx Index, colExpr string) (RangeColumnExpr, bool) {
	for i, idxColExpr := range idx.Expressions() {
		if idxColExpr == colExpr {
			if i < len(rang) {
				return rang[i], true
			}
			break
		}
	}
	return RangeColumnExpr{}, false
}

// Equals evaluates whether the calling Range is equivalent to the given Range.
func (rang Range) Equals(otherRange Range) (bool, error) {
	if len(rang) != len(otherRange) {
		return false, nil
	}
	for i := range rang {
		if ok, err := rang[i].Equals(otherRange[i]); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// Intersect intersects the given Range with the calling Range.
func (rang Range) Intersect(otherRange Range) (Range, error) {
	if len(rang) != len(otherRange) {
		return nil, nil
	}
	newRangeCollection := make(Range, len(rang))
	for i := range rang {
		intersectedRange, ok, err := rang[i].TryIntersect(otherRange[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			return rang.AsEmpty(), nil
		}
		newRangeCollection[i] = intersectedRange
	}
	return newRangeCollection, nil
}

// TryMerge attempts to merge the given Range with the calling Range. This can only do a merge if one Range is a subset
// of the other, or if all columns except for one are equivalent, upon which a union is attempted on that column.
// Returns true if the merge was successful.
func (rang Range) TryMerge(otherRange Range) (Range, bool, error) {
	if len(rang) != len(otherRange) {
		return nil, false, nil
	}
	if ok, err := rang.IsSupersetOf(otherRange); err != nil {
		return nil, false, err
	} else if ok {
		return rang, true, nil
	}
	if ok, err := otherRange.IsSupersetOf(rang); err != nil {
		return nil, false, err
	} else if ok {
		return otherRange, true, nil
	}

	indexToMerge := -1
	// The superset checks will cover if every column expr is equivalent
	for i := 0; i < len(rang); i++ {
		if ok, err := rang[i].Equals(otherRange[i]); err != nil {
			return nil, false, err
		} else if !ok {
			// Only one column may not equal another
			if indexToMerge == -1 {
				indexToMerge = i
			} else {
				return nil, false, nil
			}
		}
	}
	mergedLastExpr, ok, err := rang[indexToMerge].TryUnion(otherRange[indexToMerge])
	if err != nil || !ok {
		return nil, false, err
	}
	mergedRange := rang.Copy()
	mergedRange[indexToMerge] = mergedLastExpr
	return mergedRange, true, nil
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

// IsConnected returns whether the calling Range and given Range have overlapping values, which would result in the same
// values being returned from some subset of both ranges.
func (rang Range) IsConnected(otherRange Range) (bool, error) {
	if len(rang) != len(otherRange) {
		return false, nil
	}
	for i := range rang {
		_, ok, err := rang[i].Overlaps(otherRange[i])
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// Overlaps returns whether the calling Range and given Range have overlapping values, which would result in the same
// values being returned from some subset of both ranges.
func (rang Range) Overlaps(otherRange Range) (bool, error) {
	if len(rang) != len(otherRange) {
		return false, nil
	}
	for i := range rang {
		_, ok, err := rang[i].Overlaps(otherRange[i])
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// RemoveOverlap removes any overlap that the given Range may have with the calling Range. If the two ranges do not
// overlap and are not mergeable then they're both returned. If one is a subset of the other or is mergeable then only
// one Range is returned. Otherwise, this returns a collection of ranges that do not overlap with each other, and covers
// the entirety of the original ranges (and nothing more). If the two ranges do not overlap and are not mergeable then
// false is returned, otherwise returns true.
func (rang Range) RemoveOverlap(otherRange Range) (RangeCollection, bool, error) {
	// An explanation on why overlapping ranges may return more than one range, and why they can't just be merged as-is.
	// Let's start with a Range that has a single RangeColumnExpression (a one-dimensional range). Imagine this as a
	// number line with contiguous sections defined as the range. If you have any two sections that overlap, then you
	// can simply take the lowest and highest bounds between both of those sections to create a single, larger range
	// that fully encompasses both (while not including any elements that were not in the original ranges).
	//
	// Now let's look at a Range that has two RangeColumnExpressions (a two-dimensional range). Imagine this as a sheet
	// of paper on a table (for easier visualization). If these two sheet overlap then we can't just take the lowest
	// and highest bounds of these sheets as that may include areas outside either sheet of paper. Instead, we can cut
	// the sheets so that we get smaller sheets of paper, with one "sub sheet" perfectly overlapping the other. This
	// may be done with two cuts on each sheet, giving us a total of 8 smaller sheets overall. Of course the perfectly
	// overlapping sheets can be combined, so we throw one of them away. From there we're back to our original Range
	// example with only one dimension, as now this overlapping subsheet will differ from the sheets on its edges by
	// only a single dimension (the sheet to the left, for example, will be the same height but extending further left).
	// We can then combine it with its edge-adjacent sheets until we have a collection of sheets that do not overlap
	// and all have different widths and heights.
	//
	// The great thing about this example with two dimensions is that it can be used for N dimensions, where we break
	// down the ranges until we get a perfectly overlapping range, and then merge (a single dimension at a time) all
	// edge-adjacent ranges until we arrive at a set of ranges that do not overlap and cannot be combined.

	// If the two ranges may be merged then we just do that and return.
	// Also allows us to not have to worry about the case where every column is equivalent.
	if mergedRange, ok, err := rang.TryMerge(otherRange); err != nil {
		return nil, false, err
	} else if ok {
		return []Range{mergedRange}, true, nil
	}
	// We check for overlapping after checking for merge as two ranges may not overlap but may be mergeable.
	// This would occur if all other columns are equivalent except for one column that is overlapping or adjacent.
	if ok, err := rang.Overlaps(otherRange); err != nil || !ok {
		return []Range{rang, otherRange}, false, err
	}

	var ranges []Range
	for i := range rang {
		if ok, err := rang[i].Equals(otherRange[i]); err != nil {
			return nil, false, err
		} else if ok {
			continue
		}
		// Get the RangeColumnExpr that overlaps both RangeColumnExprs
		overlapExpr, _, err := rang[i].Overlaps(otherRange[i])
		if err != nil {
			return nil, false, err
		}
		// Subtract the overlapping range from each existing range.
		// This will give us a collection of ranges that do not have any overlap.
		range1Subtracted, err := rang[i].Subtract(overlapExpr)
		if err != nil {
			return nil, false, err
		}
		for _, newColExpr := range range1Subtracted {
			ranges = append(ranges, rang.replace(i, newColExpr))
		}
		range2Subtracted, err := otherRange[i].Subtract(overlapExpr)
		if err != nil {
			return nil, false, err
		}
		for _, newColExpr := range range2Subtracted {
			ranges = append(ranges, otherRange.replace(i, newColExpr))
		}
		// Create two ranges that replace each respective RangeColumnExpr with the overlapping one, giving us two
		// ranges that are guaranteed to overlap (and are a subset of the originals). We can then recursively call this
		// function on the new overlapping ranges which will eventually return a set of non-overlapping ranges.
		newRanges, _, err := rang.replace(i, overlapExpr).RemoveOverlap(otherRange.replace(i, overlapExpr))
		if err != nil {
			return nil, false, err
		}
		ranges = append(ranges, newRanges...)
		break
	}

	return ranges, true, nil
}

// String returns this Range as a string for display purposes.
func (rang Range) String() string {
	sb := strings.Builder{}
	sb.WriteByte('{')
	for i, colExpr := range rang {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(colExpr.String())
	}
	sb.WriteByte('}')
	return sb.String()
}

// DebugString returns this Range as a string for debugging purposes.
func (rang Range) DebugString() string {
	sb := strings.Builder{}
	sb.WriteByte('{')
	for i, colExpr := range rang {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(colExpr.DebugString())
	}
	sb.WriteByte('}')
	return sb.String()
}

// replace returns a new Range with the column at the given index replaced by the given RangeColumnExpr. Does NOT
// perform any validation checks such as the index being within the bounds of the Range or the RangeColumnExpr having
// the same type as the other columns, so use with caution.
func (rang Range) replace(i int, colExpr RangeColumnExpr) Range {
	newRange := rang.Copy()
	newRange[i] = colExpr
	return newRange
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

// RemoveOverlappingRanges removes all overlap between all ranges.
func RemoveOverlappingRanges(ranges ...Range) (RangeCollection, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	// There are more efficient ways to do these comparisons, but this is just a simple implementation for now
	var newRanges RangeCollection
	for i := 0; i < len(ranges); i++ {
		hadOverlap := false
		for nri := 0; nri < len(newRanges); nri++ {
			if resultingRanges, ok, err := ranges[i].RemoveOverlap(newRanges[nri]); err != nil {
				return nil, err
			} else if ok {
				hadOverlap = true
				// Remove the overlapping Range from newRanges
				nrLast := len(newRanges) - 1
				newRanges[nri], newRanges[nrLast] = newRanges[nrLast], newRanges[nri]
				newRanges = newRanges[:nrLast]
				// Add the new ranges to the end of the given slice allowing us to compare those against everything else.
				ranges = append(ranges, resultingRanges...)
				break
			}
		}
		if !hadOverlap {
			newRanges = append(newRanges, ranges[i])
		}
	}

	return newRanges, nil
}
