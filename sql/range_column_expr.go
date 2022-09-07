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
	"sort"
	"strings"
)

// RangeType returns what a RangeColumnExpr represents, such as a GreaterThan on some column, or a column set between
// two bounds.
type RangeType int

const (
	RangeType_Invalid           RangeType = iota // This range is invalid, which should not be possible. Please create a GitHub issue if this is ever returned.
	RangeType_Empty                              // This range represents the empty set of values.
	RangeType_All                                // This range represents every possible value.
	RangeType_GreaterThan                        // This range is equivalent to checking for all values greater than the lowerbound.
	RangeType_GreaterOrEqual                     // This range is equivalent to checking for all values greater than or equal to the lowerbound.
	RangeType_LessThanOrNull                     // This range is equivalent to checking for all values less than the upperbound.
	RangeType_LessOrEqualOrNull                  // This range is equivalent to checking for all values less than or equal to the upperbound.
	RangeType_ClosedClosed                       // This range covers a finite set of values with the lower and upperbounds inclusive.
	RangeType_OpenOpen                           // This range covers a finite set of values with the lower and upperbounds exclusive.
	RangeType_OpenClosed                         // This range covers a finite set of values with the lowerbound exclusive and upperbound inclusive.
	RangeType_ClosedOpen                         // This range covers a finite set of values with the lowerbound inclusive and upperbound exclusive.
	RangeType_EqualNull                          // A range matching only NULL.
)

// RangeColumnExpr represents the contiguous set of values on a specific column.
type RangeColumnExpr struct {
	LowerBound RangeCut
	UpperBound RangeCut
	Typ        Type
}

// OpenRangeColumnExpr returns a RangeColumnExpr representing {l < x < u}.
func OpenRangeColumnExpr(lower, upper interface{}, typ Type) RangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		Above{Key: lower},
		Below{Key: upper},
		typ,
	}
}

// ClosedRangeColumnExpr returns a RangeColumnExpr representing {l <= x <= u}.
func ClosedRangeColumnExpr(lower, upper interface{}, typ Type) RangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		Below{Key: lower},
		Above{Key: upper},
		typ,
	}
}

// CustomRangeColumnExpr returns a RangeColumnExpr defined by the bounds given.
func CustomRangeColumnExpr(lower, upper interface{}, lowerBound, upperBound RangeBoundType, typ Type) RangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	var lCut RangeCut
	var uCut RangeCut
	if lowerBound == Open {
		lCut = Above{Key: lower}
	} else {
		lCut = Below{Key: lower}
	}
	if upperBound == Open {
		uCut = Below{Key: upper}
	} else {
		uCut = Above{Key: upper}
	}
	return RangeColumnExpr{
		lCut,
		uCut,
		typ,
	}
}

// LessThanRangeColumnExpr returns a RangeColumnExpr representing {x < u}.
func LessThanRangeColumnExpr(upper interface{}, typ Type) RangeColumnExpr {
	if upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		AboveNull{},
		Below{Key: upper},
		typ,
	}
}

// LessOrEqualRangeColumnExpr returns a RangeColumnExpr representing  {x <= u}.
func LessOrEqualRangeColumnExpr(upper interface{}, typ Type) RangeColumnExpr {
	if upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		AboveNull{},
		Above{Key: upper},
		typ,
	}
}

// GreaterThanRangeColumnExpr returns a RangeColumnExpr representing {x > l}.
func GreaterThanRangeColumnExpr(lower interface{}, typ Type) RangeColumnExpr {
	if lower == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		Above{Key: lower},
		AboveAll{},
		typ,
	}
}

// GreaterOrEqualRangeColumnExpr returns a RangeColumnExpr representing {x >= l}.
func GreaterOrEqualRangeColumnExpr(lower interface{}, typ Type) RangeColumnExpr {
	if lower == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return RangeColumnExpr{
		Below{Key: lower},
		AboveAll{},
		typ,
	}
}

// AllRangeColumnExpr returns a RangeColumnExpr representing all values.
func AllRangeColumnExpr(typ Type) RangeColumnExpr {
	return RangeColumnExpr{
		BelowNull{},
		AboveAll{},
		typ,
	}
}

// EmptyRangeColumnExpr returns the empty RangeColumnExpr for the given type.
func EmptyRangeColumnExpr(typ Type) RangeColumnExpr {
	return RangeColumnExpr{
		AboveAll{},
		AboveAll{},
		typ,
	}
}

// NullRangeColumnExpr returns the null RangeColumnExpr for the given type.
func NullRangeColumnExpr(typ Type) RangeColumnExpr {
	return RangeColumnExpr{
		LowerBound: BelowNull{},
		UpperBound: AboveNull{},
		Typ:        typ,
	}
}

// NotNullRangeColumnExpr returns the not null RangeColumnExpr for the given type.
func NotNullRangeColumnExpr(typ Type) RangeColumnExpr {
	return RangeColumnExpr{
		AboveNull{},
		AboveAll{},
		typ,
	}
}

// Equals checks for equality with the given RangeColumnExpr.
func (r RangeColumnExpr) Equals(other RangeColumnExpr) (bool, error) {
	cmpLower, err := r.LowerBound.Compare(other.LowerBound, r.Typ)
	if err != nil {
		return false, err
	}
	cmpUpper, err := r.UpperBound.Compare(other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	return cmpLower == 0 && cmpUpper == 0, nil
}

// HasLowerBound returns whether this RangeColumnExpr has a value for the lower bound.
func (r RangeColumnExpr) HasLowerBound() bool {
	return RangeCutIsBinding(r.LowerBound)
}

// HasUpperBound returns whether this RangeColumnExpr has a value for the upper bound.
func (r RangeColumnExpr) HasUpperBound() bool {
	return RangeCutIsBinding(r.UpperBound)
}

// IsEmpty returns whether this RangeColumnExpr is empty.
func (r RangeColumnExpr) IsEmpty() (bool, error) {
	cmp, err := r.LowerBound.Compare(r.UpperBound, r.Typ)
	return cmp >= 0, err
}

// IsConnected evaluates whether the given RangeColumnExpr overlaps or is adjacent to the calling RangeColumnExpr.
func (r RangeColumnExpr) IsConnected(other RangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}
	comp, err := r.LowerBound.Compare(other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	if comp > 0 {
		return false, nil
	}
	comp, err = other.LowerBound.Compare(r.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	return comp <= 0, nil
}

// Overlaps evaluates whether the given RangeColumnExpr overlaps the calling RangeColumnExpr. If they do, returns the
// overlapping region as a RangeColumnExpr.
func (r RangeColumnExpr) Overlaps(other RangeColumnExpr) (RangeColumnExpr, bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return EmptyRangeColumnExpr(r.Typ), false, nil
	}
	comp, err := r.LowerBound.Compare(other.UpperBound, r.Typ)
	if err != nil || comp >= 0 {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	comp, err = other.LowerBound.Compare(r.UpperBound, r.Typ)
	if err != nil || comp >= 0 {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	lowerbound, err := GetRangeCutMax(r.Typ, r.LowerBound, other.LowerBound)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	upperbound, err := GetRangeCutMin(r.Typ, r.UpperBound, other.UpperBound)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	return RangeColumnExpr{
		LowerBound: lowerbound,
		UpperBound: upperbound,
		Typ:        r.Typ,
	}, true, nil
}

// Subtract removes the given RangeColumnExpr from the calling RangeColumnExpr. In the event that the given
// RangeColumnExpr is a strict subset of the calling RangeColumnExpr, two RangeColumnExprs will be returned. If the
// given RangeColumnExpr does not overlap the calling RangeColumnExpr, then the calling RangeColumnExpr is returned.
// If the calling RangeColumnExpr is a strict subset (or equivalent) of the given RangeColumnExpr, then an empty slice
// is returned. In all other cases, a slice with a single RangeColumnExpr will be returned.
func (r RangeColumnExpr) Subtract(other RangeColumnExpr) ([]RangeColumnExpr, error) {
	_, overlaps, err := r.Overlaps(other)
	if err != nil {
		return nil, err
	}
	if !overlaps {
		return []RangeColumnExpr{r}, nil
	}
	lComp, err := r.LowerBound.Compare(other.LowerBound, r.Typ)
	if err != nil {
		return nil, err
	}
	uComp, err := r.UpperBound.Compare(other.UpperBound, r.Typ)
	if err != nil {
		return nil, err
	}
	// Each bound, when compared to the other, has 3 possible states: less (-1), equal (0), or greater (1).
	// As there are two bounds (upper and lower), that gives us 9 total combinations.
	// To make use of a switch statement (avoiding 9 if-else statements), we can convert the states to an integer.
	// Adding 1 to each bound moves the lowest value to 0 and highest to 2, so we can use it as a trit (ternary "bit").
	switch (3 * (lComp + 1)) + (uComp + 1) {
	case 0: // lComp == -1 && uComp == -1
		return []RangeColumnExpr{{r.LowerBound, other.LowerBound, r.Typ}}, nil
	case 1: // lComp == -1 && uComp == 0
		return []RangeColumnExpr{{r.LowerBound, other.LowerBound, r.Typ}}, nil
	case 2: // lComp == -1 && uComp == 1
		return []RangeColumnExpr{
			{r.LowerBound, other.LowerBound, r.Typ},
			{other.UpperBound, r.UpperBound, r.Typ},
		}, nil
	case 3: // lComp == 0  && uComp == -1
		return nil, nil
	case 4: // lComp == 0  && uComp == 0
		return nil, nil
	case 5: // lComp == 0  && uComp == 1
		return []RangeColumnExpr{{other.UpperBound, r.UpperBound, r.Typ}}, nil
	case 6: // lComp == 1  && uComp == -1
		return nil, nil
	case 7: // lComp == 1  && uComp == 0
		return nil, nil
	case 8: // lComp == 1  && uComp == 1
		return []RangeColumnExpr{{other.UpperBound, r.UpperBound, r.Typ}}, nil
	default: // should never be hit
		panic(fmt.Errorf("unknown RangeColumnExpr subtraction case: %d", (3*(lComp+1))+(uComp+1)))
	}
}

// IsSubsetOf evaluates whether the calling RangeColumnExpr is fully encompassed by the given RangeColumnExpr.
func (r RangeColumnExpr) IsSubsetOf(other RangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}
	comp, err := r.LowerBound.Compare(other.LowerBound, r.Typ)
	if err != nil || comp == -1 {
		return false, err
	}
	comp, err = r.UpperBound.Compare(other.UpperBound, r.Typ)
	if err != nil || comp == 1 {
		return false, err
	}
	return true, nil
}

// IsSupersetOf evaluates whether the calling RangeColumnExpr fully encompasses the given RangeColumnExpr.
func (r RangeColumnExpr) IsSupersetOf(other RangeColumnExpr) (bool, error) {
	return other.IsSubsetOf(r)
}

// String returns this RangeColumnExpr as a string for display purposes.
func (r RangeColumnExpr) String() string {
	return fmt.Sprintf("(%s, %s)", r.LowerBound.String(), r.UpperBound.String())
}

// DebugString returns this RangeColumnExpr as a string for debugging purposes.
func (r RangeColumnExpr) DebugString() string {
	sb := strings.Builder{}
	switch r.LowerBound.(type) {
	case Above:
		sb.WriteString("(" + fmt.Sprint(GetRangeCutKey(r.LowerBound)))
	case Below:
		sb.WriteString("[" + fmt.Sprint(GetRangeCutKey(r.LowerBound)))
	case AboveAll:
		sb.WriteString("(∞")
	case AboveNull:
		sb.WriteString("(NULL")
	case BelowNull:
		sb.WriteString("[NULL")
	}
	sb.WriteString(", ")
	switch r.UpperBound.(type) {
	case Above:
		sb.WriteString(fmt.Sprint(GetRangeCutKey(r.UpperBound)) + "]")
	case Below:
		sb.WriteString(fmt.Sprint(GetRangeCutKey(r.UpperBound)) + ")")
	case AboveAll:
		sb.WriteString("∞)")
	case AboveNull:
		sb.WriteString("NULL]")
	case BelowNull:
		sb.WriteString("NULL)")
	}
	return sb.String()
}

// TryIntersect attempts to intersect the given RangeColumnExpr with the calling RangeColumnExpr. Returns true if the
// intersection result is not the empty RangeColumnExpr, however a valid RangeColumnExpr is always returned if the error
// is nil.
func (r RangeColumnExpr) TryIntersect(other RangeColumnExpr) (RangeColumnExpr, bool, error) {
	_, l, err := OrderedCuts(r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	u, _, err := OrderedCuts(r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	comp, err := l.Compare(u, r.Typ)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	if comp < 0 {
		return RangeColumnExpr{l, u, r.Typ}, true, nil
	}
	return EmptyRangeColumnExpr(r.Typ), false, nil
}

// TryUnion attempts to combine the given RangeColumnExpr with the calling RangeColumnExpr. Returns true if the union
// was a success.
func (r RangeColumnExpr) TryUnion(other RangeColumnExpr) (RangeColumnExpr, bool, error) {
	if isEmpty, err := other.IsEmpty(); err != nil {
		return RangeColumnExpr{}, false, err
	} else if isEmpty {
		return r, true, nil
	}
	if isEmpty, err := r.IsEmpty(); err != nil {
		return RangeColumnExpr{}, false, err
	} else if isEmpty {
		return other, true, nil
	}
	connected, err := r.IsConnected(other)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	if !connected {
		return RangeColumnExpr{}, false, nil
	}
	l, _, err := OrderedCuts(r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	_, u, err := OrderedCuts(r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return RangeColumnExpr{}, false, err
	}
	return RangeColumnExpr{l, u, r.Typ}, true, nil
}

// Type returns this RangeColumnExpr's RangeType.
func (r RangeColumnExpr) Type() RangeType {
	switch r.LowerBound.(type) {
	case Above:
		switch r.UpperBound.(type) {
		case Above:
			return RangeType_OpenClosed
		case AboveAll:
			return RangeType_GreaterThan
		case Below:
			return RangeType_OpenOpen
		}
	case AboveAll:
		switch r.UpperBound.(type) {
		case AboveAll:
			return RangeType_Empty
		}
	case Below:
		switch r.UpperBound.(type) {
		case Above:
			return RangeType_ClosedClosed
		case AboveAll:
			return RangeType_GreaterOrEqual
		case Below:
			return RangeType_ClosedOpen
		}
	case AboveNull:
		switch r.UpperBound.(type) {
		case Above:
			return RangeType_OpenClosed
		case AboveAll:
			// TODO: NotNull?
			return RangeType_GreaterThan
		case Below:
			return RangeType_OpenOpen
		case AboveNull:
			return RangeType_Empty
		}
	case BelowNull:
		switch r.UpperBound.(type) {
		case Above:
			return RangeType_LessOrEqualOrNull
		case AboveAll:
			return RangeType_All
		case Below:
			return RangeType_LessThanOrNull
		case AboveNull:
			return RangeType_EqualNull
		case BelowNull:
			return RangeType_Empty
		}
	}
	return RangeType_Invalid
}

// RepresentsEquals returns whether this RangeColumnExpr represents an "equals". An "equals" is a special kind of
// RangeType_ClosedClosed that iterates over a single value (or the specific prefix of some value).
func (r RangeColumnExpr) RepresentsEquals() (bool, error) {
	if r.Type() == RangeType_ClosedClosed {
		cmp, err := r.Typ.Compare(GetRangeCutKey(r.LowerBound), GetRangeCutKey(r.UpperBound))
		if err != nil {
			return false, err
		}
		return cmp == 0, nil
	}
	return false, nil
}

// OrderedCuts returns the given Cuts in order from lowest-touched values to highest-touched values.
func OrderedCuts(l, r RangeCut, typ Type) (RangeCut, RangeCut, error) {
	comp, err := l.Compare(r, typ)
	if err != nil {
		return nil, nil, err
	}
	if comp <= 0 {
		return l, r, nil
	}
	return r, l, nil
}

// rangeColumnExprSlice is a sortable slice of RangeColumnExprs.
type rangeColumnExprSlice struct {
	ranges []RangeColumnExpr
	err    error
}

func (r *rangeColumnExprSlice) Len() int      { return len(r.ranges) }
func (r *rangeColumnExprSlice) Swap(i, j int) { r.ranges[i], r.ranges[j] = r.ranges[j], r.ranges[i] }
func (r *rangeColumnExprSlice) Less(i, j int) bool {
	lc, err := r.ranges[i].LowerBound.Compare(r.ranges[j].LowerBound, r.ranges[i].Typ)
	if err != nil {
		r.err = err
		return false
	}
	if lc < 0 {
		return true
	} else if lc > 0 {
		return false
	}
	uc, err := r.ranges[i].UpperBound.Compare(r.ranges[j].UpperBound, r.ranges[i].Typ)
	if err != nil {
		r.err = err
		return false
	}
	return uc < 0
}

// SimplifyRangeColumn combines all RangeColumnExprs that are connected and returns a new slice.
func SimplifyRangeColumn(rces ...RangeColumnExpr) ([]RangeColumnExpr, error) {
	if len(rces) == 0 {
		return rces, nil
	}
	typ := rces[0].Typ
	for i := 1; i < len(rces); i++ {
		if typ.Type() != rces[i].Typ.Type() {
			return nil, fmt.Errorf("may only simplify ranges that share the same type")
		}
	}
	sorted := make([]RangeColumnExpr, len(rces))
	copy(sorted, rces)
	rSlice := &rangeColumnExprSlice{ranges: sorted}
	sort.Sort(rSlice)
	if rSlice.err != nil {
		return nil, rSlice.err
	}
	var res []RangeColumnExpr
	cur := EmptyRangeColumnExpr(rces[0].Typ)
	for _, r := range sorted {
		merged, ok, err := cur.TryUnion(r)
		if err != nil {
			return nil, err
		}
		if ok {
			cur = merged
		} else if curIsEmpty, err := cur.IsEmpty(); err != nil {
			return nil, err
		} else if !curIsEmpty {
			res = append(res, cur)
			cur = r
		}
	}
	if curIsEmpty, err := cur.IsEmpty(); err != nil {
		return nil, err
	} else if !curIsEmpty {
		res = append(res, cur)
	}
	return res, nil
}
