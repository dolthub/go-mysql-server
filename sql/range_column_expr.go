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

// RangeType returns what a MySQLRangeColumnExpr represents, such as a GreaterThan on some column, or a column set between
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

// MySQLRangeColumnExpr represents the contiguous set of values on a specific column.
type MySQLRangeColumnExpr struct {
	// TODO: rename to just lower/upper
	LowerBound *Bound
	UpperBound *Bound
	Typ        Type
}

// OpenRangeColumnExpr returns a MySQLRangeColumnExpr representing {l < x < u}.
func OpenRangeColumnExpr(lower, upper interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, false),
		UpperBound: NewBound(upper, false),
		Typ:        typ,
	}
}

// ClosedRangeColumnExpr returns a MySQLRangeColumnExpr representing {l <= x <= u}.
func ClosedRangeColumnExpr(lower, upper interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, true),
		UpperBound: NewBound(upper, true),
		Typ:        typ,
	}
}

// LessThanRangeColumnExpr returns a MySQLRangeColumnExpr representing {x < u}.
func LessThanRangeColumnExpr(upper interface{}, typ Type) MySQLRangeColumnExpr {
	if upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewAboveNullBound(),
		UpperBound: NewBound(upper, false),
		Typ:        typ,
	}
}

// LessOrEqualRangeColumnExpr returns a MySQLRangeColumnExpr representing  {x <= u}.
func LessOrEqualRangeColumnExpr(upper interface{}, typ Type) MySQLRangeColumnExpr {
	if upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewAboveNullBound(),
		UpperBound: NewBound(upper, false),
		Typ:        typ,
	}
}

// GreaterThanRangeColumnExpr returns a MySQLRangeColumnExpr representing {x > l}.
func GreaterThanRangeColumnExpr(lower interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, false),
		UpperBound: NewAboveAllBound(),
		Typ:        typ,
	}
}

// GreaterOrEqualRangeColumnExpr returns a MySQLRangeColumnExpr representing {x >= l}.
func GreaterOrEqualRangeColumnExpr(lower interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, true),
		UpperBound: NewAboveAllBound(),
		Typ:        typ,
	}
}

// AllRangeColumnExpr returns a MySQLRangeColumnExpr representing all values.
func AllRangeColumnExpr(typ Type) MySQLRangeColumnExpr {
	return MySQLRangeColumnExpr{
		LowerBound: NewBelowNullBound(),
		UpperBound: NewAboveAllBound(),
		Typ:        typ,
	}
}

// EmptyRangeColumnExpr returns the empty MySQLRangeColumnExpr for the given type.
func EmptyRangeColumnExpr(typ Type) MySQLRangeColumnExpr {
	// TODO: Should just be nil and nil?
	return MySQLRangeColumnExpr{
		LowerBound: NewAboveAllBound(),
		UpperBound: NewAboveAllBound(),
		Typ:        typ,
	}
}

// NullRangeColumnExpr returns the null MySQLRangeColumnExpr for the given type.
func NullRangeColumnExpr(typ Type) MySQLRangeColumnExpr {
	return MySQLRangeColumnExpr{
		LowerBound: NewBelowNullBound(),
		UpperBound: NewAboveNullBound(),
		Typ:        typ,
	}
}

// NotNullRangeColumnExpr returns the not null MySQLRangeColumnExpr for the given type.
func NotNullRangeColumnExpr(typ Type) MySQLRangeColumnExpr {
	return MySQLRangeColumnExpr{
		LowerBound: NewAboveNullBound(),
		UpperBound: NewAboveAllBound(),
		Typ:        typ,
	}
}

// Equals checks for equality with the given MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) Equals(other MySQLRangeColumnExpr) (bool, error) {
	cmpLower, err := r.LowerBound.Compare(nil, other.LowerBound, r.Typ)
	if err != nil {
		return false, err
	}
	cmpUpper, err := r.UpperBound.Compare(nil, other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	return cmpLower == 0 && cmpUpper == 0, nil
}

// HasLowerBound returns whether this MySQLRangeColumnExpr has a value for the lower bound.
func (r MySQLRangeColumnExpr) HasLowerBound() bool {
	return r.LowerBound.BoundType == Binding
}

// HasUpperBound returns whether this MySQLRangeColumnExpr has a value for the upper bound.
func (r MySQLRangeColumnExpr) HasUpperBound() bool {
	return r.UpperBound.BoundType == Binding
}

// IsEmpty returns whether this MySQLRangeColumnExpr is empty.
func (r MySQLRangeColumnExpr) IsEmpty(ctx *Context) (bool, error) {
	cmp, err := r.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	return cmp >= 0, err
}

// IsConnected evaluates whether the given MySQLRangeColumnExpr overlaps or is adjacent to the calling MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) IsConnected(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() { // TODO: is this faster than type.Equals()?
		return false, nil
	}

	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	if cmp > 0 {
		return false, nil
	}
	if cmp == 0 {
		return r.LowerBound.Inclusive || other.UpperBound.Inclusive, nil
	}

	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	if cmp > 0 {
		return false, nil
	}
	if cmp == 0 {
		return other.LowerBound.Inclusive || r.UpperBound.Inclusive, nil
	}

	return true, nil
}

func MaxLowerBound(ctx *Context, b1, b2 *Bound, typ Type) (*Bound, error) {
	cmp, err := b1.Compare(ctx, b2, typ)
	if err != nil {
		return nil, err
	}
	if cmp == 1 {
		return b1, nil
	}
	if cmp == -1 {
		return b2, nil
	}
	if !b1.Inclusive {
		return b1, nil
	}
	return b2, nil
}

func MinUpperBound(ctx *Context, b1, b2 *Bound, typ Type) (*Bound, error) {
	cmp, err := b1.Compare(ctx, b2, typ)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if cmp == -1 {
		return b1, nil
	}
	if cmp == 1 {
		return b2, nil
	}
	if !b1.Inclusive {
		return b1, nil
	}
	return b2, nil
}

// Overlaps evaluates whether the calling MySQLRangeColumnExpr overlaps the given MySQLRangeColumnExpr.
// If they do, return the overlapping region as a MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) Overlaps(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}

	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil || cmp > 0 {
		return false, err
	}
	// Adjacent bounds are not overlapping
	if cmp == 0 {
		return r.LowerBound.Inclusive && other.UpperBound.Inclusive, nil
	}

	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil || cmp > 0 {
		return false, err
	}
	// Adjacent bounds are not overlapping
	if cmp == 0 {
		return r.LowerBound.Inclusive && other.UpperBound.Inclusive, nil
	}

	return true, nil
}

// FindOverlap will return a range containing the overlap of the calling Range and other Range
// TODO: use pointers
func (r MySQLRangeColumnExpr) FindOverlap(ctx *Context, other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return EmptyRangeColumnExpr(r.Typ), false, nil
	}

	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil || cmp == 1 {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	if cmp == 0 {
		if !r.LowerBound.Inclusive || !other.UpperBound.Inclusive {
			return EmptyRangeColumnExpr(r.Typ), false, nil
		}
		return ClosedRangeColumnExpr(r.LowerBound.Key, other.UpperBound.Key, r.Typ), true, nil
	}

	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil || cmp == 1 {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	if cmp == 0 {
		if !other.LowerBound.Inclusive || !r.UpperBound.Inclusive {
			return EmptyRangeColumnExpr(r.Typ), false, nil
		}
		return ClosedRangeColumnExpr(other.LowerBound.Key, r.UpperBound.Key, r.Typ), true, nil
	}

	lower, err := MaxLowerBound(ctx, r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}
	upper, err := MinUpperBound(ctx, r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), false, err
	}

	return MySQLRangeColumnExpr{
		LowerBound: lower,
		UpperBound: upper,
		Typ:        r.Typ,
	}, true, nil
}

// Subtract removes the given MySQLRangeColumnExpr from the calling MySQLRangeColumnExpr. In the event that the given
// MySQLRangeColumnExpr is a strict subset of the calling MySQLRangeColumnExpr, two RangeColumnExprs will be returned. If the
// given MySQLRangeColumnExpr does not overlap the calling MySQLRangeColumnExpr, then the calling MySQLRangeColumnExpr is returned.
// If the calling MySQLRangeColumnExpr is a strict subset (or equivalent) of the given MySQLRangeColumnExpr, then an empty slice
// is returned. In all other cases, a slice with a single MySQLRangeColumnExpr will be returned.
func (r MySQLRangeColumnExpr) Subtract(ctx *Context, other MySQLRangeColumnExpr) ([]MySQLRangeColumnExpr, error) {
	overlaps, err := r.Overlaps(ctx, other)
	if err != nil {
		return nil, err
	}
	if !overlaps {
		return []MySQLRangeColumnExpr{r}, nil
	}

	lCmp, err := r.LowerBound.Compare(ctx, other.LowerBound, r.Typ)
	if err != nil {
		return nil, err
	}
	uCmp, err := r.UpperBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil {
		return nil, err
	}

	var res []MySQLRangeColumnExpr
	switch lCmp {
	case -1:
		res = append(res, MySQLRangeColumnExpr{
			LowerBound: r.LowerBound,
			UpperBound: other.LowerBound.Invert(),
			Typ:        r.Typ,
		})
		switch uCmp {
		case 0:
			if r.UpperBound.Inclusive && !other.UpperBound.Inclusive {
				res = append(res, MySQLRangeColumnExpr{
					LowerBound: r.UpperBound,
					UpperBound: r.UpperBound,
					Typ:        r.Typ,
				})
			}
		case 1:
			res = append(res, MySQLRangeColumnExpr{
				LowerBound: other.UpperBound.Invert(),
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			})
		}
	case 0:
		if r.LowerBound.Inclusive && !other.LowerBound.Inclusive {
			res = append(res, MySQLRangeColumnExpr{
				LowerBound: r.LowerBound,
				UpperBound: r.LowerBound,
				Typ:        r.Typ,
			})
		}
		switch uCmp {
		case 0:
			if r.UpperBound.Inclusive && !other.UpperBound.Inclusive {
				res = append(res, MySQLRangeColumnExpr{
					LowerBound: r.UpperBound,
					UpperBound: r.UpperBound,
					Typ:        r.Typ,
				})
			}
		case 1:
			res = append(res, MySQLRangeColumnExpr{
				LowerBound: other.UpperBound.Invert(),
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			})
		}
	case 1:
		switch uCmp {
		case 0:
			if r.UpperBound.Inclusive && !other.UpperBound.Inclusive {
				res = append(res, MySQLRangeColumnExpr{
					LowerBound: r.UpperBound,
					UpperBound: r.UpperBound,
					Typ:        r.Typ,
				})
			}
		case 1:
			res = append(res, MySQLRangeColumnExpr{
				LowerBound: other.UpperBound.Invert(),
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			})
		}
	}
	return res, nil
}

// IsSubsetOf evaluates whether the calling MySQLRangeColumnExpr is fully encompassed by the given MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) IsSubsetOf(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}
	cmp, err := r.LowerBound.Compare(ctx, other.LowerBound, r.Typ)
	if err != nil || cmp == -1 {
		return false, err
	}
	if cmp == 0 {
		return r.LowerBound.Inclusive, nil
	}
	comp, err = r.UpperBound.Compare(other.UpperBound, r.Typ)
	if err != nil || comp == 1 {
		return false, err
	}
	return true, nil
}

// IsSupersetOf evaluates whether the calling MySQLRangeColumnExpr fully encompasses the given MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) IsSupersetOf(other MySQLRangeColumnExpr) (bool, error) {
	return other.IsSubsetOf(r)
}

// String returns this MySQLRangeColumnExpr as a string for display purposes.
func (r MySQLRangeColumnExpr) String() string {
	return fmt.Sprintf("(%s, %s)", r.LowerBound.String(), r.UpperBound.String())
}

// DebugString returns this MySQLRangeColumnExpr as a string for debugging purposes.
func (r MySQLRangeColumnExpr) DebugString() string {
	var lowerB interface{} = "-∞"
	if MySQLRangeCutIsBinding(r.LowerBound) {
		lowerB = GetMySQLRangeCutKey(r.LowerBound)
	}
	var upperB interface{} = "∞"
	if MySQLRangeCutIsBinding(r.UpperBound) {
		upperB = GetMySQLRangeCutKey(r.UpperBound)
	}
	switch v := lowerB.(type) {
	case []byte:
		lowerB = string(v)
	}
	switch v := upperB.(type) {
	case []byte:
		upperB = string(v)
	}

	sb := strings.Builder{}
	switch r.LowerBound.(type) {
	case Above:
		lowerB := GetMySQLRangeCutKey(r.LowerBound)
		sb.WriteString("(" + fmt.Sprint(lowerB))
	case Below:
		lowerB := GetMySQLRangeCutKey(r.LowerBound)
		sb.WriteString("[" + fmt.Sprint(lowerB))
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
		upperB := GetMySQLRangeCutKey(r.UpperBound)
		sb.WriteString(fmt.Sprint(upperB) + "]")
	case Below:
		upperB := GetMySQLRangeCutKey(r.UpperBound)
		sb.WriteString(fmt.Sprint(upperB) + ")")
	case AboveAll:
		sb.WriteString("∞)")
	case AboveNull:
		sb.WriteString("NULL]")
	case BelowNull:
		sb.WriteString("NULL)")
	}
	return sb.String()
}

// TryIntersect attempts to intersect the given MySQLRangeColumnExpr with the calling MySQLRangeColumnExpr. Returns true if the
// intersection result is not the empty MySQLRangeColumnExpr, however a valid MySQLRangeColumnExpr is always returned if the error
// is nil.
func (r MySQLRangeColumnExpr) TryIntersect(other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, bool, error) {
	_, l, err := OrderedCuts(r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	u, _, err := OrderedCuts(r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	comp, err := l.Compare(u, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	if comp < 0 {
		return MySQLRangeColumnExpr{l, u, r.Typ}, true, nil
	}
	return EmptyRangeColumnExpr(r.Typ), false, nil
}

// TryUnion attempts to combine the given MySQLRangeColumnExpr with the calling MySQLRangeColumnExpr. Returns true if the union
// was a success.
func (r MySQLRangeColumnExpr) TryUnion(other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, bool, error) {
	if isEmpty, err := other.IsEmpty(); err != nil {
		return MySQLRangeColumnExpr{}, false, err
	} else if isEmpty {
		return r, true, nil
	}
	if isEmpty, err := r.IsEmpty(); err != nil {
		return MySQLRangeColumnExpr{}, false, err
	} else if isEmpty {
		return other, true, nil
	}
	connected, err := r.IsConnected(other)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	if !connected {
		return MySQLRangeColumnExpr{}, false, nil
	}
	l, _, err := OrderedCuts(r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	_, u, err := OrderedCuts(r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	return MySQLRangeColumnExpr{l, u, r.Typ}, true, nil
}

// Type returns this MySQLRangeColumnExpr's RangeType.
func (r MySQLRangeColumnExpr) Type() RangeType {
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

// OrderedCuts returns the given Cuts in order from lowest-touched values to highest-touched values.
func OrderedCuts(l, r MySQLRangeCut, typ Type) (MySQLRangeCut, MySQLRangeCut, error) {
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
	err    error
	ranges []MySQLRangeColumnExpr
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
func SimplifyRangeColumn(rces ...MySQLRangeColumnExpr) ([]MySQLRangeColumnExpr, error) {
	if len(rces) == 0 {
		return rces, nil
	}
	typ := rces[0].Typ
	for i := 1; i < len(rces); i++ {
		if typ.Type() != rces[i].Typ.Type() {
			return nil, fmt.Errorf("may only simplify ranges that share the same type")
		}
	}
	sorted := make([]MySQLRangeColumnExpr, len(rces))
	copy(sorted, rces)
	rSlice := &rangeColumnExprSlice{ranges: sorted}
	sort.Sort(rSlice)
	if rSlice.err != nil {
		return nil, rSlice.err
	}
	var res []MySQLRangeColumnExpr
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
