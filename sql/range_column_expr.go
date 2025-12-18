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
	"context"
	"fmt"
	"slices"
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
		LowerBound: NewBound(lower, Above),
		UpperBound: NewBound(upper, Below),
		Typ:        typ,
	}
}

// ClosedRangeColumnExpr returns a MySQLRangeColumnExpr representing {l <= x <= u}.
func ClosedRangeColumnExpr(lower, upper interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil || upper == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, Below),
		UpperBound: NewBound(upper, Above),
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
		UpperBound: NewBound(upper, Below),
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
		UpperBound: NewBound(upper, Above),
		Typ:        typ,
	}
}

// GreaterThanRangeColumnExpr returns a MySQLRangeColumnExpr representing {x > l}.
func GreaterThanRangeColumnExpr(lower interface{}, typ Type) MySQLRangeColumnExpr {
	if lower == nil {
		return EmptyRangeColumnExpr(typ)
	}
	return MySQLRangeColumnExpr{
		LowerBound: NewBound(lower, Above),
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
		LowerBound: NewBound(lower, Below),
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
func (r MySQLRangeColumnExpr) Equals(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	cmpL, err := r.LowerBound.Compare(ctx, other.LowerBound, r.Typ)
	if err != nil {
		return false, err
	}
	cmpU, err := r.UpperBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	return cmpL == 0 && cmpU == 0, nil
}

// IsEmpty returns whether this MySQLRangeColumnExpr is empty.
func (r MySQLRangeColumnExpr) IsEmpty(ctx context.Context) (bool, error) {
	cmp, err := r.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	return cmp >= 0, err
}

// IsConnected evaluates whether the given MySQLRangeColumnExpr overlaps or is adjacent to the calling MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) IsConnected(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}
	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	if cmp > 0 {
		return false, nil
	}
	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil {
		return false, err
	}
	return cmp <= 0, nil
}

// Overlaps evaluates whether the calling MySQLRangeColumnExpr overlaps the given MySQLRangeColumnExpr.
// If they do, return the overlapping region as a MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) Overlaps(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	if r.Typ.String() != other.Typ.String() {
		return false, nil
	}
	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil || cmp >= 0 {
		return false, err
	}
	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil || cmp >= 0 {
		return false, err
	}
	return true, nil
}

func MaxBound(ctx *Context, b1, b2 *Bound, typ Type) (*Bound, error) {
	cmp, err := b1.Compare(ctx, b2, typ)
	if err != nil {
		return nil, err
	}
	if cmp == 1 {
		return b1, nil
	}
	return b2, nil
}

func MinBound(ctx *Context, b1, b2 *Bound, typ Type) (*Bound, error) {
	cmp, err := b1.Compare(ctx, b2, typ)
	if err != nil {
		return nil, err
	}
	if cmp == -1 {
		return b1, nil
	}
	return b2, nil
}

// FindOverlap returns the overlapping region as a MySQLRangeColumnExpr.
// TODO: use pointers
func (r MySQLRangeColumnExpr) FindOverlap(ctx *Context, other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, error) {
	if r.Typ.String() != other.Typ.String() {
		return EmptyRangeColumnExpr(r.Typ), nil
	}

	cmp, err := r.LowerBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil || cmp >= 0 {
		return EmptyRangeColumnExpr(r.Typ), err
	}
	cmp, err = other.LowerBound.Compare(ctx, r.UpperBound, r.Typ)
	if err != nil || cmp >= 0 {
		return EmptyRangeColumnExpr(r.Typ), err
	}

	lower, err := MaxBound(ctx, r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), err
	}
	upper, err := MinBound(ctx, r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return EmptyRangeColumnExpr(r.Typ), err
	}

	return MySQLRangeColumnExpr{
		LowerBound: lower,
		UpperBound: upper,
		Typ:        r.Typ,
	}, nil
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
	cmpL, err := r.LowerBound.Compare(ctx, other.LowerBound, r.Typ)
	if err != nil {
		return nil, err
	}
	cmpU, err := r.UpperBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil {
		return nil, err
	}
	switch {
	case cmpL == -1 && cmpU == -1:
		return []MySQLRangeColumnExpr{
			{
				LowerBound: r.LowerBound,
				UpperBound: other.LowerBound,
				Typ:        r.Typ,
			},
		}, nil
	case cmpL == -1 && cmpU == 0:
		return []MySQLRangeColumnExpr{
			{
				LowerBound: r.LowerBound,
				UpperBound: other.LowerBound,
				Typ:        r.Typ,
			},
		}, nil
	case cmpL == -1 && cmpU == 1:
		return []MySQLRangeColumnExpr{
			{
				LowerBound: r.LowerBound,
				UpperBound: other.LowerBound,
				Typ:        r.Typ,
			},
			{
				LowerBound: other.UpperBound,
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			},
		}, nil
	case cmpL == 0 && cmpU == 1:
		return []MySQLRangeColumnExpr{
			{
				LowerBound: other.UpperBound,
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			},
		}, nil
	case cmpL == 1 && cmpU == 1:
		return []MySQLRangeColumnExpr{
			{
				LowerBound: other.UpperBound,
				UpperBound: r.UpperBound,
				Typ:        r.Typ,
			},
		}, nil
	default:
		// cmpL == 0 && cmpU == -1
		// cmpL == 0 && cmpU == 0
		// cmpL == 1 && cmpU == -1
		// cmpL == 1 && cmpU == 0
		return nil, nil
	}
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
	cmp, err = r.UpperBound.Compare(ctx, other.UpperBound, r.Typ)
	if err != nil || cmp == 1 {
		return false, err
	}
	return true, nil
}

// IsSupersetOf evaluates whether the calling MySQLRangeColumnExpr fully encompasses the given MySQLRangeColumnExpr.
func (r MySQLRangeColumnExpr) IsSupersetOf(ctx *Context, other MySQLRangeColumnExpr) (bool, error) {
	return other.IsSubsetOf(ctx, r)
}

// String returns this MySQLRangeColumnExpr as a string for display purposes.
func (r MySQLRangeColumnExpr) String() string {
	return "(" + r.LowerBound.String() + "," + r.UpperBound.String() + ")"
}

// DebugString returns this MySQLRangeColumnExpr as a string for debugging purposes.
func (r MySQLRangeColumnExpr) DebugString() string {
	var res string
	switch r.LowerBound.BoundType {
	case Above:
		res += fmt.Sprintf("(%v", r.LowerBound.Key)
	case Below:
		res += fmt.Sprintf("[%v", r.LowerBound.Key)
	case AboveAll:
		res += "(∞"
	case AboveNull:
		res += "(NULL"
	case BelowNull:
		res += "[NULL"
	}
	res += ", "
	switch r.UpperBound.BoundType {
	case Above:
		res += fmt.Sprintf("%v]", r.UpperBound.Key)
	case Below:
		res += fmt.Sprintf("%v)", r.UpperBound.Key)
	case AboveAll:
		res += "∞)"
	case AboveNull:
		res += "NULL]"
	case BelowNull:
		res += "NULL)"
	}
	return res
}

// TryIntersect attempts to intersect the given MySQLRangeColumnExpr with the calling MySQLRangeColumnExpr. Returns true if the
// intersection result is not the empty MySQLRangeColumnExpr, however a valid MySQLRangeColumnExpr is always returned if the error
// is nil.
func (r MySQLRangeColumnExpr) TryIntersect(ctx context.Context, other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, bool, error) {
	_, l, err := OrderedBounds(ctx, r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	u, _, err := OrderedBounds(ctx, r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	comp, err := l.Compare(ctx, u, r.Typ)
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
func (r MySQLRangeColumnExpr) TryUnion(ctx *Context, other MySQLRangeColumnExpr) (MySQLRangeColumnExpr, bool, error) {
	if isEmpty, err := other.IsEmpty(ctx); err != nil {
		return MySQLRangeColumnExpr{}, false, err
	} else if isEmpty {
		return r, true, nil
	}
	if isEmpty, err := r.IsEmpty(ctx); err != nil {
		return MySQLRangeColumnExpr{}, false, err
	} else if isEmpty {
		return other, true, nil
	}
	connected, err := r.IsConnected(ctx, other)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	if !connected {
		return MySQLRangeColumnExpr{}, false, nil
	}
	l, _, err := OrderedBounds(ctx, r.LowerBound, other.LowerBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	_, u, err := OrderedBounds(ctx, r.UpperBound, other.UpperBound, r.Typ)
	if err != nil {
		return MySQLRangeColumnExpr{}, false, err
	}
	return MySQLRangeColumnExpr{l, u, r.Typ}, true, nil
}

// Type returns this MySQLRangeColumnExpr's RangeType.
func (r MySQLRangeColumnExpr) Type() RangeType {
	switch r.LowerBound.BoundType {
	case Above:
		switch r.UpperBound.BoundType {
		case Above:
			return RangeType_OpenClosed
		case AboveAll:
			return RangeType_GreaterThan
		case Below:
			return RangeType_OpenOpen
		}
	case Below:
		switch r.UpperBound.BoundType {
		case Above:
			return RangeType_ClosedClosed
		case AboveAll:
			return RangeType_GreaterOrEqual
		case Below:
			return RangeType_ClosedOpen
		}
	case BelowNull:
		switch r.UpperBound.BoundType {
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
	case AboveNull:
		switch r.UpperBound.BoundType {
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
	case AboveAll:
		switch r.UpperBound.BoundType {
		case AboveAll:
			return RangeType_Empty
		}
	}
	return RangeType_Invalid
}

// OrderedBounds returns the given Cuts in order from lowest-touched values to highest-touched values.
func OrderedBounds(ctx context.Context, l, r *Bound, typ Type) (*Bound, *Bound, error) {
	cmp, err := l.Compare(ctx, r, typ)
	if err != nil {
		return nil, nil, err
	}
	if cmp <= 0 {
		return l, r, nil
	}
	return r, l, nil
}

// SimplifyRangeColumn combines all RangeColumnExprs that are connected and returns a new slice.
func SimplifyRangeColumn(ctx *Context, rngColExprs []MySQLRangeColumnExpr) ([]MySQLRangeColumnExpr, error) {
	if len(rngColExprs) == 0 {
		return rngColExprs, nil
	}
	typ := rngColExprs[0].Typ
	for i := 1; i < len(rngColExprs); i++ {
		if typ.Type() != rngColExprs[i].Typ.Type() {
			return nil, fmt.Errorf("may only simplify ranges that share the same type")
		}
	}

	var sortErr error
	slices.SortFunc(rngColExprs, func(a, b MySQLRangeColumnExpr) int {
		cmpL, err := a.LowerBound.Compare(ctx, b.LowerBound, typ)
		if err != nil {
			sortErr = err
			return 0
		}
		if cmpL != 0 {
			return cmpL
		}
		cmpU, err := a.UpperBound.Compare(ctx, b.UpperBound, typ)
		if err != nil {
			sortErr = err
			return 0
		}
		return cmpU
	})
	if sortErr != nil {
		return nil, sortErr
	}

	cur := EmptyRangeColumnExpr(rngColExprs[0].Typ)
	res := make([]MySQLRangeColumnExpr, 0, len(rngColExprs))
	for _, rngColExpr := range rngColExprs {
		merged, ok, err := cur.TryUnion(ctx, rngColExpr)
		if err != nil {
			return nil, err
		}
		if ok {
			cur = merged
		} else if curIsEmpty, err := cur.IsEmpty(ctx); err != nil {
			return nil, err
		} else if !curIsEmpty {
			res = append(res, cur)
			cur = rngColExpr
		}
	}
	if curIsEmpty, err := cur.IsEmpty(ctx); err != nil {
		return nil, err
	} else if !curIsEmpty {
		res = append(res, cur)
	}

	return res, nil
}
