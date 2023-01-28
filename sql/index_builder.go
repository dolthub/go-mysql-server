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
	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrInvalidColExpr      = errors.NewKind("the expression `%s` could not be found from the index `%s`")
	ErrRangeSimplification = errors.NewKind("attempting to simplify ranges has removed all ranges")
	ErrInvalidRangeType    = errors.NewKind("encountered the RangeType_Invalid")
)

// IndexBuilder builds ranges based on the combination of calls made for the given index, and then relies on the Index
// to return an IndexLookup from the created ranges.
type IndexBuilder struct {
	idx          Index
	isInvalid    bool
	err          error
	colExprTypes map[string]Type
	ranges       map[string][]RangeColumnExpr
}

// NewIndexBuilder returns a new IndexBuilder. Used internally to construct a range that will later be passed to
// integrators through the Index function NewLookup.
func NewIndexBuilder(idx Index) *IndexBuilder {
	colExprTypes := make(map[string]Type)
	ranges := make(map[string][]RangeColumnExpr)
	for _, cet := range idx.ColumnExpressionTypes() {
		colExprTypes[cet.Expression] = cet.Type
		ranges[cet.Expression] = []RangeColumnExpr{AllRangeColumnExpr(cet.Type)}
	}
	return &IndexBuilder{
		idx:          idx,
		isInvalid:    false,
		err:          nil,
		colExprTypes: colExprTypes,
		ranges:       ranges,
	}
}

// Equals represents colExpr = key. For IN expressions, pass all of them in the same Equals call.
func (b *IndexBuilder) Equals(ctx *Context, colExpr string, keys ...interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	potentialRanges := make([]RangeColumnExpr, len(keys))
	for i, key := range keys {
		potentialRanges[i] = ClosedRangeColumnExpr(key, key, typ)
	}
	b.updateCol(ctx, colExpr, potentialRanges...)
	return b
}

// NotEquals represents colExpr <> key.
func (b *IndexBuilder) NotEquals(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, GreaterThanRangeColumnExpr(key, typ), LessThanRangeColumnExpr(key, typ))
	if !b.isInvalid {
		ranges, err := SimplifyRangeColumn(b.ranges[colExpr]...)
		if err != nil {
			b.isInvalid = true
			b.err = err
			return b
		}
		if len(ranges) == 0 {
			b.isInvalid = true
			return b
		}
		b.ranges[colExpr] = ranges
	}
	return b
}

// GreaterThan represents colExpr > key.
func (b *IndexBuilder) GreaterThan(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, GreaterThanRangeColumnExpr(key, typ))
	return b
}

// GreaterOrEqual represents colExpr >= key.
func (b *IndexBuilder) GreaterOrEqual(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, GreaterOrEqualRangeColumnExpr(key, typ))
	return b
}

// LessThan represents colExpr < key.
func (b *IndexBuilder) LessThan(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, LessThanRangeColumnExpr(key, typ))
	return b
}

// LessOrEqual represents colExpr <= key.
func (b *IndexBuilder) LessOrEqual(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, LessOrEqualRangeColumnExpr(key, typ))
	return b
}

// IsNull represents colExpr = nil
func (b *IndexBuilder) IsNull(ctx *Context, colExpr string) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, NullRangeColumnExpr(typ))

	return b
}

// IsNotNull represents colExpr != nil
func (b *IndexBuilder) IsNotNull(ctx *Context, colExpr string) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, NotNullRangeColumnExpr(typ))

	return b
}

// Ranges returns all ranges for this index builder. If the builder is in an error state then this returns nil.
func (b *IndexBuilder) Ranges(ctx *Context) RangeCollection {
	if b.err != nil {
		return nil
	}
	// An invalid builder that did not error got into a state where no columns will ever match, so we return an empty range
	if b.isInvalid {
		cets := b.idx.ColumnExpressionTypes()
		emptyRange := make(Range, len(cets))
		for i, cet := range cets {
			emptyRange[i] = EmptyRangeColumnExpr(cet.Type)
		}
		return RangeCollection{emptyRange}
	}
	var allColumns [][]RangeColumnExpr
	for _, colExpr := range b.idx.Expressions() {
		ranges, ok := b.ranges[colExpr]
		if !ok {
			// An index builder is guaranteed to cover the first n expressions, so if we hit an expression that we do
			// not have an entry for then we've hit all the ranges.
			break
		}
		allColumns = append(allColumns, ranges)
	}

	// In the builder ranges map we store multiple column expressions per column, however we want all permutations to
	// be their own range, so here we're creating a new range for every permutation.
	colCounts := make([]int, len(allColumns))
	permutation := make([]int, len(allColumns))
	for i, rangeColumn := range allColumns {
		colCounts[i] = len(rangeColumn)
	}
	var ranges []Range
	exit := false
	for !exit {
		exit = true
		currentRange := make(Range, len(allColumns))
		for colIdx, exprCount := range colCounts {
			permutation[colIdx] = (permutation[colIdx] + 1) % exprCount
			if permutation[colIdx] != 0 {
				exit = false
				break
			}
		}
		for colIdx, exprIdx := range permutation {
			currentRange[colIdx] = allColumns[colIdx][exprIdx]
		}
		isempty, err := currentRange.IsEmpty()
		if err != nil {
			b.err = err
			return nil
		}
		if !isempty {
			ranges = append(ranges, currentRange)
		}
	}
	if len(ranges) == 0 {
		cets := b.idx.ColumnExpressionTypes()
		emptyRange := make(Range, len(cets))
		for i, cet := range cets {
			emptyRange[i] = EmptyRangeColumnExpr(cet.Type)
		}
		return RangeCollection{emptyRange}
	}
	return ranges
}

// Build constructs a new IndexLookup based on the ranges that have been built internally by this builder.
func (b *IndexBuilder) Build(ctx *Context) (IndexLookup, error) {
	if b.err != nil {
		return emptyLookup, b.err
	} else {
		ranges := b.Ranges(ctx)
		if len(ranges) == 0 {
			return emptyLookup, nil
		}
		return IndexLookup{Index: b.idx, Ranges: ranges}, nil
	}
}

// updateCol updates the internal columns with the given ranges by intersecting each given range with each existing
// range. That means that each given range is treated as an OR with respect to the other given ranges. If multiple
// ranges are to be intersected with respect to one another, multiple calls to updateCol should be made.
func (b *IndexBuilder) updateCol(ctx *Context, colExpr string, potentialRanges ...RangeColumnExpr) {
	if len(potentialRanges) == 0 {
		return
	}

	currentRanges, ok := b.ranges[colExpr]
	if !ok {
		b.ranges[colExpr] = potentialRanges
		return
	}

	var newRanges []RangeColumnExpr
	for _, currentRange := range currentRanges {
		for _, potentialRange := range potentialRanges {
			newRange, ok, err := currentRange.TryIntersect(potentialRange)
			if err != nil {
				b.isInvalid = true
				if !ErrInvalidValue.Is(err) {
					b.err = err
				}
				return
			}
			if ok {
				isempty, err := newRange.IsEmpty()
				if err != nil {
					b.isInvalid = true
					b.err = err
					return
				}
				if !isempty {
					newRanges = append(newRanges, newRange)
				}
			}
		}
	}

	// If we end up with zero ranges then we had an impossible combination, such as (x < 1 AND x > 1)
	if len(newRanges) == 0 {
		b.isInvalid = true
		return
	}
	b.ranges[colExpr] = newRanges
}
