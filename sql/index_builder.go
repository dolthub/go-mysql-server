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
	ranges       map[string]RangeColumn
}

// NewIndexBuilder returns a new IndexBuilder. Used internally to construct a range that will later be passed to
// integrators through the Index function NewLookup.
func NewIndexBuilder(ctx *Context, idx Index) *IndexBuilder {
	colExprTypes := make(map[string]Type)
	for _, cet := range idx.ColumnExpressionTypes(ctx) {
		colExprTypes[cet.Expression] = cet.Type
	}
	return &IndexBuilder{
		idx:          idx,
		isInvalid:    false,
		err:          nil,
		colExprTypes: colExprTypes,
		ranges:       make(map[string]RangeColumn),
	}
}

// Equals represents colExpr = key.
func (b *IndexBuilder) Equals(ctx *Context, colExpr string, key interface{}) *IndexBuilder {
	if b.isInvalid {
		return b
	}
	typ, ok := b.colExprTypes[colExpr]
	if !ok {
		b.isInvalid = true
		b.err = ErrInvalidColExpr.New(colExpr, b.idx.ID())
		return b
	}
	b.updateCol(ctx, colExpr, ClosedRangeColumnExpr(key, key, typ))
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
			b.err = ErrRangeSimplification.New()
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

// Range returns the range for this index builder. If the builder is invalid for any reason then this returns nil.
func (b *IndexBuilder) Range() Range {
	if b.err != nil || b.isInvalid {
		return nil
	}
	var rangeCollection Range
	for _, colExpr := range b.idx.Expressions() {
		ranges, ok := b.ranges[colExpr]
		if !ok {
			// An index builder is guaranteed to cover the first n expressions, so if we hit an expression that we do
			// not have an entry for then we've hit all the ranges.
			break
		}
		rangeCollection = append(rangeCollection, ranges)
	}
	return rangeCollection
}

// Build constructs a new IndexLookup based on the ranges that have been built internally by this builder.
func (b *IndexBuilder) Build(ctx *Context) (IndexLookup, error) {
	if b.err != nil {
		return nil, b.err
	} else if b.isInvalid {
		return nil, nil
	} else {
		return b.idx.NewLookup(ctx, b.Range())
	}
}

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
		newRange := currentRange
		for _, potentialRange := range potentialRanges {
			var err error
			newRange, ok, err = newRange.TryIntersect(potentialRange)
			if err != nil {
				b.isInvalid = true
				b.err = err
				return
			}
			if ok {
				newRanges = append(newRanges, newRange)
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
