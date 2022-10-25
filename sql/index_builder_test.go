// Copyright 2022 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexBuilderRanges(t *testing.T) {
	ctx := NewContext(context.Background())

	t.Run("None=[NULL,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{AllRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("IsNull=[NULL,NULL]", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.IsNull(ctx, "column_0")
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{NullRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("IsNull,Equals2=EmptyRange", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.IsNull(ctx, "column_0")
		builder = builder.Equals(ctx, "column_0", 2)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{EmptyRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("NotEquals2=(NULL,2),(2,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.NotEquals(ctx, "column_0", 2)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{GreaterThanRangeColumnExpr(2, Int8)}, Range{LessThanRangeColumnExpr(2, Int8)}}, ranges)
	})

	t.Run("NotEquals2,Equals2=(Inf,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.NotEquals(ctx, "column_0", 2)
		builder = builder.Equals(ctx, "column_0", 2)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{EmptyRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("Equals2,NotEquals2=(Inf,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.Equals(ctx, "column_0", 2)
		builder = builder.NotEquals(ctx, "column_0", 2)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{EmptyRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("LT4=(NULL,4)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.LessThan(ctx, "column_0", 4)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{LessThanRangeColumnExpr(4, Int8)}}, ranges)
	})

	t.Run("GT2,LT4=(2,4)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.GreaterThan(ctx, "column_0", 2)
		builder = builder.LessThan(ctx, "column_0", 4)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{OpenRangeColumnExpr(2, 4, Int8)}}, ranges)
	})

	t.Run("GT2,GT6=(4,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.GreaterThan(ctx, "column_0", 2)
		builder = builder.GreaterThan(ctx, "column_0", 6)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{GreaterThanRangeColumnExpr(6, Int8)}}, ranges)
	})

	t.Run("GT2,LT4,GT6=(Inf,Inf)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.GreaterThan(ctx, "column_0", 2)
		builder = builder.LessThan(ctx, "column_0", 4)
		builder = builder.GreaterThan(ctx, "column_0", 6)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{EmptyRangeColumnExpr(Int8)}}, ranges)
	})

	t.Run("NotEqual2,NotEquals4=(2,4),(4,Inf),(NULL,2)", func(t *testing.T) {
		builder := NewIndexBuilder(testIndex{1})
		builder = builder.NotEquals(ctx, "column_0", 2)
		builder = builder.NotEquals(ctx, "column_0", 4)
		ranges := builder.Ranges(ctx)
		assert.NotNil(t, ranges)
		assert.Equal(t, RangeCollection{Range{OpenRangeColumnExpr(2, 4, Int8)}, Range{GreaterThanRangeColumnExpr(4, Int8)}, Range{LessThanRangeColumnExpr(2, Int8)}}, ranges)
	})

	t.Run("ThreeColumnCombine", func(t *testing.T) {
		clauses := make([]RangeCollection, 3)
		clauses[0] = NewIndexBuilder(testIndex{3}).GreaterOrEqual(ctx, "column_0", 99).LessThan(ctx, "column_1", 66).Ranges(ctx)
		clauses[1] = NewIndexBuilder(testIndex{3}).GreaterOrEqual(ctx, "column_0", 1).LessOrEqual(ctx, "column_0", 47).Ranges(ctx)
		clauses[2] = NewIndexBuilder(testIndex{3}).NotEquals(ctx, "column_0", 2).LessThan(ctx, "column_1", 30).Ranges(ctx)
		assert.Len(t, clauses[0], 1)
		assert.Len(t, clauses[1], 1)
		assert.Len(t, clauses[2], 2)
		for _, perm := range [][]int{
			{0, 1, 2},
			{0, 2, 1},
			{1, 2, 0},
			{1, 0, 2},
			{2, 0, 1},
			{2, 1, 0},
		} {
			var all RangeCollection
			all = append(all, clauses[perm[0]]...)
			all = append(all, clauses[perm[1]]...)
			all = append(all, clauses[perm[2]]...)
			combined, err := RemoveOverlappingRanges(all...)
			assert.NoError(t, err)
			assert.NotNil(t, combined)
			assert.Equal(t, RangeCollection{
				Range{LessThanRangeColumnExpr(1, Int8), LessThanRangeColumnExpr(30, Int8), AllRangeColumnExpr(Int8)},
				Range{ClosedRangeColumnExpr(1, 47, Int8), AllRangeColumnExpr(Int8), AllRangeColumnExpr(Int8)},
				Range{OpenRangeColumnExpr(47, 99, Int8), LessThanRangeColumnExpr(30, Int8), AllRangeColumnExpr(Int8)},
				Range{GreaterOrEqualRangeColumnExpr(99, Int8), LessThanRangeColumnExpr(66, Int8), AllRangeColumnExpr(Int8)},
			}, combined)
		}
	})
}

type testIndex struct {
	numcols int
}

func (testIndex) CanSupport(...Range) bool {
	return true
}

func (testIndex) ID() string {
	return "test_index"
}

func (testIndex) Database() string {
	return "database"
}

func (testIndex) Table() string {
	return "table"
}

func (i testIndex) Expressions() []string {
	res := make([]string, i.numcols)
	for i := range res {
		res[i] = fmt.Sprintf("column_%d", i)
	}
	return res
}

func (testIndex) IsUnique() bool {
	return false
}

func (testIndex) Comment() string {
	return ""
}

func (testIndex) IndexType() string {
	return "FAKE"
}

func (testIndex) IsGenerated() bool {
	return false
}

func (i testIndex) ColumnExpressionTypes() []ColumnExpressionType {
	es := i.Expressions()
	res := make([]ColumnExpressionType, len(es))
	for i := range es {
		res[i] = ColumnExpressionType{Expression: es[i], Type: Int8}
	}
	return res
}

var _ Index = testIndex{}
