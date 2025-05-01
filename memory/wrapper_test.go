// Copyright 2025 Dolthub, Inc.
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

package memory_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type SimpleWrapper[T any] struct {
	wrapped       T
	maxByteLength int64
	isExactLength bool
	wasUnwrapped  bool
}

// NewUnknownLengthWrapper returns a SimpleWrapper whose exact length cannot be known without unwrapping.
func NewUnknownLengthWrapper[T any](wrapped T) *SimpleWrapper[T] {
	return &SimpleWrapper[T]{
		wrapped:       wrapped,
		maxByteLength: types.LongText.MaxCharacterLength(),
		isExactLength: false,
	}
}

// NewKnownLengthWrapper returns a SimpleWrapper whose exact length can be known without unwrapping.
func NewKnownLengthWrapper[T ~string | ~[]byte](wrapped T) *SimpleWrapper[T] {
	return &SimpleWrapper[T]{
		wrapped:       wrapped,
		maxByteLength: int64(len(wrapped)),
		isExactLength: true,
	}
}

func (w *SimpleWrapper[T]) assertInterfaces() {
	var _ sql.Wrapper[T] = w
}

func (w *SimpleWrapper[T]) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	// The most common use case for wrappers is to store a pointer to out-of-band storage.
	// If the pointers are equal, the wrappers can be assumed to be equal.
	// But if the pointers are not equal, nothing can be assumed.
	// We mimic that behavior here.
	if otherWrapper, ok := other.(*SimpleWrapper[T]); ok {
		switch val := any(w.wrapped).(type) {
		case string:
			if val == any(otherWrapper.wrapped).(string) {
				return 0, true, nil
			}
		case []byte:
			if bytes.Equal(val, any(otherWrapper.wrapped).([]byte)) {
				return 0, true, nil
			}
		}
	}
	return 0, false, nil
}

func (w *SimpleWrapper[T]) Unwrap(ctx context.Context) (result T, err error) {
	w.wasUnwrapped = true
	return w.wrapped, nil
}

func (w *SimpleWrapper[T]) UnwrapAny(ctx context.Context) (result interface{}, err error) {
	w.wasUnwrapped = true
	return w.wrapped, nil
}

func (w SimpleWrapper[T]) MaxByteLength() int64 {
	return w.maxByteLength
}

func (w SimpleWrapper[T]) IsExactLength() bool {
	return w.isExactLength
}

func (w SimpleWrapper[T]) Hash() interface{} {
	return nil
}

// ErrorWrapper is a wrapped type that errors when unwrapped. This can be used to test that certain operations
// won't trigger an unwrap.
type ErrorWrapper[T any] struct {
	maxByteLength int64
	isExactLength bool
}

func (w ErrorWrapper[T]) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	return 0, false, nil
}

var textErrorWrapper = ErrorWrapper[string]{maxByteLength: types.Text.MaxByteLength(), isExactLength: false}
var longTextErrorWrapper = ErrorWrapper[string]{maxByteLength: types.LongText.MaxByteLength(), isExactLength: false}

func (w ErrorWrapper[T]) assertInterfaces() {
	var _ sql.Wrapper[T] = w
}

func (w ErrorWrapper[T]) Unwrap(ctx context.Context) (result T, err error) {
	return result, fmt.Errorf("unwrap failed")
}

func (w ErrorWrapper[T]) UnwrapAny(ctx context.Context) (result interface{}, err error) {
	return result, fmt.Errorf("unwrap failed")
}

func (w ErrorWrapper[T]) MaxByteLength() int64 {
	return w.maxByteLength
}

func (w ErrorWrapper[T]) IsExactLength() bool {
	return w.isExactLength
}

func (w ErrorWrapper[T]) Hash() interface{} {
	panic("not implemented")
}

// TestWrapperCompare tests that a wrapped value can be used in comparisons.
func TestWrapperCompare(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})
	table := memory.NewFilteredTable(db.BaseDatabase, "test", schema, nil)

	upperUnknownLength := NewKnownLengthWrapper("UPPER")
	lowerUnknownLength := NewKnownLengthWrapper("lower")

	require.NoError(t, table.Insert(ctx, sql.Row{int64(1), "BIG"}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(2), upperUnknownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(3), lowerUnknownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(4), "little"}))

	// Filter for strings in all caps
	filters := []sql.Expression{
		expression.NewEquals(
			function.NewUpper(expression.NewGetFieldWithTable(1, 0, types.Text, "db", "test", "col1", false)),
			expression.NewGetFieldWithTable(1, 0, types.Text, "db", "test", "col1", false),
		),
	}

	filtered := table.WithFilters(ctx, filters)

	filteredRows := getAllRows(t, ctx, filtered)
	require.Len(t, filteredRows, 2)
	require.ElementsMatch(t, filteredRows, []sql.Row{{int64(1), "BIG"}, {int64(2), upperUnknownLength}})

	{
		// Filter using LIKE
		likeFilter := []sql.Expression{
			expression.NewLike(
				expression.NewGetFieldWithTable(1, 0, types.Text, "db", "test", "col1", false),
				expression.NewLiteral("l%", types.Text),
				nil),
		}

		filtered := table.WithFilters(ctx, likeFilter)

		filteredRows := getAllRows(t, ctx, filtered)
		require.Len(t, filteredRows, 2)
		require.ElementsMatch(t, filteredRows, []sql.Row{{int64(3), lowerUnknownLength}, {int64(4), "little"}})
	}
}

// TestWrapperCompare tests that a wrapped value can be used in comparisons without being unwrapped.
func TestWrapperSelfCompare(t *testing.T) {
	t.Skip("This is not currently enabled because it may have performance implications.")
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})
	table := memory.NewFilteredTable(db.BaseDatabase, "test", schema, nil)

	wrappedValue := NewKnownLengthWrapper("UPPER")

	require.NoError(t, table.Insert(ctx, sql.Row{int64(1), wrappedValue}))

	// Filter for strings in all caps
	filters := []sql.Expression{
		expression.NewEquals(
			expression.NewLiteral(wrappedValue, types.Text),
			expression.NewGetFieldWithTable(1, 0, types.Text, "db", "test", "col1", false),
		),
	}

	filtered := table.WithFilters(ctx, filters)

	filteredRows := getAllRows(t, ctx, filtered)
	require.Len(t, filteredRows, 1)
	require.ElementsMatch(t, filteredRows, []sql.Row{{int64(1), wrappedValue}})
	require.False(t, wrappedValue.wasUnwrapped)

	otherWrappedValue := NewKnownLengthWrapper("lower")

	require.NoError(t, table.Insert(ctx, sql.Row{int64(2), otherWrappedValue}))

	filtered = table.WithFilters(ctx, filters)

	filteredRows = getAllRows(t, ctx, filtered)
	require.Len(t, filteredRows, 1)
	require.ElementsMatch(t, filteredRows, []sql.Row{{int64(1), wrappedValue}})
	require.True(t, wrappedValue.wasUnwrapped)
	require.True(t, otherWrappedValue.wasUnwrapped)
}

// TestWrapperLength tests that the LENGTH function only unwraps wrapped values if the exact length is not known.
func TestWrapperLength(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})
	table := memory.NewFilteredTable(db.BaseDatabase, "test", schema, nil)

	shortKnownLength := NewKnownLengthWrapper("tim")
	longKnownLength := NewKnownLengthWrapper("aaron")

	shortUnknownLength := NewUnknownLengthWrapper("nick")
	longUnknownLength := NewUnknownLengthWrapper("daylon")

	require.NoError(t, table.Insert(ctx, sql.Row{int64(1), "zach"}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(2), shortKnownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(3), longKnownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(4), shortUnknownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(5), longUnknownLength}))
	require.NoError(t, table.Insert(ctx, sql.Row{int64(6), "jason"}))

	// Filter for strings of length >= 5, matching exactly one string of each type (known length wrapper, unknown length wrapper, and not wrapped)
	filters := []sql.Expression{
		expression.NewGreaterThanOrEqual(
			function.NewLength(expression.NewGetFieldWithTable(1, 0, types.Text, "db", "test", "col1", false)),
			expression.NewLiteral(int32(5), types.Int32),
		),
	}

	filtered := table.WithFilters(ctx, filters)

	filteredRows := getAllRows(t, ctx, filtered)
	require.Len(t, filteredRows, 3)
	require.False(t, shortKnownLength.wasUnwrapped)
	require.False(t, longKnownLength.wasUnwrapped)
	require.True(t, shortUnknownLength.wasUnwrapped)
	require.True(t, longUnknownLength.wasUnwrapped)
}

// TestWrapperInsertingIntoWideColumnDoesntUnwrap tests that inserting into a column wider than the wrapper's
// max length doesn't trigger an unwrap.
func TestWrapperInsertingIntoWideColumnDoesntUnwrap(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), textErrorWrapper}))
}

// TestWrapperInsertingIntoNarrowColumnCausesUnwrap tests that inserting into a column narrower than the wrapper's
// max length triggers an unwrap.
func TestWrapperInsertingIntoNarrowColumnCausesUnwrap(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	// The custom "unwrap failed" error gets suppressed by the tableEditor, resulting in an "invalid type" error instead.
	require.ErrorContains(t, testTable.Insert(ctx, sql.Row{int64(2), longTextErrorWrapper}), "invalid type")
}
