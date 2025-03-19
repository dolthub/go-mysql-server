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

package enginetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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

func exactLengthErrorWrapper(maxByteLength int64) ErrorWrapper[string] {
	return ErrorWrapper[string]{maxByteLength: maxByteLength, isExactLength: true}
}

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

func setupWrapperTests(t *testing.T) (*sql.Context, *memory.Database, *MemoryHarness, *sqle.Engine) {
	db := memory.NewDatabase("mydb")
	pro := memory.NewDBProvider(db)
	harness := NewDefaultMemoryHarness().WithProvider(pro)
	ctx := NewContext(harness)
	e := NewEngineWithProvider(t, harness, pro)
	return ctx, db, harness, e
}

// testQueryWithoutUnwrapping checks if a returned query result matches the expected result.
// Unlike calling TestQueryWithContext, this doesn't normalize the results (which would result in unwrapping any wrapped values.)
func testQueryWithoutUnwrapping(t *testing.T, ctx *sql.Context, e QueryEngine, query string, expectedRows []sql.Row) {
	_, rowIter, _, err := e.Query(ctx, query)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, rowIter)
	require.NoError(t, err)
	require.Equal(t, expectedRows, rows, "Unexpected result for query %s", query)
}

// TestWrapperCopyInKey tests that copying a wrapped value in the primary key doesn't require the value to be unwrapped.
// This is skipped because inserting into tables requires comparisons between primary keys, which currently requires
// unwrapping. But in the future, we may be able to skip fully unwrapping values for specific operations.
func TestWrapperCopyInKey(t *testing.T) {
	t.Skip()
	ctx, db, harness, e := setupWrapperTests(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false)},
	})
	table := memory.NewTable(db.BaseDatabase, "test", schema, nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"brave"}))
	require.NoError(t, table.Insert(ctx, sql.Row{longTextErrorWrapper}))
	require.NoError(t, table.Insert(ctx, sql.Row{"!"}))

	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t2 AS SELECT 1, col1, 2 FROM test;", nil, nil, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "SELECT * from t2;", []sql.Row{{1, "brave", 2}, {1, longTextErrorWrapper, 2}, {1, "!", 2}}, nil, nil, nil)
}

// TestWrapperCopyInKey tests that copying a wrapped value not in the primary key doesn't require the value to be unwrapped.
func TestWrapperCopyNotInKey(t *testing.T) {
	ctx, db, harness, e := setupWrapperTests(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), longTextErrorWrapper}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(3), "!"}))

	copySchema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "one", Source: "t2", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "pk", Source: "t2", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "t2", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
		&sql.Column{Name: "two", Source: "t2", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: false},
	})

	testTable2 := memory.NewTable(db.BaseDatabase, "t2", copySchema, nil)
	db.AddTable("t2", testTable2)

	TestQueryWithContext(t, ctx, e, harness, "INSERT INTO t2 SELECT 1, pk, col1, 2 FROM test;", nil, nil, nil, nil)
	testQueryWithoutUnwrapping(t, ctx, e, "SELECT * from t2;", []sql.Row{{int64(1), int64(1), "brave", int64(2)}, {int64(1), int64(2), longTextErrorWrapper, int64(2)}, {int64(1), int64(3), "!", int64(2)}})
}

// TestWrapperCopyWhenWideningColumn tests that widening a column doesn't cause values to be unwrapped.
func TestWrapperCopyWhenWideningColumn(t *testing.T) {
	ctx, db, harness, e := setupWrapperTests(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), textErrorWrapper}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(3), "!"}))

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE test MODIFY COLUMN col1 LONGTEXT;", nil, nil, nil, nil)
	testQueryWithoutUnwrapping(t, ctx, e, "SELECT * from test;", []sql.Row{{int64(1), "brave"}, {int64(2), textErrorWrapper}, {int64(3), "!"}})
}

// TestWrapperCopyWhenWideningColumn tests that widening a column doesn't cause values to be unwrapped.
func TestWrapperCopyWhenNarrowingColumn(t *testing.T) {
	ctx, db, harness, e := setupWrapperTests(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), longTextErrorWrapper}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(3), "!"}))

	AssertErrWithCtx(t, e, harness, ctx, "ALTER TABLE test MODIFY COLUMN col1 TEXT;", nil, nil, "unwrap failed")
}

func TestWrapperCopyWithExactLengthWhenNarrowingColumn(t *testing.T) {
	ctx, db, harness, e := setupWrapperTests(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	wrapper := exactLengthErrorWrapper(64)
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), wrapper}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(3), "!"}))

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE test MODIFY COLUMN col1 TEXT;", nil, nil, nil, nil)
	testQueryWithoutUnwrapping(t, ctx, e, "SELECT * from test;", []sql.Row{{int64(1), "brave"}, {int64(2), wrapper}, {int64(3), "!"}})
}
