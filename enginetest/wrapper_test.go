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
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
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

func (w ErrorWrapper[T]) Hash() interface{} {
	panic("not implemented")
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
	table := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)

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

	testTable := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)
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

	testTable2 := memory.NewTable(ctx, db.BaseDatabase, "t2", copySchema, nil)
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

	testTable := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)
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

	testTable := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)
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

	testTable := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	wrapper := exactLengthErrorWrapper(64)
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), wrapper}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(3), "!"}))

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE test MODIFY COLUMN col1 TEXT;", nil, nil, nil, nil)
	testQueryWithoutUnwrapping(t, ctx, e, "SELECT * from test;", []sql.Row{{int64(1), "brave"}, {int64(2), wrapper}, {int64(3), "!"}})
}

// outOfRowBlobWrapper stands in for a BLOB that a storage layer keeps
// out-of-band. This package cannot import the storage layer's value type, so
// the tests use this one. It implements [sql.BytesWrapper]. Convert returns it
// unchanged when it fits the type's maximum length, so only Unwrap yields the
// bytes.
type outOfRowBlobWrapper struct {
	val []byte
}

var _ sql.BytesWrapper = outOfRowBlobWrapper{}

// Unwrap returns the bytes stored out-of-band. Consumers must call it before
// reading the value.
func (w outOfRowBlobWrapper) Unwrap(ctx context.Context) ([]byte, error) {
	return w.val, nil
}

// UnwrapAny returns the same bytes as Unwrap, typed as interface{}.
func (w outOfRowBlobWrapper) UnwrapAny(ctx context.Context) (interface{}, error) {
	return w.val, nil
}

// IsExactLength reports that MaxByteLength is the exact length, so the length
// is known without unwrapping.
func (w outOfRowBlobWrapper) IsExactLength() bool {
	return true
}

// MaxByteLength returns the exact length of the bytes.
func (w outOfRowBlobWrapper) MaxByteLength() int64 {
	return int64(len(w.val))
}

// Compare reports that it cannot compare, so it always returns comparable as
// false.
func (w outOfRowBlobWrapper) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	return 0, false, nil
}

// Hash returns the bytes as a string, so equal bytes hash equally.
func (w outOfRowBlobWrapper) Hash() interface{} {
	return string(w.val)
}

func TestWrapperBlobFunctions(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12263
	ctx, db, _, e := setupWrapperTests(t)

	content := bytes.Repeat([]byte("x"), 5000)
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "b", Source: "test", Type: types.LongBlob, Nullable: true},
		&sql.Column{Name: "z", Source: "test", Type: types.LongBlob, Nullable: true},
	})
	testTable := memory.NewTable(ctx, db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	compressed := mustCompressBlob(t, content)
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), content, compressed}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), outOfRowBlobWrapper{val: content}, outOfRowBlobWrapper{val: compressed}}))

	// Each function runs on pk 1 (inline) and pk 2 (out-of-band).
	tests := []struct {
		name     string
		query    string
		expected []sql.Row
	}{
		{
			name:     "md5",
			query:    "SELECT md5(b) FROM test WHERE pk = %d;",
			expected: []sql.Row{{"a6bb7bde3251ca2d810d32dadd9e8ae7"}},
		},
		{
			name:     "sha2",
			query:    "SELECT sha2(b, 256) FROM test WHERE pk = %d;",
			expected: []sql.Row{{"c59d3c0480cc2d71d8f646e735e92da65450311eec46e81a5db8c7e6e8a92054"}},
		},
		{
			name:     "to_base64",
			query:    "SELECT from_base64(to_base64(b)) FROM test WHERE pk = %d;",
			expected: []sql.Row{{content}},
		},
		{
			name:     "compress",
			query:    "SELECT uncompress(compress(b)) FROM test WHERE pk = %d;",
			expected: []sql.Row{{content}},
		},
		{
			name:     "quote",
			query:    "SELECT quote(b) FROM test WHERE pk = %d;",
			expected: []sql.Row{{"'" + string(content) + "'"}},
		},
		{
			name:     "uncompress",
			query:    "SELECT uncompress(z) FROM test WHERE pk = %d;",
			expected: []sql.Row{{content}},
		},
		{
			name:     "uncompressed_length",
			query:    "SELECT uncompressed_length(z) FROM test WHERE pk = %d;",
			expected: []sql.Row{{uint32(len(content))}},
		},
		{
			name:     "is_binary",
			query:    "SELECT is_binary(b) FROM test WHERE pk = %d;",
			expected: []sql.Row{{false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("in-row", func(t *testing.T) {
				testQueryWithoutUnwrapping(t, ctx, e, fmt.Sprintf(tt.query, 1), tt.expected)
			})
			t.Run("out-of-row", func(t *testing.T) {
				// A panic fails only this subtest.
				require.NotPanics(t, func() {
					testQueryWithoutUnwrapping(t, ctx, e, fmt.Sprintf(tt.query, 2), tt.expected)
				})
			})
		})
	}
}

// mustCompressBlob returns data in the layout that compress writes and
// uncompress and uncompressed_length read. The layout is a 4-byte
// little-endian length followed by a zlib stream. It fails the test on error.
func mustCompressBlob(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	_, err := zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	header := binary.LittleEndian.AppendUint32(nil, uint32(len(data)))
	return append(header, buf.Bytes()...)
}
