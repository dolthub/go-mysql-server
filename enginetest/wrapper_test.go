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
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
	"testing"
)

type SimpleWrapper[T any] struct {
	wrapped T
}

func (w SimpleWrapper[T]) assertInterfaces() {
	var _ sql.Wrapper[T] = w
}

func (w SimpleWrapper[T]) Unwrap(ctx context.Context) (result T, err error) {
	return w.wrapped, nil
}

func (w SimpleWrapper[T]) StringType() sql.StringType {
	return types.LongText
}

func (w SimpleWrapper[T]) UnwrapAny(ctx context.Context) (result interface{}, err error) {
	return w.wrapped, nil
}

type ErrorWrapper[T any] struct{}

func (w ErrorWrapper[T]) assertInterfaces() {
	var _ sql.Wrapper[T] = w
}

func (w ErrorWrapper[T]) Unwrap(ctx context.Context) (result T, err error) {
	return result, fmt.Errorf("unwrap failed")
}

func (w ErrorWrapper[T]) UnwrapAny(ctx context.Context) (result interface{}, err error) {
	return result, fmt.Errorf("unwrap failed")
}

func (w ErrorWrapper[T]) StringType() sql.StringType {
	return types.LongText
}

func setup(t *testing.T) (*sql.Context, *memory.Database, *MemoryHarness, *sqle.Engine) {
	db := memory.NewDatabase("mydb")
	pro := memory.NewDBProvider(db)
	harness := NewDefaultMemoryHarness().WithProvider(pro)
	ctx := NewContext(harness)
	e := NewEngineWithProvider(t, harness, pro)
	return ctx, db, harness, e
}

// TestWrapperCopyInKey tests that copying a wrapped value in the primary key doesn't require the value to be uwrapped.
func TestWrapperCopyInKey(t *testing.T) {
	t.Skip()
	db := memory.NewDatabase("db")
	harness := NewDefaultMemoryHarness()
	ctx := NewContext(harness)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false)},
	})
	table := memory.NewTable(db.BaseDatabase, "test", schema, nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"brave"}))
	require.NoError(t, table.Insert(ctx, sql.Row{ErrorWrapper[string]{}}))
	require.NoError(t, table.Insert(ctx, sql.Row{"!"}))

	e := mustNewEngine(t, harness)

	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t2 AS SELECT 1, col1, 2 FROM test;", nil, nil, nil, nil)
}

// TestWrapperCopyInKey tests that copying a wrapped value not in the primary key doesn't require the value to be uwrapped.
func TestWrapperCopyNotInKey(t *testing.T) {
	ctx, db, harness, e := setup(t)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "pk", Source: "test", Type: types.Int64, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `1`, types.Int64, false), PrimaryKey: true},
		&sql.Column{Name: "col1", Source: "test", Type: types.LongText, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false), PrimaryKey: false},
	})

	testTable := memory.NewTable(db.BaseDatabase, "test", schema, nil)
	db.AddTable("test", testTable)

	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(1), "brave"}))
	require.NoError(t, testTable.Insert(ctx, sql.Row{int64(2), ErrorWrapper[string]{}}))
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
}
