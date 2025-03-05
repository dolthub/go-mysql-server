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
	"context"
	"fmt"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
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

func (w SimpleWrapper[T]) MaxLength() int64 {
	return 2048
}

func (w SimpleWrapper[T]) StringType() sql.StringType {
	return types.Text
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

func (w ErrorWrapper[T]) MaxLength() int64 {
	return 2048
}

func (w ErrorWrapper[T]) StringType() sql.StringType {
	return types.Text
}

func (w ErrorWrapper[T]) UnwrapAny(ctx context.Context) (result interface{}, err error) {
	return result, fmt.Errorf("unwrap failed")
}

// TestWrapperSimple tests that a storage layer can return a Wrapper and have it be used in simple queries.
func TestWrapperSimple(t *testing.T) {}

// TestWrapperCopy tests that copying a wrapped value doesn't require the value to be uwrapped.
func TestWrapperCopy(t *testing.T) {}

func TestTable2(t *testing.T) {

	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "col1", Source: "test", Type: types.Text, Nullable: false, Default: planbuilder.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, types.Text, false)},
	})
	table := memory.NewFilteredTable(db.BaseDatabase, "test", schema, nil)

	require.NoError(t, table.Insert(ctx, sql.Row{"brave"}))
	require.NoError(t, table.Insert(ctx, sql.Row{SimpleWrapper[string]{"new"}}))
	require.NoError(t, table.Insert(ctx, sql.Row{SimpleWrapper[string]{"world"}}))
	require.NoError(t, table.Insert(ctx, sql.Row{"!"}))

	filters := []sql.Expression{
		expression.NewGreaterThanOrEqual(
			// expression.NewGetFieldWithTable(0, 0, types.Int32, "db", "test", "col1", false),
			function.NewLength(expression.NewGetFieldWithTable(0, 0, types.Text, "db", "test", "col1", false)),
			expression.NewLiteral(int32(5), types.Int32),
		),
	}

	filtered := table.WithFilters(ctx, filters)

	filteredRows := getAllRows(t, ctx, filtered)
	require.Len(t, filteredRows, 2)

}
