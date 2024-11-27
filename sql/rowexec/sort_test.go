// Copyright 2020-2021 Dolthub, Inc.
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

package rowexec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSort(t *testing.T) {
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text, Nullable: true},
		{Name: "col2", Type: types.Int32, Nullable: true},
		{Name: "col3", Type: types.Float64, Nullable: true},
	})

	type sortTest struct {
		rows       []sql.UntypedSqlRow
		sortFields []sql.SortField
		expected   []sql.UntypedSqlRow
	}

	testCases := []sortTest{
		{
			rows: []sql.UntypedSqlRow{
				{"c", nil, nil},
				{"a", int32(3), float64(3.0)},
				{"b", int32(3), float64(3.0)},
				{"c", int32(1), float64(1.0)},
				{nil, int32(1), nil},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{"c", nil, nil},
				{nil, int32(1), nil},
				{"c", int32(1), float64(1.0)},
				{"b", int32(3), float64(3.0)},
				{"a", int32(3), float64(3.0)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{"c", int32(3), float64(3.0)},
				{"c", int32(3), nil},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{"c", int32(3), nil},
				{"c", int32(3), float64(3.0)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{"c", nil, nil},
				{"a", int32(3), float64(3.0)},
				{"b", int32(3), float64(3.0)},
				{"c", int32(1), float64(1.0)},
				{nil, int32(1), nil},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
			},
			expected: []sql.UntypedSqlRow{
				{"c", nil, nil},
				{nil, int32(1), nil},
				{"c", int32(1), float64(1.0)},
				{"a", int32(3), float64(3.0)},
				{"b", int32(3), float64(3.0)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{"a", int32(1), float64(2)},
				{"a", int32(1), float64(1)},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{"a", int32(1), float64(1)},
				{"a", int32(1), float64(2)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{"a", int32(1), float64(2)},
				{"a", int32(1), float64(1)},
				{"a", int32(2), float64(2)},
				{"a", int32(3), float64(1)},
				{"b", int32(2), float64(2)},
				{"c", int32(3), float64(1)},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{"a", int32(3), float64(1)},
				{"a", int32(2), float64(2)},
				{"a", int32(1), float64(1)},
				{"a", int32(1), float64(2)},
				{"b", int32(2), float64(2)},
				{"c", int32(3), float64(1)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{nil, nil, float64(2)},
				{nil, nil, float64(1)},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{nil, nil, float64(1)},
				{nil, nil, float64(2)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{nil, nil, float64(1)},
				{nil, nil, float64(2)},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{nil, nil, float64(2)},
				{nil, nil, float64(1)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{nil, nil, float64(1)},
				{nil, nil, nil},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.UntypedSqlRow{
				{nil, nil, nil},
				{nil, nil, float64(1)},
			},
		},
		{
			rows: []sql.UntypedSqlRow{
				{nil, nil, nil},
				{nil, nil, float64(1)},
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(1, types.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, types.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
			},
			expected: []sql.UntypedSqlRow{
				{nil, nil, float64(1)},
				{nil, nil, nil},
			},
		},
	}

	for i, tt := range testCases {
		t.Run(fmt.Sprintf("Sort test %d", i), func(t *testing.T) {
			require := require.New(t)

			db := memory.NewDatabase("test")
			tbl := memory.NewTable(db, "test", schema, nil)
			pro := memory.NewDBProvider(db)
			ctx := newContext(pro)

			for _, row := range tt.rows {
				require.NoError(tbl.Insert(ctx, row))
			}

			sort := plan.NewSort(tt.sortFields, plan.NewResolvedTable(tbl, nil, nil))

			actual, err := NodeToRows(ctx, sort)
			require.NoError(err)
			require.Equal(tt.expected, sql.RowsToUntyped(actual))
		})
	}
}

func TestSortAscending(t *testing.T) {
	require := require.New(t)

	data := []sql.UntypedSqlRow{
		{"c"},
		{"a"},
		{"d"},
		{nil},
		{"b"},
	}

	db := memory.NewDatabase("test")
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text, Nullable: true},
	})

	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	child := memory.NewTable(db, "test", schema, nil)
	for _, row := range data {
		require.NoError(child.Insert(ctx, row))
	}

	sf := []sql.SortField{
		{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
	}
	s := plan.NewSort(sf, plan.NewResolvedTable(child, nil, nil))
	require.Equal(schema.Schema, s.Schema())

	expected := []sql.UntypedSqlRow{
		{nil},
		{"a"},
		{"b"},
		{"c"},
		{"d"},
	}

	actual, err := NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, sql.RowsToUntyped(actual))
}

func TestSortDescending(t *testing.T) {
	require := require.New(t)

	data := []sql.UntypedSqlRow{
		{"c"},
		{"a"},
		{"d"},
		{nil},
		{"b"},
	}

	db := memory.NewDatabase("test")
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text, Nullable: true},
	})

	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	child := memory.NewTable(db, "test", schema, nil)
	for _, row := range data {
		require.NoError(child.Insert(ctx, row))
	}

	sf := []sql.SortField{
		{Column: expression.NewGetField(0, types.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
	}
	s := plan.NewSort(sf, plan.NewResolvedTable(child, nil, nil))
	require.Equal(schema.Schema, s.Schema())

	expected := []sql.UntypedSqlRow{
		{"d"},
		{"c"},
		{"b"},
		{"a"},
		{nil},
	}

	actual, err := NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, sql.RowsToUntyped(actual))
}
