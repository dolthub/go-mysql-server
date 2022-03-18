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

package plan

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int32, Nullable: true},
		{Name: "col3", Type: sql.Float64, Nullable: true},
	})

	type sortTest struct {
		rows       []sql.Row
		sortFields []sql.SortField
		expected   []sql.Row
	}

	testCases := []sortTest{
		{
			rows: []sql.Row{
				sql.NewRow("c", nil, nil),
				sql.NewRow("a", int32(3), 3.0),
				sql.NewRow("b", int32(3), 3.0),
				sql.NewRow("c", int32(1), 1.0),
				sql.NewRow(nil, int32(1), nil),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow("c", nil, nil),
				sql.NewRow(nil, int32(1), nil),
				sql.NewRow("c", int32(1), 1.0),
				sql.NewRow("b", int32(3), 3.0),
				sql.NewRow("a", int32(3), 3.0),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow("c", int32(3), 3.0),
				sql.NewRow("c", int32(3), nil),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow("c", int32(3), nil),
				sql.NewRow("c", int32(3), 3.0),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow("c", nil, nil),
				sql.NewRow("a", int32(3), 3.0),
				sql.NewRow("b", int32(3), 3.0),
				sql.NewRow("c", int32(1), 1.0),
				sql.NewRow(nil, int32(1), nil),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
			},
			expected: []sql.Row{
				sql.NewRow("c", nil, nil),
				sql.NewRow(nil, int32(1), nil),
				sql.NewRow("c", int32(1), 1.0),
				sql.NewRow("a", int32(3), 3.0),
				sql.NewRow("b", int32(3), 3.0),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow("a", int32(1), 2),
				sql.NewRow("a", int32(1), 1),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow("a", int32(1), 1),
				sql.NewRow("a", int32(1), 2),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow("a", int32(1), 2),
				sql.NewRow("a", int32(1), 1),
				sql.NewRow("a", int32(2), 2),
				sql.NewRow("a", int32(3), 1),
				sql.NewRow("b", int32(2), 2),
				sql.NewRow("c", int32(3), 1),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow("a", int32(3), 1),
				sql.NewRow("a", int32(2), 2),
				sql.NewRow("a", int32(1), 1),
				sql.NewRow("a", int32(1), 2),
				sql.NewRow("b", int32(2), 2),
				sql.NewRow("c", int32(3), 1),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow(nil, nil, 2),
				sql.NewRow(nil, nil, 1),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow(nil, nil, 1),
				sql.NewRow(nil, nil, 2),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow(nil, nil, 1),
				sql.NewRow(nil, nil, 2),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow(nil, nil, 2),
				sql.NewRow(nil, nil, 1),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow(nil, nil, 1),
				sql.NewRow(nil, nil, nil),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
			},
			expected: []sql.Row{
				sql.NewRow(nil, nil, nil),
				sql.NewRow(nil, nil, 1),
			},
		},
		{
			rows: []sql.Row{
				sql.NewRow(nil, nil, nil),
				sql.NewRow(nil, nil, 1),
			},
			sortFields: []sql.SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: sql.Ascending, NullOrdering: sql.NullsLast},
			},
			expected: []sql.Row{
				sql.NewRow(nil, nil, 1),
				sql.NewRow(nil, nil, nil),
			},
		},
	}

	for i, tt := range testCases {
		t.Run(fmt.Sprintf("Sort test %d", i), func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			tbl := memory.NewTable("test", schema, nil)
			for _, row := range tt.rows {
				require.NoError(tbl.Insert(sql.NewEmptyContext(), row))
			}

			sort := NewSort(tt.sortFields, NewResolvedTable(tbl, nil, nil))

			actual, err := sql.NodeToRows(ctx, sort)
			require.NoError(err)
			require.Equal(tt.expected, actual)
		})
	}
}

func TestSortAscending(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	data := []sql.Row{
		sql.NewRow("c"),
		sql.NewRow("a"),
		sql.NewRow("d"),
		sql.NewRow(nil),
		sql.NewRow("b"),
	}

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	})

	child := memory.NewTable("test", schema, nil)
	for _, row := range data {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	sf := []sql.SortField{
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Ascending, NullOrdering: sql.NullsFirst},
	}
	s := NewSort(sf, NewResolvedTable(child, nil, nil))
	require.Equal(schema.Schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow(nil),
		sql.NewRow("a"),
		sql.NewRow("b"),
		sql.NewRow("c"),
		sql.NewRow("d"),
	}

	actual, err := sql.NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, actual)
}

func TestSortDescending(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	data := []sql.Row{
		sql.NewRow("c"),
		sql.NewRow("a"),
		sql.NewRow("d"),
		sql.NewRow(nil),
		sql.NewRow("b"),
	}

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	})

	child := memory.NewTable("test", schema, nil)
	for _, row := range data {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	sf := []sql.SortField{
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: sql.Descending, NullOrdering: sql.NullsFirst},
	}
	s := NewSort(sf, NewResolvedTable(child, nil, nil))
	require.Equal(schema.Schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow("d"),
		sql.NewRow("c"),
		sql.NewRow("b"),
		sql.NewRow("a"),
		sql.NewRow(nil),
	}

	actual, err := sql.NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, actual)
}
