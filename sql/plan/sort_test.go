package plan

import (
	"fmt"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	schema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int32, Nullable: true},
		{Name: "col3", Type: sql.Float64, Nullable: true},
	}

	type sortTest struct {
		rows       []sql.Row
		sortFields []SortField
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
			sortFields: []SortField{
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Descending, NullOrdering: NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Descending, NullOrdering: NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsLast},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Descending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Descending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Descending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Descending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
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
			sortFields: []SortField{
				{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsLast},
				{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsLast},
				{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsLast},
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

			tbl := memory.NewTable("test", schema)
			for _, row := range tt.rows {
				require.NoError(tbl.Insert(sql.NewEmptyContext(), row))
			}

			sort := NewSort(tt.sortFields, NewResolvedTable(tbl))

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

	schema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	}

	child := memory.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	sf := []SortField{
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
	}
	s := NewSort(sf, NewResolvedTable(child))
	require.Equal(schema, s.Schema())

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

	schema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	}

	child := memory.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	sf := []SortField{
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Descending, NullOrdering: NullsFirst},
	}
	s := NewSort(sf, NewResolvedTable(child))
	require.Equal(schema, s.Schema())

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
