package plan

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	data := []sql.Row{
		sql.NewRow("c", nil, nil),
		sql.NewRow("a", int32(3), 3.0),
		sql.NewRow("b", int32(3), 3.0),
		sql.NewRow("c", int32(1), 1.0),
		sql.NewRow(nil, int32(1), nil),
	}

	schema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int32, Nullable: true},
		{Name: "col3", Type: sql.Float64, Nullable: true},
	}

	child := memory.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	s := NewSort([]SortField{
		{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Descending, NullOrdering: NullsLast},
		{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
	}, NewResolvedTable(child))
	require.Equal(schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow("c", nil, nil),
		sql.NewRow("c", int32(1), 1.0),
		sql.NewRow(nil, int32(1), nil),
		sql.NewRow("b", int32(3), 3.0),
		sql.NewRow("a", int32(3), 3.0),
	}

	actual, err := sql.NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, actual)

	s = NewSort([]SortField{
		{Column: expression.NewGetField(2, sql.Float64, "col3", true), Order: Ascending, NullOrdering: NullsFirst},
		{Column: expression.NewGetField(1, sql.Int32, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
		{Column: expression.NewGetField(0, sql.Text, "col1", true), Order: Ascending, NullOrdering: NullsLast},
	}, NewResolvedTable(child))
	require.Equal(schema, s.Schema())

	expected = []sql.Row{
		sql.NewRow("c", nil, nil),
		sql.NewRow(nil, int32(1), nil),
		sql.NewRow("c", int32(1), 1.0),
		sql.NewRow("a", int32(3), 3.0),
		sql.NewRow("b", int32(3), 3.0),
	}

	actual, err = sql.NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, actual)
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
		sql.NewRow(nil),
		sql.NewRow("d"),
		sql.NewRow("c"),
		sql.NewRow("b"),
		sql.NewRow("a"),
	}

	actual, err := sql.NodeToRows(ctx, s)
	require.NoError(err)
	require.Equal(expected, actual)
}
