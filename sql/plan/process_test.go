package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestQueryProcess(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64},
	})

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))

	var notifications int

	node := NewQueryProcess(
		NewProject(
			[]sql.Expression{
				expression.NewGetField(0, sql.Int64, "a", false),
			},
			NewResolvedTable(table),
		),
		func() {
			notifications++
		},
	)

	iter, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1)},
		{int64(2)},
	}

	require.ElementsMatch(expected, rows)
	require.Equal(1, notifications)
}

func TestProcessTable(t *testing.T) {
	require := require.New(t)

	table := mem.NewPartitionedTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64},
	}, 2)

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(3)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(4)))

	var notifications int

	node := NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
		},
		NewResolvedTable(
			NewProcessTable(
				table,
				func() {
					notifications++
				},
			),
		),
	)

	iter, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	}

	require.ElementsMatch(expected, rows)
	require.Equal(2, notifications)
}

func TestProcessIndexableTable(t *testing.T) {
	require := require.New(t)

	table := mem.NewPartitionedTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
	}, 2)

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(3)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(4)))

	var notifications int

	pt := NewProcessIndexableTable(
		table,
		func() {
			notifications++
		},
	)

	iter, err := pt.IndexKeyValues(sql.NewEmptyContext(), []string{"a"})
	require.NoError(err)

	var values [][]interface{}
	for {
		_, kviter, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		for {
			v, _, err := kviter.Next()
			if err == io.EOF {
				break
			}
			values = append(values, v)
			require.NoError(err)
		}
	}

	expectedValues := [][]interface{}{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	}

	require.ElementsMatch(expectedValues, values)
	require.Equal(2, notifications)
}
