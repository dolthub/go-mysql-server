package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowColumns(t *testing.T) {
	require := require.New(t)

	table := NewResolvedTable(mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Text},
		{Name: "b", Type: sql.Int64, Nullable: true},
		{Name: "c", Type: sql.Int64, Default: int64(1)},
	}))

	iter, err := NewShowColumns(false, table).RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		sql.Row{"a", "TEXT", "NO", "", "", ""},
		sql.Row{"b", "INT64", "YES", "", "", ""},
		sql.Row{"c", "INT64", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)
}
func TestShowColumnsFull(t *testing.T) {
	require := require.New(t)

	table := NewResolvedTable(mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Text},
		{Name: "b", Type: sql.Int64, Nullable: true},
		{Name: "c", Type: sql.Int64, Default: int64(1)},
	}))

	iter, err := NewShowColumns(true, table).RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		sql.Row{"a", "TEXT", "utf8_bin", "NO", "", "", "", "", ""},
		sql.Row{"b", "INT64", nil, "YES", "", "", "", "", ""},
		sql.Row{"c", "INT64", nil, "NO", "", "1", "", "", ""},
	}

	require.Equal(expected, rows)
}
