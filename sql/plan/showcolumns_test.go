package plan

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestShowColumns(t *testing.T) {
	require := require.New(t)

	table := NewResolvedTable(memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Text, PrimaryKey: true},
		{Name: "b", Type: sql.Int64, Nullable: true},
		{Name: "c", Type: sql.Int64, Default: int64(1)},
	}))

	iter, err := NewShowColumns(false, table).RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		sql.Row{"a", "TEXT", "NO", "PRI", "", ""},
		sql.Row{"b", "BIGINT", "YES", "", "", ""},
		sql.Row{"c", "BIGINT", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)
}
func TestShowColumnsFull(t *testing.T) {
	require := require.New(t)

	table := NewResolvedTable(memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Text, PrimaryKey: true},
		{Name: "b", Type: sql.Int64, Nullable: true},
		{Name: "c", Type: sql.Int64, Default: int64(1), Comment: "a comment"},
	}))

	iter, err := NewShowColumns(true, table).RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		sql.Row{"a", "TEXT", "utf8_bin", "NO", "PRI", "", "", "", ""},
		sql.Row{"b", "BIGINT", nil, "YES", "", "", "", "", ""},
		sql.Row{"c", "BIGINT", nil, "NO", "", "1", "", "", "a comment"},
	}

	require.Equal(expected, rows)
}
