package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestTableAlias(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := mem.NewTable("bar", sql.Schema{
		{Name: "a", Type: sql.Text, Nullable: true},
		{Name: "b", Type: sql.Text, Nullable: true},
	})
	alias := NewTableAlias("foo", NewResolvedTable(table))

	var rows = []sql.Row{
		sql.NewRow("1", "2"),
		sql.NewRow("3", "4"),
		sql.NewRow("5", "6"),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}

	require.Equal(table.Schema(), alias.Schema())
	iter, err := alias.RowIter(ctx)
	require.NoError(err)

	var i int
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		require.NoError(err)
		require.Equal(rows[i], row)
		i++
	}

	require.Equal(len(rows), i)
}
