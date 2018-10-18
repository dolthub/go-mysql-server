package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowCreateDatabase(t *testing.T) {
	require := require.New(t)

	node := NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true)
	iter, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8_bin */"},
	}, rows)

	node = NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false)
	iter, err = node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8_bin */"},
	}, rows)
}
