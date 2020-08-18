package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestShowCreateDatabase(t *testing.T) {
	require := require.New(t)

	node := NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true)
	iter, err := node.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */"},
	}, rows)

	node = NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false)
	iter, err = node.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */"},
	}, rows)
}
