package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowTableStatus(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()

	db1 := mem.NewDatabase("a")
	db1.AddTable("t1", mem.NewTable("t1", nil))
	db1.AddTable("t2", mem.NewTable("t2", nil))
	catalog.AddDatabase(db1)

	db2 := mem.NewDatabase("b")
	db2.AddTable("t3", mem.NewTable("t3", nil))
	db2.AddTable("t4", mem.NewTable("t4", nil))
	catalog.AddDatabase(db2)

	node := NewShowTableStatus()
	node.Catalog = catalog

	iter, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"t1", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		{"t2", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
	}

	require.Equal(expected, rows)

	node = NewShowTableStatus("a")
	node.Catalog = catalog

	iter, err = node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	expected = []sql.Row{
		{"t1", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		{"t2", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
	}

	require.Equal(expected, rows)
}
