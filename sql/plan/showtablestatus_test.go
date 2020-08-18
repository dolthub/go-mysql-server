package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestShowTableStatus(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()

	db1 := memory.NewDatabase("a")
	db1.AddTable("t1", memory.NewTable("t1", nil))
	db1.AddTable("t2", memory.NewTable("t2", nil))
	catalog.AddDatabase(db1)

	db2 := memory.NewDatabase("b")
	db2.AddTable("t3", memory.NewTable("t3", nil))
	db2.AddTable("t4", memory.NewTable("t4", nil))
	catalog.AddDatabase(db2)

	node := NewShowTableStatus()
	node.Catalog = catalog

	ctx := sql.NewEmptyContext().WithCurrentDB("a")
	iter, err := node.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"t1", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, sql.Collation_Default.String(), nil, nil},
		{"t2", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, sql.Collation_Default.String(), nil, nil},
	}

	require.Equal(expected, rows)

	node = NewShowTableStatus("a")
	node.Catalog = catalog

	iter, err = node.RowIter(ctx, nil)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	expected = []sql.Row{
		{"t1", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, sql.Collation_Default.String(), nil, nil},
		{"t2", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, sql.Collation_Default.String(), nil, nil},
	}

	require.Equal(expected, rows)
}
