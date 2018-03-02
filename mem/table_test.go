package mem

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestTable_Name(t *testing.T) {
	require := require.New(t)
	s := sql.Schema{
		{"col1", sql.Text, nil, true},
	}
	table := NewTable("test", s)
	require.Equal("test", table.Name())
}

func TestTable_Insert_RowIter(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession()

	s := sql.Schema{
		{"col1", sql.Text, nil, true},
	}

	table := NewTable("test", s)

	rows, err := sql.NodeToRows(session, table)
	require.Nil(err)
	require.Len(rows, 0)

	err = table.Insert(sql.NewRow("foo"))
	rows, err = sql.NodeToRows(session, table)
	require.Nil(err)
	require.Len(rows, 1)
	require.Nil(s.CheckRow(rows[0]))

	err = table.Insert(sql.NewRow("bar"))
	rows, err = sql.NodeToRows(session, table)
	require.Nil(err)
	require.Len(rows, 2)
	require.Nil(s.CheckRow(rows[0]))
	require.Nil(s.CheckRow(rows[1]))
}
