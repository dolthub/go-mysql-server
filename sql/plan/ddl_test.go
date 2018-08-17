package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	db := mem.NewDatabase("test")
	tables := db.Tables()
	_, ok := tables["testTable"]
	require.False(ok)

	s := sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	}

	c := NewCreateTable(db, "testTable", s)

	rows, err := c.RowIter(sql.NewEmptyContext())

	require.NoError(err)

	r, err := rows.Next()
	require.Equal(err, io.EOF)
	require.Nil(r)

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}
}
