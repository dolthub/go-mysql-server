package plan

import (
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	tables := db.Tables()
	_, ok := tables["testTable"]
	require.False(ok)

	s := sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	}

	createTable(t, db, "testTable", s)

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}
}

func TestDropTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")

	s := sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	}

	createTable(t, db, "testTable1", s)
	createTable(t, db, "testTable2", s)
	createTable(t, db, "testTable3", s)

	d := NewDropTable(db, false, "testTable1", "testTable2")
	rows, err := d.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	r, err := rows.Next()
	require.Equal(err, io.EOF)
	require.Nil(r)

	_, ok := db.Tables()["testTable1"]
	require.False(ok)
	_, ok = db.Tables()["testTable2"]
	require.False(ok)
	_, ok = db.Tables()["testTable3"]
	require.True(ok)

	d = NewDropTable(db, false, "testTable1")
	_, err = d.RowIter(sql.NewEmptyContext())
	require.Error(err)

	d = NewDropTable(db, true, "testTable1")
	_, err = d.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	d = NewDropTable(db, true, "testTable1", "testTable2", "testTable3")
	_, err = d.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	_, ok = db.Tables()["testTable3"]
	require.False(ok)
}

func createTable(t *testing.T, db sql.Database, name string, schema sql.Schema) {
	c := NewCreateTable(db, name, schema)

	rows, err := c.RowIter(sql.NewEmptyContext())
	require.NoError(t, err)

	r, err := rows.Next()
	require.Equal(t, err, io.EOF)
	require.Nil(t, r)
}