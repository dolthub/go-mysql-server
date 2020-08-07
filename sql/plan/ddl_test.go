package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
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

	require.NoError(createTable(t, db, "testTable", s, false))

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}

	require.Error(createTable(t, db, "testTable", s, false))
	require.NoError(createTable(t, db, "testTable", s, true))
}

func TestDropTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")

	s := sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	}

	require.NoError(createTable(t, db, "testTable1", s, false))
	require.NoError(createTable(t, db, "testTable2", s, false))
	require.NoError(createTable(t, db, "testTable3", s, false))

	d := NewDropTable(db, false, "testTable1", "testTable2")
	rows, err := d.RowIter(sql.NewEmptyContext(), nil)
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
	_, err = d.RowIter(sql.NewEmptyContext(), nil)
	require.Error(err)

	d = NewDropTable(db, true, "testTable1")
	_, err = d.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	d = NewDropTable(db, true, "testTable1", "testTable2", "testTable3")
	_, err = d.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	_, ok = db.Tables()["testTable3"]
	require.False(ok)
}

func createTable(t *testing.T, db sql.Database, name string, schema sql.Schema, ifNotExists bool) error {
	c := NewCreateTable(db, name, schema, ifNotExists, nil, nil)

	rows, err := c.RowIter(sql.NewEmptyContext(), nil)
	if err != nil {
		return err
	}

	r, err := rows.Next()
	require.Nil(t, r)
	require.Equal(t, io.EOF, err)
	return nil
}
