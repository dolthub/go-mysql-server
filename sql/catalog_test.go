package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestCatalog_Database(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	db, err := c.Database("foo")
	require.EqualError(err, "database not found: foo")
	require.Nil(db)

	mydb := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, mydb)

	db, err = c.Database("foo")
	require.NoError(err)
	require.Equal(mydb, db)
}

func TestCatalog_Table(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()

	table, err := c.Table("foo", "bar")
	require.EqualError(err, "database not found: foo")
	require.Nil(table)

	db := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, db)

	table, err = c.Table("foo", "bar")
	require.EqualError(err, "table not found: bar")
	require.Nil(table)

	mytable := mem.NewTable("bar", nil)
	db.AddTable("bar", mytable)

	table, err = c.Table("foo", "bar")
	require.NoError(err)
	require.Equal(mytable, table)
}
