package sql_test

import (
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/assert"
)

func TestCatalog_Database(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	db, err := c.Database("foo")
	assert.EqualError(err, "database not found: foo")
	assert.Nil(db)

	mydb := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, mydb)

	db, err = c.Database("foo")
	assert.NoError(err)
	assert.Equal(mydb, db)
}

func TestCatalog_Table(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()

	table, err := c.Table("foo", "bar")
	assert.EqualError(err, "database not found: foo")
	assert.Nil(table)

	mydb := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, mydb)

	table, err = c.Table("foo", "bar")
	assert.EqualError(err, "table not found: bar")
	assert.Nil(table)

	mytable := mem.NewTable("bar", sql.Schema{})
	mydb.AddTable("bar", mytable)

	table, err = c.Table("foo", "bar")
	assert.NoError(err)
	assert.Equal(mytable, table)
}
