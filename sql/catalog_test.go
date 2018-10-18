package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestCatalogCurrentDatabase(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	require.Equal("", c.CurrentDatabase())

	c.AddDatabase(mem.NewDatabase("foo"))
	require.Equal("foo", c.CurrentDatabase())

	c.SetCurrentDatabase("bar")
	require.Equal("bar", c.CurrentDatabase())
}

func TestAllDatabases(t *testing.T) {
	require := require.New(t)

	var dbs = sql.Databases{
		mem.NewDatabase("a"),
		mem.NewDatabase("b"),
		mem.NewDatabase("c"),
	}

	c := sql.NewCatalog()
	for _, db := range dbs {
		c.AddDatabase(db)
	}

	require.Equal(dbs, c.AllDatabases())
}

func TestCatalogDatabase(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	db, err := c.Database("foo")
	require.EqualError(err, "database not found: foo")
	require.Nil(db)

	mydb := mem.NewDatabase("foo")
	c.AddDatabase(mydb)

	db, err = c.Database("foo")
	require.NoError(err)
	require.Equal(mydb, db)
}

func TestCatalogTable(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()

	table, err := c.Table("foo", "bar")
	require.EqualError(err, "database not found: foo")
	require.Nil(table)

	db := mem.NewDatabase("foo")
	c.AddDatabase(db)

	table, err = c.Table("foo", "bar")
	require.EqualError(err, "table not found: bar")
	require.Nil(table)

	mytable := mem.NewTable("bar", nil)
	db.AddTable("bar", mytable)

	table, err = c.Table("foo", "bar")
	require.NoError(err)
	require.Equal(mytable, table)

	table, err = c.Table("foo", "BAR")
	require.NoError(err)
	require.Equal(mytable, table)
}

func TestCatalogUnlockTables(t *testing.T) {
	require := require.New(t)

	db := mem.NewDatabase("db")
	t1 := newLockableTable(mem.NewTable("t1", nil))
	t2 := newLockableTable(mem.NewTable("t2", nil))
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	c := sql.NewCatalog()
	c.AddDatabase(db)

	c.LockTable(1, "t1")
	c.LockTable(1, "t2")

	require.NoError(c.UnlockTables(nil, 1))

	require.Equal(1, t1.unlocks)
	require.Equal(1, t2.unlocks)
}

type lockableTable struct {
	sql.Table
	unlocks int
}

func newLockableTable(t sql.Table) *lockableTable {
	return &lockableTable{Table: t}
}

var _ sql.Lockable = (*lockableTable)(nil)

func (l *lockableTable) Lock(ctx *sql.Context, write bool) error {
	return nil
}

func (l *lockableTable) Unlock(ctx *sql.Context, id uint32) error {
	l.unlocks++
	return nil
}
