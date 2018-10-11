package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestLockTables(t *testing.T) {
	require := require.New(t)

	t1 := newLockableTable(mem.NewTable("foo", nil))
	t2 := newLockableTable(mem.NewTable("bar", nil))
	node := NewLockTables([]*TableLock{
		{NewResolvedTable(t1), true},
		{NewResolvedTable(t2), false},
	})
	node.Catalog = sql.NewCatalog()

	_, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	require.Equal(1, t1.writeLocks)
	require.Equal(0, t1.readLocks)
	require.Equal(1, t2.readLocks)
	require.Equal(0, t2.writeLocks)
}

func TestUnlockTables(t *testing.T) {
	require := require.New(t)

	db := mem.NewDatabase("db")
	t1 := newLockableTable(mem.NewTable("foo", nil))
	t2 := newLockableTable(mem.NewTable("bar", nil))
	t3 := newLockableTable(mem.NewTable("baz", nil))
	db.AddTable("foo", t1)
	db.AddTable("bar", t2)
	db.AddTable("baz", t3)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	catalog.LockTable(0, "foo")
	catalog.LockTable(0, "bar")

	node := NewUnlockTables()
	node.Catalog = catalog

	_, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	require.Equal(1, t1.unlocks)
	require.Equal(1, t2.unlocks)
	require.Equal(0, t3.unlocks)
}

type lockableTable struct {
	sql.Table
	readLocks  int
	writeLocks int
	unlocks    int
}

func newLockableTable(t sql.Table) *lockableTable {
	return &lockableTable{Table: t}
}

var _ sql.Lockable = (*lockableTable)(nil)

func (l *lockableTable) Lock(ctx *sql.Context, write bool) error {
	if write {
		l.writeLocks++
	} else {
		l.readLocks++
	}
	return nil
}

func (l *lockableTable) Unlock(ctx *sql.Context, id uint32) error {
	l.unlocks++
	return nil
}
