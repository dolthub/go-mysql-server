// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestAllDatabases(t *testing.T) {
	require := require.New(t)

	var dbs = []sql.Database{
		memory.NewDatabase("a"),
		memory.NewDatabase("b"),
		memory.NewDatabase("c"),
	}

	c := NewCatalog(sql.NewDatabaseProvider(dbs...))
	require.Equal(dbs, c.AllDatabases(sql.NewEmptyContext()))
}

func TestCatalogDatabase(t *testing.T) {
	require := require.New(t)

	mydb := memory.NewDatabase("foo")
	c := NewCatalog(sql.NewDatabaseProvider(mydb))

	db, err := c.Database(sql.NewEmptyContext(), "flo")
	require.EqualError(err, "database not found: flo, maybe you mean foo?")
	require.Nil(db)

	db, err = c.Database(sql.NewEmptyContext(), "foo")
	require.NoError(err)
	require.Equal(mydb, db)
}

func TestCatalogTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	c := NewCatalog(sql.NewDatabaseProvider(db))
	ctx := sql.NewEmptyContext()

	table, _, err := c.Table(ctx, "foo", "bar")
	require.EqualError(err, "table not found: bar")
	require.Nil(table)

	mytable := memory.NewTable("bar", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection())
	db.AddTable("bar", mytable)

	table, _, err = c.Table(ctx, "foo", "baz")
	require.EqualError(err, "table not found: baz, maybe you mean bar?")
	require.Nil(table)

	table, _, err = c.Table(ctx, "foo", "bar")
	require.NoError(err)
	require.Equal(mytable, table)

	table, _, err = c.Table(ctx, "foo", "BAR")
	require.NoError(err)
	require.Equal(mytable, table)
}

func TestCatalogUnlockTables(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("db")
	t1 := newLockableTable(memory.NewTable("t1", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	t2 := newLockableTable(memory.NewTable("t2", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	c := NewCatalog(sql.NewDatabaseProvider(db))

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase(db.Name())
	c.LockTable(ctx, "t1")
	c.LockTable(ctx, "t2")

	require.NoError(c.UnlockTables(ctx, ctx.ID()))

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
