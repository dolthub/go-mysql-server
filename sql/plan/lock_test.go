// Copyright 2020-2021 Dolthub, Inc.
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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestLockTables(t *testing.T) {
	require := require.New(t)

	t1 := newLockableTable(memory.NewTable("foo", nil))
	t2 := newLockableTable(memory.NewTable("bar", nil))
	node := NewLockTables([]*TableLock{
		{NewResolvedTable(t1, nil, nil), true},
		{NewResolvedTable(t2, nil, nil), false},
	})
	node.Catalog = sql.NewCatalog(sql.NewTestProvider())

	_, err := node.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	require.Equal(1, t1.writeLocks)
	require.Equal(0, t1.readLocks)
	require.Equal(1, t2.readLocks)
	require.Equal(0, t2.writeLocks)
}

func TestUnlockTables(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("db")
	t1 := newLockableTable(memory.NewTable("foo", nil))
	t2 := newLockableTable(memory.NewTable("bar", nil))
	t3 := newLockableTable(memory.NewTable("baz", nil))
	db.AddTable("foo", t1)
	db.AddTable("bar", t2)
	db.AddTable("baz", t3)

	catalog := sql.NewCatalog(sql.NewTestProvider(db))

	ctx := sql.NewContext(context.Background()).WithCurrentDB("db").WithCurrentDB("db")
	catalog.LockTable(ctx, "foo")
	catalog.LockTable(ctx, "bar")

	node := NewUnlockTables()
	node.Catalog = catalog

	_, err := node.RowIter(ctx, nil)
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
