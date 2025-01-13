// Copyright 2023 Dolthub, Inc.
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

package in_mem_table

import (
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

func ToRows[V any](ctx *sql.Context, ops *ValueOps[V], is IndexedSet[V]) ([]sql.Row, error) {
	var res []sql.Row
	var err error
	is.VisitEntries(func(v V) {
		if err != nil {
			return
		}
		var r sql.Row = sql.UntypedSqlRow{}
		r, err = ops.ToRow(ctx, v)
		if err == nil {
			res = append(res, r)
		}
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func MultiToRows[V any](ctx *sql.Context, ops *MultiValueOps[V], is IndexedSet[V]) ([]sql.Row, error) {
	var res []sql.Row
	var err error
	is.VisitEntries(func(v V) {
		if err != nil {
			return
		}
		var rs []sql.Row
		rs, err = ops.ToRows(ctx, v)
		if err == nil {
			res = append(res, rs...)
		}
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

var _ sql.Table = (*IndexedSetTable[string])(nil)
var _ sql.InsertableTable = (*IndexedSetTable[string])(nil)
var _ sql.UpdatableTable = (*IndexedSetTable[string])(nil)
var _ sql.DeletableTable = (*IndexedSetTable[string])(nil)
var _ sql.ReplaceableTable = (*IndexedSetTable[string])(nil)
var _ sql.TruncateableTable = (*IndexedSetTable[string])(nil)

type IndexedSetTable[V any] struct {
	name   string
	schema sql.Schema
	coll   sql.CollationID

	set IndexedSet[V]
	ops ValueOps[V]

	lock  sync.Locker
	rlock sync.Locker
}

func NewIndexedSetTable[V any](name string, schema sql.Schema, coll sql.CollationID, set IndexedSet[V], ops ValueOps[V], lock, rlock sync.Locker) *IndexedSetTable[V] {
	return &IndexedSetTable[V]{
		name,
		schema,
		coll,
		set,
		ops,
		lock,
		rlock,
	}
}

func (t *IndexedSetTable[V]) TableId() sql.TableId {
	return 0
}

func (t *IndexedSetTable[V]) Set() IndexedSet[V] {
	return t.set
}

func (t *IndexedSetTable[V]) Name() string {
	return t.name
}

func (t *IndexedSetTable[V]) String() string {
	return t.Name()
}

func (t *IndexedSetTable[V]) Schema() sql.Schema {
	return t.schema
}

func (t *IndexedSetTable[V]) Collation() sql.CollationID {
	return t.coll
}

func (t *IndexedSetTable[V]) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(partition{}), nil
}

func (t *IndexedSetTable[V]) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	t.rlock.Lock()
	defer t.rlock.Unlock()
	rows, err := ToRows[V](ctx, &t.ops, t.set)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func (t *IndexedSetTable[V]) Inserter(ctx *sql.Context) sql.RowInserter {
	return t.Editor()
}

func (t *IndexedSetTable[V]) Updater(ctx *sql.Context) sql.RowUpdater {
	return t.Editor()
}

func (t *IndexedSetTable[V]) Deleter(ctx *sql.Context) sql.RowDeleter {
	return t.Editor()
}

func (t *IndexedSetTable[V]) Replacer(ctx *sql.Context) sql.RowReplacer {
	return t.Editor()
}

func (t *IndexedSetTable[V]) Truncate(ctx *sql.Context) (int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	c := t.set.Count()
	t.set.Clear()
	return c, nil
}

type editor interface {
	sql.RowInserter
	sql.RowUpdater
	sql.RowDeleter
	sql.RowReplacer
}

func (t *IndexedSetTable[V]) Editor() editor {
	return OperationLockingTableEditor{
		t.lock,
		&IndexedSetTableEditor[V]{
			t.set,
			t.ops,
		},
	}
}

var _ sql.Table = (*MultiIndexedSetTable[string])(nil)
var _ sql.InsertableTable = (*MultiIndexedSetTable[string])(nil)
var _ sql.UpdatableTable = (*MultiIndexedSetTable[string])(nil)
var _ sql.DeletableTable = (*MultiIndexedSetTable[string])(nil)
var _ sql.ReplaceableTable = (*MultiIndexedSetTable[string])(nil)

type MultiIndexedSetTable[V any] struct {
	name   string
	schema sql.Schema
	coll   sql.CollationID

	set IndexedSet[V]
	ops MultiValueOps[V]

	lock  sync.Locker
	rlock sync.Locker
}

func NewMultiIndexedSetTable[V any](name string, schema sql.Schema, coll sql.CollationID, set IndexedSet[V], ops MultiValueOps[V], lock, rlock sync.Locker) *MultiIndexedSetTable[V] {
	return &MultiIndexedSetTable[V]{
		name,
		schema,
		coll,
		set,
		ops,
		lock,
		rlock,
	}
}

func (t *MultiIndexedSetTable[V]) Set() IndexedSet[V] {
	return t.set
}

func (t *MultiIndexedSetTable[V]) Name() string {
	return t.name
}

func (t *MultiIndexedSetTable[V]) String() string {
	return t.Name()
}

func (t *MultiIndexedSetTable[V]) Schema() sql.Schema {
	return t.schema
}

func (t *MultiIndexedSetTable[V]) Collation() sql.CollationID {
	return t.coll
}

func (t *MultiIndexedSetTable[V]) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(partition{}), nil
}

func (t *MultiIndexedSetTable[V]) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	t.rlock.Lock()
	defer t.rlock.Unlock()
	rows, err := MultiToRows[V](ctx, &t.ops, t.set)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func (t *MultiIndexedSetTable[V]) Inserter(ctx *sql.Context) sql.RowInserter {
	return t.Editor()
}

func (t *MultiIndexedSetTable[V]) Updater(ctx *sql.Context) sql.RowUpdater {
	return t.Editor()
}

func (t *MultiIndexedSetTable[V]) Deleter(ctx *sql.Context) sql.RowDeleter {
	return t.Editor()
}

func (t *MultiIndexedSetTable[V]) Replacer(ctx *sql.Context) sql.RowReplacer {
	return t.Editor()
}

func (t *MultiIndexedSetTable[V]) Editor() editor {
	return OperationLockingTableEditor{
		t.lock,
		&MultiIndexedSetTableEditor[V]{
			t.set,
			t.ops,
		},
	}
}

type partition struct{}

var _ sql.Partition = partition{}

func (partition) Key() []byte {
	return nil
}
