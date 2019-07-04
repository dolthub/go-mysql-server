package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexesByTable(t *testing.T) {
	var require = require.New(t)

	var r = NewIndexRegistry()
	r.indexOrder = []indexKey{
		{"foo", "bar_idx_1"},
		{"foo", "bar_idx_2"},
		{"foo", "bar_idx_3"},
		{"foo", "baz_idx_1"},
		{"oof", "rab_idx_1"},
	}

	r.indexes = map[indexKey]Index{
		indexKey{"foo", "bar_idx_1"}: &dummyIdx{
			database: "foo",
			table:    "bar",
			id:       "bar_idx_1",
			expr:     []Expression{dummyExpr{1, "2"}},
		},
		indexKey{"foo", "bar_idx_2"}: &dummyIdx{
			database: "foo",
			table:    "bar",
			id:       "bar_idx_2",
			expr:     []Expression{dummyExpr{2, "3"}},
		},
		indexKey{"foo", "bar_idx_3"}: &dummyIdx{
			database: "foo",
			table:    "bar",
			id:       "bar_idx_3",
			expr:     []Expression{dummyExpr{3, "4"}},
		},
		indexKey{"foo", "baz_idx_1"}: &dummyIdx{
			database: "foo",
			table:    "baz",
			id:       "baz_idx_1",
			expr:     []Expression{dummyExpr{4, "5"}},
		},
		indexKey{"oof", "rab_idx_1"}: &dummyIdx{
			database: "oof",
			table:    "rab",
			id:       "rab_idx_1",
			expr:     []Expression{dummyExpr{5, "6"}},
		},
	}

	r.statuses[indexKey{"foo", "bar_idx_1"}] = IndexReady
	r.statuses[indexKey{"foo", "bar_idx_2"}] = IndexReady
	r.statuses[indexKey{"foo", "bar_idx_3"}] = IndexNotReady
	r.statuses[indexKey{"foo", "baz_idx_1"}] = IndexReady
	r.statuses[indexKey{"oof", "rab_idx_1"}] = IndexReady

	indexes := r.IndexesByTable("foo", "bar")
	require.Len(indexes, 3)

	for i, idx := range indexes {
		expected := r.indexes[r.indexOrder[i]]
		require.Equal(expected, idx)
		r.ReleaseIndex(idx)
	}
}

func TestIndexByExpression(t *testing.T) {
	require := require.New(t)

	r := NewIndexRegistry()
	r.indexOrder = []indexKey{
		{"foo", ""},
		{"foo", "bar"},
	}
	r.indexes = map[indexKey]Index{
		indexKey{"foo", ""}: &dummyIdx{
			database: "foo",
			expr:     []Expression{dummyExpr{1, "2"}},
		},
		indexKey{"foo", "bar"}: &dummyIdx{
			database: "foo",
			id:       "bar",
			expr:     []Expression{dummyExpr{2, "3"}},
		},
	}
	r.statuses[indexKey{"foo", ""}] = IndexReady

	idx := r.IndexByExpression("bar", dummyExpr{1, "2"})
	require.Nil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{1, "2"})
	require.NotNil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{2, "3"})
	require.Nil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{3, "4"})
	require.Nil(idx)
}

func TestAddIndex(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()
	idx := &dummyIdx{
		id:       "foo",
		expr:     []Expression{new(dummyExpr)},
		database: "foo",
		table:    "foo",
	}

	done, ready, err := r.AddIndex(idx)
	require.NoError(err)

	i := r.Index("foo", "foo")
	require.False(r.CanUseIndex(i))

	done <- struct{}{}

	<-ready
	i = r.Index("foo", "foo")
	require.True(r.CanUseIndex(i))

	_, _, err = r.AddIndex(idx)
	require.Error(err)
	require.True(ErrIndexIDAlreadyRegistered.Is(err))

	_, _, err = r.AddIndex(&dummyIdx{
		id:       "another",
		expr:     []Expression{new(dummyExpr)},
		database: "foo",
		table:    "foo",
	})
	require.Error(err)
	require.True(ErrIndexExpressionAlreadyRegistered.Is(err))
}

func TestDeleteIndex(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()

	idx := &dummyIdx{"foo", nil, "foo", "foo"}
	idx2 := &dummyIdx{"foo", nil, "foo", "bar"}
	r.indexes[indexKey{"foo", "foo"}] = idx
	r.indexes[indexKey{"foo", "bar"}] = idx2

	_, err := r.DeleteIndex("foo", "foo", false)
	require.Error(err)
	require.True(ErrIndexDeleteInvalidStatus.Is(err))

	_, err = r.DeleteIndex("foo", "foo", true)
	require.NoError(err)

	r.setStatus(idx2, IndexReady)

	_, err = r.DeleteIndex("foo", "foo", false)
	require.NoError(err)

	require.Len(r.indexes, 0)
}

func TestDeleteIndex_InUse(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()
	idx := &dummyIdx{
		"foo", nil, "foo", "foo",
	}
	r.indexes[indexKey{"foo", "foo"}] = idx
	r.setStatus(idx, IndexReady)
	r.retainIndex("foo", "foo")

	done, err := r.DeleteIndex("foo", "foo", false)
	require.NoError(err)

	require.Len(r.indexes, 1)
	require.False(r.CanUseIndex(idx))

	go func() {
		r.ReleaseIndex(idx)
	}()

	<-done
	require.Len(r.indexes, 0)
}

func TestExpressionsWithIndexes(t *testing.T) {
	require := require.New(t)

	r := NewIndexRegistry()

	var indexes = []*dummyIdx{
		{
			"idx1",
			[]Expression{
				&dummyExpr{0, "foo"},
				&dummyExpr{1, "bar"},
			},
			"foo",
			"foo",
		},
		{
			"idx2",
			[]Expression{
				&dummyExpr{0, "foo"},
				&dummyExpr{1, "bar"},
				&dummyExpr{3, "baz"},
			},
			"foo",
			"foo",
		},
		{
			"idx3",
			[]Expression{
				&dummyExpr{0, "foo"},
			},
			"foo",
			"foo",
		},
	}

	for _, idx := range indexes {
		done, ready, err := r.AddIndex(idx)
		require.NoError(err)
		close(done)
		<-ready
	}

	exprs := r.ExpressionsWithIndexes(
		"foo",
		&dummyExpr{0, "foo"},
		&dummyExpr{1, "bar"},
		&dummyExpr{3, "baz"},
	)

	expected := [][]Expression{
		{
			&dummyExpr{0, "foo"},
			&dummyExpr{1, "bar"},
			&dummyExpr{3, "baz"},
		},
		{
			&dummyExpr{0, "foo"},
			&dummyExpr{1, "bar"},
		},
	}

	require.ElementsMatch(expected, exprs)
}

func TestLoadIndexes(t *testing.T) {
	require := require.New(t)

	d1 := &loadDriver{id: "d1", indexes: []Index{
		&dummyIdx{id: "idx1", database: "db1", table: "t1"},
		&dummyIdx{id: "idx2", database: "db2", table: "t3"},
	}}

	d2 := &loadDriver{id: "d2", indexes: []Index{
		&dummyIdx{id: "idx3", database: "db1", table: "t2"},
		&dummyIdx{id: "idx4", database: "db2", table: "t4"},
	}}

	registry := NewIndexRegistry()
	registry.RegisterIndexDriver(d1)
	registry.RegisterIndexDriver(d2)

	dbs := Databases{
		dummyDB{
			name: "db1",
			tables: map[string]Table{
				"t1": &dummyTable{name: "t1"},
				"t2": &dummyTable{name: "t2"},
			},
		},
		dummyDB{
			name: "db2",
			tables: map[string]Table{
				"t3": &dummyTable{name: "t3"},
				"t4": &dummyTable{name: "t4"},
			},
		},
	}

	require.NoError(registry.LoadIndexes(dbs))

	expected := append(d1.indexes[:], d2.indexes...)
	var result []Index
	for _, idx := range registry.indexes {
		result = append(result, idx)
	}

	require.ElementsMatch(expected, result)

	for _, idx := range expected {
		require.Equal(registry.statuses[indexKey{idx.Database(), idx.ID()}], IndexReady)
	}
}

func TestLoadOutdatedIndexes(t *testing.T) {
	require := require.New(t)

	d := &loadDriver{id: "d1", indexes: []Index{
		&checksumIndex{&dummyIdx{id: "idx1", database: "db1", table: "t1"}, "2"},
		&checksumIndex{&dummyIdx{id: "idx2", database: "db1", table: "t2"}, "2"},
	}}

	registry := NewIndexRegistry()
	registry.RegisterIndexDriver(d)

	dbs := Databases{
		dummyDB{
			name: "db1",
			tables: map[string]Table{
				"t1": &checksumTable{&dummyTable{name: "t1"}, "2"},
				"t2": &checksumTable{&dummyTable{name: "t2"}, "1"},
			},
		},
	}

	require.NoError(registry.LoadIndexes(dbs))

	var result []Index
	for _, idx := range registry.indexes {
		result = append(result, idx)
	}

	require.ElementsMatch(d.indexes, result)

	require.Equal(registry.statuses[indexKey{"db1", "idx1"}], IndexReady)
	require.Equal(registry.statuses[indexKey{"db1", "idx2"}], IndexOutdated)
}

type dummyDB struct {
	name   string
	tables map[string]Table
}

func (d dummyDB) Name() string             { return d.name }
func (d dummyDB) Tables() map[string]Table { return d.tables }

type dummyTable struct {
	Table
	name string
}

func (t dummyTable) Name() string { return t.name }

type loadDriver struct {
	indexes []Index
	id      string
}

func (d loadDriver) ID() string { return d.id }
func (loadDriver) Create(db, table, id string, expressions []Expression, config map[string]string) (Index, error) {
	panic("create is a placeholder")
}
func (d loadDriver) LoadAll(db, table string) ([]Index, error) {
	var result []Index
	for _, i := range d.indexes {
		if i.Table() == table && i.Database() == db {
			result = append(result, i)
		}
	}
	return result, nil
}
func (loadDriver) Save(ctx *Context, index Index, iter PartitionIndexKeyValueIter) error { return nil }
func (loadDriver) Delete(Index, PartitionIter) error                                     { return nil }

type dummyIdx struct {
	id       string
	expr     []Expression
	database string
	table    string
}

var _ Index = (*dummyIdx)(nil)

func (i dummyIdx) Expressions() []string {
	var exprs []string
	for _, e := range i.expr {
		exprs = append(exprs, e.String())
	}
	return exprs
}
func (i dummyIdx) ID() string                                  { return i.id }
func (i dummyIdx) Get(...interface{}) (IndexLookup, error)     { panic("not implemented") }
func (i dummyIdx) Has(Partition, ...interface{}) (bool, error) { panic("not implemented") }
func (i dummyIdx) Database() string                            { return i.database }
func (i dummyIdx) Table() string                               { return i.table }
func (i dummyIdx) Driver() string                              { return "dummy" }

type dummyExpr struct {
	index   int
	colName string
}

var _ Expression = (*dummyExpr)(nil)

func (dummyExpr) Children() []Expression                  { return nil }
func (dummyExpr) Eval(*Context, Row) (interface{}, error) { panic("not implemented") }
func (e dummyExpr) WithChildren(children ...Expression) (Expression, error) {
	return e, nil
}
func (e dummyExpr) String() string { return fmt.Sprintf("dummyExpr{%d, %s}", e.index, e.colName) }
func (dummyExpr) IsNullable() bool { return false }
func (dummyExpr) Resolved() bool   { return false }
func (dummyExpr) Type() Type       { panic("not implemented") }
func (e dummyExpr) WithIndex(idx int) Expression {
	return &dummyExpr{idx, e.colName}
}

type checksumTable struct {
	Table
	checksum string
}

func (t *checksumTable) Checksum() (string, error) {
	return t.checksum, nil
}

type checksumIndex struct {
	Index
	checksum string
}

func (idx *checksumIndex) Checksum() (string, error) {
	return idx.checksum, nil
}
