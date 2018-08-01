package plan

import (
	"context"
	"math"
	"testing"
	"time"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/test"

	"github.com/stretchr/testify/require"
)

func TestCreateIndex(t *testing.T) {
	require := require.New(t)

	table := &indexableTable{mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})}

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	catalog.RegisterIndexDriver(driver)
	db := mem.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.Databases = append(catalog.Databases, db)

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", true),
	}

	ci := NewCreateIndex("idx", table, exprs, "mock", make(map[string]string))
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	tracer := new(test.MemTracer)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer))
	_, err := ci.RowIter(ctx)
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Len(driver.deleted, 0)
	require.Equal([]string{"idx"}, driver.saved)
	idx := catalog.IndexRegistry.Index("foo", "idx")
	require.NotNil(idx)
	require.Equal(&mockIndex{"foo", "foo", "idx", []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}}, idx)

	found := false
	for _, span := range tracer.Spans {
		if span == "plan.createIndex" {
			found = true
			break
		}
	}

	require.True(found)
}

func TestCreateIndexNotIndexableExprs(t *testing.T) {
	require := require.New(t)

	table := &indexableTable{mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Blob},
		{Name: "b", Source: "foo", Type: sql.JSON},
		{Name: "c", Source: "foo", Type: sql.Text},
	})}

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	catalog.RegisterIndexDriver(driver)
	db := mem.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.Databases = append(catalog.Databases, db)

	ci := NewCreateIndex(
		"idx",
		table,
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Blob, "foo", "a", true),
		},
		"mock",
		make(map[string]string),
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	_, err := ci.RowIter(sql.NewEmptyContext())
	require.Error(err)
	require.True(ErrExprTypeNotIndexable.Is(err))

	ci = NewCreateIndex(
		"idx",
		table,
		[]sql.Expression{
			expression.NewGetFieldWithTable(1, sql.JSON, "foo", "a", true),
		},
		"mock",
		make(map[string]string),
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	_, err = ci.RowIter(sql.NewEmptyContext())
	require.Error(err)
	require.True(ErrExprTypeNotIndexable.Is(err))
}

func TestCreateIndexSync(t *testing.T) {
	require := require.New(t)

	table := &indexableTable{mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})}

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	catalog.RegisterIndexDriver(driver)
	db := mem.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.Databases = append(catalog.Databases, db)

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", true),
	}

	ci := NewCreateIndex(
		"idx", table, exprs, "mock",
		map[string]string{"async": "false"},
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	tracer := new(test.MemTracer)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer))
	_, err := ci.RowIter(ctx)
	require.NoError(err)

	require.Len(driver.deleted, 0)
	require.Equal([]string{"idx"}, driver.saved)
	idx := catalog.IndexRegistry.Index("foo", "idx")
	require.NotNil(idx)
	require.Equal(&mockIndex{"foo", "foo", "idx", []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}}, idx)

	found := false
	for _, span := range tracer.Spans {
		if span == "plan.createIndex" {
			found = true
			break
		}
	}

	require.True(found)
}

func TestCreateIndexWithIter(t *testing.T) {
	require := require.New(t)
	foo := mem.NewTable("foo", sql.Schema{
		{Name: "one", Source: "foo", Type: sql.Int64},
		{Name: "two", Source: "foo", Type: sql.Int64},
	})

	rows := [][2]int64{
		{1, 2},
		{-1, -2},
		{0, 0},
		{math.MaxInt64, math.MinInt64},
	}
	for _, r := range rows {
		err := foo.Insert(sql.NewEmptyContext(), sql.NewRow(r[0], r[1]))
		require.NoError(err)
	}

	table := &indexableTable{foo}
	exprs := []sql.Expression{expression.NewPlus(
		expression.NewGetField(0, sql.Int64, "one", false),
		expression.NewGetField(0, sql.Int64, "two", false)),
	}

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	catalog.RegisterIndexDriver(driver)
	db := mem.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.Databases = append(catalog.Databases, db)

	ci := NewCreateIndex("idx", table, exprs, "mock", make(map[string]string))
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	columns, exprs, err := getColumnsAndPrepareExpressions(ci.Exprs)
	require.NoError(err)

	iter, err := getIndexKeyValueIter(sql.NewEmptyContext(), table, columns, exprs)
	require.NoError(err)

	var (
		vals []interface{}
	)
	for i := 0; err == nil; i++ {
		vals, _, err = iter.Next()
		if err == nil {
			require.Equal(1, len(vals))
			require.Equal(rows[i][0]+rows[i][1], vals[0])
		}
	}
	require.NoError(iter.Close())
}

type mockIndex struct {
	db    string
	table string
	id    string
	exprs []sql.Expression
}

var _ sql.Index = (*mockIndex)(nil)

func (i *mockIndex) ID() string       { return i.id }
func (i *mockIndex) Table() string    { return i.table }
func (i *mockIndex) Database() string { return i.db }
func (i *mockIndex) Expressions() []string {
	exprs := make([]string, len(i.exprs))
	for i, e := range i.exprs {
		exprs[i] = e.String()
	}

	return exprs
}
func (i *mockIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	panic("unimplemented")
}
func (i *mockIndex) Has(key ...interface{}) (bool, error) {
	panic("unimplemented")
}
func (*mockIndex) Driver() string { return "mock" }

type mockDriver struct {
	deleted []string
	saved   []string
}

var _ sql.IndexDriver = (*mockDriver)(nil)

func (*mockDriver) ID() string { return "mock" }
func (*mockDriver) Create(db, table, id string, exprs []sql.Expression, config map[string]string) (sql.Index, error) {
	return &mockIndex{db, table, id, exprs}, nil
}
func (*mockDriver) LoadAll(db, table string) ([]sql.Index, error) {
	panic("not implemented")
}
func (d *mockDriver) Save(ctx *sql.Context, index sql.Index, iter sql.IndexKeyValueIter) error {
	d.saved = append(d.saved, index.ID())
	return nil
}
func (d *mockDriver) Delete(index sql.Index) error {
	d.deleted = append(d.deleted, index.ID())
	return nil
}

type indexableTable struct {
	sql.Table
}

var _ sql.Indexable = (*indexableTable)(nil)

func (indexableTable) HandledFilters([]sql.Expression) []sql.Expression {
	panic("not implemented")
}

func (it *indexableTable) IndexKeyValueIter(ctx *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	t, ok := it.Table.(*mem.Table)
	if !ok {
		return nil, nil
	}

	return t.IndexKeyValueIter(ctx, colNames)
}

func (indexableTable) WithProjectAndFilters(ctx *sql.Context, columns, filters []sql.Expression) (sql.RowIter, error) {
	panic("not implemented")
}

func (indexableTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	return nil, nil
}
