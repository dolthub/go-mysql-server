package plan

import (
	"context"
	"testing"
	"time"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

	_, err := ci.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Len(driver.deleted, 0)
	require.Equal([]string{"idx"}, driver.saved)
	idx := catalog.IndexRegistry.Index("foo", "idx")
	require.NotNil(idx)
	require.Equal(&mockIndex{"foo", "foo", "idx", []sql.ExpressionHash{
		sql.NewExpressionHash(expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true)),
		sql.NewExpressionHash(expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true)),
	}}, idx)
}

type mockIndex struct {
	db    string
	table string
	id    string
	exprs []sql.ExpressionHash
}

var _ sql.Index = (*mockIndex)(nil)

func (i *mockIndex) ID() string                             { return i.id }
func (i *mockIndex) Table() string                          { return i.table }
func (i *mockIndex) Database() string                       { return i.db }
func (i *mockIndex) ExpressionHashes() []sql.ExpressionHash { return i.exprs }
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
func (*mockDriver) Create(db, table, id string, exprs []sql.ExpressionHash, config map[string]string) (sql.Index, error) {
	return &mockIndex{db, table, id, exprs}, nil
}
func (*mockDriver) LoadAll(db, table string) ([]sql.Index, error) {
	panic("not implemented")
}
func (d *mockDriver) Save(ctx context.Context, index sql.Index, iter sql.IndexKeyValueIter) error {
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

func (indexableTable) IndexKeyValueIter(_ *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	return nil, nil
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
