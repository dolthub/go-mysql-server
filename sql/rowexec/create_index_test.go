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

package rowexec

import (
	"context"
	"io"
	"math"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	. "github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/test"

	"github.com/stretchr/testify/require"
)

func TestCreateIndexAsync(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}), nil)

	idxReg := sql.NewIndexRegistry()
	driver := new(mockDriver)
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", true),
	}

	ci := NewCreateIndex("idx", NewResolvedTable(table, nil, nil), exprs, "mock", map[string]string{
		"async": "true",
	})
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	tracer := new(test.MemTracer)
	pro := memory.NewDBProvider(db)
	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer), sql.WithSession(memory.NewSession(baseSession, pro)))
	_, err := DefaultBuilder.Build(ctx, ci, nil)

	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Len(driver.deleted, 0)
	require.Equal([]string{"idx"}, driver.saved)
	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	require.Equal(&mockIndex{db: "foo", table: "foo", id: "idx", exprs: []sql.Expression{
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "a", true),
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

	db := memory.NewDatabase("foo")
	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo", Type: types.Blob},
		{Name: "b", Source: "foo", Type: types.JSON},
		{Name: "c", Source: "foo", Type: types.Text},
	}), nil)

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	ci := NewCreateIndex(
		"idx",
		NewResolvedTable(table, nil, nil),
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, types.Blob, "db", "foo", "a", true),
		},
		"mock",
		make(map[string]string),
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	sess := sql.NewBaseSession()
	sess.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err := DefaultBuilder.Build(ctx, ci, nil)
	require.Error(err)
	require.True(ErrExprTypeNotIndexable.Is(err))

	ci = NewCreateIndex(
		"idx",
		NewResolvedTable(table, nil, nil),
		[]sql.Expression{
			expression.NewGetFieldWithTable(1, types.JSON, "db", "foo", "a", true),
		},
		"mock",
		make(map[string]string),
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	_, err = DefaultBuilder.Build(ctx, ci, nil)
	require.Error(err)
	require.True(ErrExprTypeNotIndexable.Is(err))
}

func TestCreateIndexSync(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}), nil)

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", true),
	}

	ci := NewCreateIndex(
		"idx", NewResolvedTable(table, nil, nil), exprs, "mock",
		map[string]string{"async": "false"},
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	tracer := new(test.MemTracer)

	pro := memory.NewDBProvider(db)
	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer), sql.WithSession(memory.NewSession(baseSession, pro)))

	_, err := DefaultBuilder.Build(ctx, ci, nil)
	require.NoError(err)

	require.Len(driver.deleted, 0)
	require.Equal([]string{"idx"}, driver.saved)
	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	require.Equal(&mockIndex{db: "foo", table: "foo", id: "idx", exprs: []sql.Expression{
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "a", true),
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

func TestCreateIndexChecksum(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	table := &checksumTable{
		memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Source: "foo"},
			{Name: "b", Source: "foo"},
			{Name: "c", Source: "foo"},
		}), nil),
		"1",
	}

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", true),
	}

	ci := NewCreateIndex(
		"idx", NewResolvedTable(table, nil, nil), exprs, "mock",
		map[string]string{"async": "false"},
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	pro := memory.NewDBProvider(db)
	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSession, pro)))

	_, err := DefaultBuilder.Build(ctx, ci, nil)
	require.NoError(err)

	require.Equal([]string{"idx"}, driver.saved)
	require.Equal("1", driver.config["idx"][sql.ChecksumKey])
}

func TestCreateIndexChecksumWithUnderlying(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	table :=
		&underlyingTable{
			&underlyingTable{
				&underlyingTable{
					&checksumTable{
						memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
							{Name: "a", Source: "foo"},
							{Name: "b", Source: "foo"},
							{Name: "c", Source: "foo"},
						}), nil),
						"1",
					},
				},
			},
		}

	driver := new(mockDriver)
	catalog := test.NewCatalog(sql.NewDatabaseProvider())
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)

	exprs := []sql.Expression{
		expression.NewGetFieldWithTable(2, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", true),
	}

	ci := NewCreateIndex(
		"idx", NewResolvedTable(table, nil, nil), exprs, "mock",
		map[string]string{"async": "false"},
	)
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	pro := memory.NewDBProvider(db)
	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSession, pro)))

	_, err := DefaultBuilder.Build(ctx, ci, nil)
	require.NoError(err)

	require.Equal([]string{"idx"}, driver.saved)
	require.Equal("1", driver.config["idx"][sql.ChecksumKey])
}

func TestCreateIndexWithIter(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("foo")
	pro := memory.NewDBProvider(db)

	foo := memory.NewPartitionedTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "one", Source: "foo", Type: types.Int64},
		{Name: "two", Source: "foo", Type: types.Int64},
	}), nil, 2)

	rows := [][2]int64{
		{1, 2},
		{-1, -2},
		{0, 0},
		{math.MaxInt64, math.MinInt64},
	}

	for _, r := range rows {
		err := foo.Insert(newContext(pro), sql.NewRow(r[0], r[1]))
		require.NoError(err)
	}

	exprs := []sql.Expression{expression.NewPlus(
		expression.NewGetField(0, types.Int64, "one", false),
		expression.NewGetField(0, types.Int64, "two", false)),
	}

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", foo)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	ci := NewCreateIndex("idx", NewResolvedTable(foo, nil, nil), exprs, "mock", make(map[string]string))
	ci.Catalog = catalog
	ci.CurrentDatabase = "foo"

	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSession, pro)))

	columns, exprs, err := GetColumnsAndPrepareExpressions(ci.Exprs)
	require.NoError(err)

	iter, err := foo.IndexKeyValues(ctx, columns)
	require.NoError(err)

	iter = NewEvalPartitionKeyValueIter(iter, columns, exprs)

	var (
		vals [][]interface{}
		i    int
	)

	for {
		_, kviter, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(err)

		vals = append(vals, nil)

		for {
			values, _, err := kviter.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(err)

			vals[i] = append(vals[i], values...)
		}

		require.NoError(kviter.Close(ctx))

		i++
	}
	require.NoError(iter.Close(ctx))

	require.Equal([][]interface{}{
		{int64(3), int64(0)},
		{int64(-3), int64(-1)},
	}, vals)
}

type mockIndex struct {
	db       string
	table    string
	id       string
	exprs    []sql.Expression
	unique   bool
	spatial  bool
	fulltext bool
	comment  string
}

var _ sql.DriverIndex = (*mockIndex)(nil)

func (i *mockIndex) CanSupport(r ...sql.Range) bool { return false }
func (i *mockIndex) ID() string                     { return i.id }
func (i *mockIndex) Table() string                  { return i.table }
func (i *mockIndex) Database() string               { return i.db }
func (i *mockIndex) IsUnique() bool                 { return i.unique }
func (i *mockIndex) IsSpatial() bool                { return i.spatial }
func (i *mockIndex) IsFullText() bool               { return i.fulltext }
func (i *mockIndex) Comment() string                { return i.comment }
func (i *mockIndex) IndexType() string              { return "BTREE" }
func (i *mockIndex) IsGenerated() bool              { return false }
func (i *mockIndex) PrefixLengths() []uint16        { return nil }
func (i *mockIndex) Expressions() []string {
	exprs := make([]string, len(i.exprs))
	for i, e := range i.exprs {
		exprs[i] = e.String()
	}

	return exprs
}

func (*mockIndex) ColumnExpressionTypes() []sql.ColumnExpressionType {
	panic("unimplemented")
}

func (*mockIndex) Build(ctx *sql.Context) (sql.IndexLookup, error) { panic("unimplemented") }
func (*mockIndex) Driver() string                                  { return "mock" }

type mockDriver struct {
	config  map[string]map[string]string
	deleted []string
	saved   []string
}

var _ sql.IndexDriver = (*mockDriver)(nil)

func (*mockDriver) ID() string { return "mock" }
func (d *mockDriver) Create(db, table, id string, exprs []sql.Expression, config map[string]string) (sql.DriverIndex, error) {
	if d.config == nil {
		d.config = make(map[string]map[string]string)
	}
	d.config[id] = config

	return &mockIndex{db: db, table: table, id: id, exprs: exprs}, nil
}
func (*mockDriver) LoadAll(ctx *sql.Context, db, table string) ([]sql.DriverIndex, error) {
	panic("not implemented")
}

func (d *mockDriver) Save(ctx *sql.Context, index sql.DriverIndex, iter sql.PartitionIndexKeyValueIter) error {
	d.saved = append(d.saved, index.ID())
	return nil
}
func (d *mockDriver) Delete(index sql.DriverIndex, _ sql.PartitionIter) error {
	d.deleted = append(d.deleted, index.ID())
	return nil
}

type checksumTable struct {
	sql.Table
	checksum string
}

func (t *checksumTable) IgnoreSessionData() bool {
	return true
}

func (t *checksumTable) UnderlyingTable() *memory.Table {
	return t.Table.(*memory.Table)
}

func (t *checksumTable) Checksum() (string, error) {
	return t.checksum, nil
}

func (t *checksumTable) Underlying() sql.Table { return t.Table }

type underlyingTable struct {
	sql.Table
}

func (t *underlyingTable) Underlying() sql.Table { return t.Table }
