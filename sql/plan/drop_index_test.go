package plan_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	. "github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestDeleteIndex(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db := memory.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.AddDatabase(db)

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}

	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)
	close(done)
	<-ready

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)

	di := NewDropIndex("idx", NewResolvedTable(table))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(idxReg))
	_, err = di.RowIter(ctx, nil)
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal([]string{"idx"}, driver.deleted)
	require.Nil(idxReg.Index("foo", "idx"))
}

func TestDeleteIndexNotReady(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db := memory.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.AddDatabase(db)

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(idxReg))
	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)

	di := NewDropIndex("idx", NewResolvedTable(table))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	_, err = di.RowIter(ctx, nil)
	require.Error(err)
	require.True(ErrIndexNotAvailable.Is(err))

	time.Sleep(50 * time.Millisecond)

	require.Equal(([]string)(nil), driver.deleted)
	require.NotNil(idxReg.Index("foo", "idx"))

	close(done)
	<-ready
}

func TestDeleteIndexOutdated(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db := memory.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.AddDatabase(db)

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(idxReg))
	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)
	close(done)
	<-ready

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)
	idxReg.MarkOutdated(idx)

	di := NewDropIndex("idx", NewResolvedTable(table))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	_, err = di.RowIter(ctx, nil)
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal([]string{"idx"}, driver.deleted)
	require.Nil(idxReg.Index("foo", "idx"))
}
