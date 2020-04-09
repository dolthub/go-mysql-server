package analyzer

import (
	"context"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestAssignCatalog(t *testing.T) {
	require := require.New(t)
	f := getRule("assign_catalog")

	db := memory.NewDatabase("foo")
	c := sql.NewCatalog()
	c.AddDatabase(db)

	a := NewDefault(c)
	idxReg := sql.NewIndexRegistry()
	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("foo")

	tbl := memory.NewTable("foo", nil)


	node, err := f.Apply(ctx, a,
		plan.NewCreateIndex("", plan.NewResolvedTable(tbl), nil, "", make(map[string]string)))
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(ctx, a,
		plan.NewDropIndex("foo", plan.NewResolvedTable(tbl)))
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)

	node, err = f.Apply(ctx, a, plan.NewShowIndexes(db, "table-test", nil))
	require.NoError(err)

	si, ok := node.(*plan.ShowIndexes)
	require.True(ok)
	require.Equal(db, si.Database())
	require.Equal(idxReg, si.Registry)

	node, err = f.Apply(ctx, a, plan.NewShowProcessList())
	require.NoError(err)

	pl, ok := node.(*plan.ShowProcessList)
	require.True(ok)
	require.Equal(db.Name(), pl.Database)
	require.Equal(c.ProcessList, pl.ProcessList)

	node, err = f.Apply(ctx, a, plan.NewShowDatabases())
	require.NoError(err)
	sd, ok := node.(*plan.ShowDatabases)
	require.True(ok)
	require.Equal(c, sd.Catalog)

	node, err = f.Apply(ctx, a, plan.NewLockTables(nil))
	require.NoError(err)
	lt, ok := node.(*plan.LockTables)
	require.True(ok)
	require.Equal(c, lt.Catalog)

	node, err = f.Apply(ctx, a, plan.NewUnlockTables())
	require.NoError(err)
	ut, ok := node.(*plan.UnlockTables)
	require.True(ok)
	require.Equal(c, ut.Catalog)

	mockSubquery := plan.NewSubqueryAlias("mock", "", plan.NewResolvedTable(tbl))
	mockView := plan.NewCreateView(db, "", nil, mockSubquery, "select * from foo", false)
	node, err = f.Apply(ctx, a, mockView)
	require.NoError(err)
	cv, ok := node.(*plan.CreateView)
	require.True(ok)
	require.Equal(c, cv.Catalog)

	node, err = f.Apply(ctx, a, plan.NewDropView(nil, false))
	require.NoError(err)
	dv, ok := node.(*plan.DropView)
	require.True(ok)
	require.Equal(c, dv.Catalog)
}
