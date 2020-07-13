package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
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
		plan.NewCreateIndex("", plan.NewResolvedTable(tbl), nil, "", make(map[string]string)), nil)
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(ctx, a,
		plan.NewDropIndex("foo", plan.NewResolvedTable(tbl)), nil)
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)

	node, err = f.Apply(ctx, a, plan.NewShowProcessList(), nil)
	require.NoError(err)

	pl, ok := node.(*plan.ShowProcessList)
	require.True(ok)
	require.Equal(db.Name(), pl.Database)
	require.Equal(c.ProcessList, pl.ProcessList)

	node, err = f.Apply(ctx, a, plan.NewShowDatabases(), nil)
	require.NoError(err)
	sd, ok := node.(*plan.ShowDatabases)
	require.True(ok)
	require.Equal(c, sd.Catalog)

	node, err = f.Apply(ctx, a, plan.NewLockTables(nil), nil)
	require.NoError(err)
	lt, ok := node.(*plan.LockTables)
	require.True(ok)
	require.Equal(c, lt.Catalog)

	node, err = f.Apply(ctx, a, plan.NewUnlockTables(), nil)
	require.NoError(err)
	ut, ok := node.(*plan.UnlockTables)
	require.True(ok)
	require.Equal(c, ut.Catalog)

	mockSubquery := plan.NewSubqueryAlias("mock", "", plan.NewResolvedTable(tbl))
	mockView := plan.NewCreateView(db, "", nil, mockSubquery, false)
	node, err = f.Apply(ctx, a, mockView, nil)
	require.NoError(err)
	cv, ok := node.(*plan.CreateView)
	require.True(ok)
	require.Equal(c, cv.Catalog)

	node, err = f.Apply(ctx, a, plan.NewDropView(nil, false), nil)
	require.NoError(err)
	dv, ok := node.(*plan.DropView)
	require.True(ok)
	require.Equal(c, dv.Catalog)
}
