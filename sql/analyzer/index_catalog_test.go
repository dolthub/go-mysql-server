package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestCatalogIndex(t *testing.T) {
	require := require.New(t)
	f := getRule("index_catalog")

	db := mem.NewDatabase("foo")
	c := sql.NewCatalog()
	c.AddDatabase(db)

	a := NewDefault(c)
	a.CurrentDatabase = "foo"
	a.Catalog.IndexRegistry = sql.NewIndexRegistry()

	tbl := mem.NewTable("foo", nil)

	node, err := f.Apply(sql.NewEmptyContext(), a,
		plan.NewCreateIndex("", plan.NewResolvedTable("foo", tbl), nil, "", make(map[string]string)))
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a,
		plan.NewDropIndex("foo", plan.NewResolvedTable("foo", tbl)))
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewShowIndexes(db, "table-test", nil))
	require.NoError(err)

	si, ok := node.(*plan.ShowIndexes)
	require.True(ok)
	require.Equal(db, si.Database)
	require.Equal(c.IndexRegistry, si.Registry)
}
