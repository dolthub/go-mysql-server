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

	c := sql.NewCatalog()
	a := NewDefault(c)
	a.CurrentDatabase = "foo"

	tbl := mem.NewTable("foo", nil)

	node, err := f.Apply(sql.NewEmptyContext(), a, plan.NewCreateIndex("", tbl, nil, "", make(map[string]string)))
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewDropIndex("foo", tbl))
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)
}
