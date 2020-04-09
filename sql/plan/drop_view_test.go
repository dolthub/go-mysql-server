package plan

import (
	"context"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

// Generates a database with a single table called mytable and a catalog with
// the view that is also returned. The context returned is the one used to
// create the view.
func mockData(require *require.Assertions) (sql.Database, *sql.Catalog, *sql.Context, sql.View) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int32},
		{Name: "s", Source: "mytable", Type: sql.Text},
	})

	db := memory.NewDatabase("db")
	db.AddTable("db", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	subqueryAlias := NewSubqueryAlias("myview", "select i",
		NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Int32, table.Name(), "i", true),
			},
			NewUnresolvedTable("dual", ""),
		),
	)

	createView := NewCreateView(db, subqueryAlias.Name(), nil, subqueryAlias, "select i from dual", false)
	createView.Catalog = catalog

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))

	_, err := createView.RowIter(ctx)
	require.NoError(err)

	return db, catalog, ctx, createView.View()
}

// Tests that DropView works as expected and that the view is dropped in
// the catalog when RowIter is called, regardless of the value of ifExists
func TestDropExistingView(t *testing.T) {
	require := require.New(t)

	test := func(ifExists bool) {
		db, catalog, ctx, view := mockData(require)

		singleDropView := NewSingleDropView(db, view.Name())
		dropView := NewDropView([]sql.Node{singleDropView}, ifExists)
		dropView.Catalog = catalog

		_, err := dropView.RowIter(ctx)
		require.NoError(err)

		require.False(ctx.Exists(db.Name(), view.Name()))
	}

	test(false)
	test(true)
}

// Tests that DropView errors when trying to delete a non-existing view if and
// only if the flag ifExists is set to false
func TestDropNonExistingView(t *testing.T) {
	require := require.New(t)

	test := func(ifExists bool) error {
		db, catalog, ctx, view := mockData(require)

		singleDropView := NewSingleDropView(db, "non-existing-view")
		dropView := NewDropView([]sql.Node{singleDropView}, ifExists)
		dropView.Catalog = catalog

		_, err := dropView.RowIter(ctx)

		require.True(ctx.Exists(db.Name(), view.Name()))

		return err
	}

	err := test(true)
	require.NoError(err)

	err = test(false)
	require.Error(err)
}
