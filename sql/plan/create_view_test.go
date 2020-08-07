package plan

import (
	"context"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func mockCreateView(isReplace bool) *CreateView {
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

	createView := NewCreateView(db, subqueryAlias.Name(), nil, subqueryAlias, isReplace)
	createView.Catalog = catalog

	return createView
}

// Tests that CreateView works as expected and that the view is registered in
// the catalog when RowIter is called
func TestCreateView(t *testing.T) {
	require := require.New(t)

	createView := mockCreateView(false)
	viewReg := sql.NewViewRegistry()

	ctx := sql.NewContext(context.Background(), sql.WithViewRegistry(viewReg))
	_, err := createView.RowIter(ctx, nil)
	require.NoError(err)

	expectedView := sql.NewView(createView.Name, createView.Child, createView.Definition.TextDefinition)
	actualView, err := viewReg.View(createView.database.Name(), createView.Name)
	require.NoError(err)
	require.Equal(expectedView, *actualView)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingView(t *testing.T) {
	require := require.New(t)

	createView := mockCreateView(false)

	view := createView.View()
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.database.Name(), view)
	require.NoError(err)

	ctx := sql.NewContext(context.Background(), sql.WithViewRegistry(viewReg))
	_, err = createView.RowIter(ctx, nil)
	require.Error(err)
	require.True(sql.ErrExistingView.Is(err))
}

// Tests that CreateView RowIter succeeds when the view exists and the
// IsReplace flag is set to true
func TestReplaceExistingView(t *testing.T) {
	require := require.New(t)

	createView := mockCreateView(true)

	view := sql.NewView(createView.Name, nil, "")
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.database.Name(), view)
	require.NoError(err)

	createView.IsReplace = true

	ctx := sql.NewContext(context.Background(), sql.WithViewRegistry(viewReg))
	_, err = createView.RowIter(ctx, nil)
	require.NoError(err)

	expectedView := createView.View()
	actualView, err := viewReg.View(createView.database.Name(), createView.Name)
	require.NoError(err)
	require.Equal(expectedView, *actualView)
}
