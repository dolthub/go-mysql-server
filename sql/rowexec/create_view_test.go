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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func newCreateView(db memory.MemoryDatabase, isReplace bool) *plan.CreateView {
	table := memory.NewTable(db.Database(), "mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Source: "mytable", Type: types.Int32},
		{Name: "s", Source: "mytable", Type: types.Text},
	}), nil)

	db.AddTable("db", table)

	subqueryAlias := plan.NewSubqueryAlias("myview", "select i from mytable",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, types.Int32, "db", table.Name(), "i", true),
			},
			plan.NewUnresolvedTable(table.Name(), ""),
		),
	)

	createView := plan.NewCreateView(db, subqueryAlias.Name(), subqueryAlias, isReplace, "CREATE VIEW myview AS SELECT i FROM mytable", "", "", "")

	return createView
}

// Tests that CreateView works as expected and that the view is registered in
// the catalog when RowIter is called
func TestCreateViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	ctx := sql.NewContext(context.Background())
	_, err := DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(err)

	expectedView := sql.NewView(createView.Name, createView.Child, createView.Definition.TextDefinition, createView.CreateViewString)
	actualView, ok := ctx.GetViewRegistry().View(createView.Database().Name(), createView.Name)
	require.True(ok)
	require.Equal(expectedView, actualView)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingViewNative(t *testing.T) {
	createView := newCreateView(memory.NewDatabase("mydb"), false)

	ctx := sql.NewContext(context.Background())
	_, err := DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(t, err)

	ctx = sql.NewContext(context.Background())
	_, err = DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.Error(t, err)
	require.True(t, sql.ErrExistingView.Is(err))
}

// Tests that CreateView RowIter succeeds when the view exists and the
// IsReplace flag is set to true
func TestReplaceExistingViewNative(t *testing.T) {
	db := memory.NewDatabase("mydb")
	createView := newCreateView(db, false)

	ctx := sql.NewContext(context.Background())
	_, err := DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(t, err)

	expectedViewTextDef := createView.Definition.TextDefinition
	view, ok, err := db.GetViewDefinition(ctx, createView.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expectedViewTextDef, view.TextDefinition)

	// This is kind of nonsensical, but we just want to see if it gets stored correctly
	subqueryAlias := plan.NewSubqueryAlias("myview", "select i + 1 from mytable",
		plan.NewProject(
			[]sql.Expression{
				expression.NewArithmetic(
					expression.NewGetFieldWithTable(1, types.Int32, "db", "mytable", "i", true),
					expression.NewLiteral(1, types.Int8),
					"+",
				),
			},
			plan.NewUnresolvedTable("mytable", ""),
		),
	)

	createView = plan.NewCreateView(db, subqueryAlias.Name(), subqueryAlias, true, "CREATE VIEW myview AS SELECT i + 1 FROM mytable", "", "", "")
	_, err = DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(t, err)

	view, ok, err = db.GetViewDefinition(ctx, createView.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, subqueryAlias.TextDefinition, view.TextDefinition)
}

// Tests that CreateView works as expected and that the view is registered in
// the catalog when RowIter is called
func TestCreateViewNative(t *testing.T) {
	db := memory.NewDatabase("mydb")
	createView := newCreateView(db, false)

	ctx := sql.NewContext(context.Background())
	_, err := DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(t, err)

	actualView, ok, err := db.GetViewDefinition(ctx, createView.Name)

	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, createView.Definition.TextDefinition, actualView.TextDefinition)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	view := createView.View()
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.Database().Name(), view)
	require.NoError(err)

	sess := sql.NewBaseSession()
	sess.SetViewRegistry(viewReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err = DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.Error(err)
	require.True(sql.ErrExistingView.Is(err))
}

// Tests that CreateView RowIter succeeds when the view exists and the
// IsReplace flag is set to true
func TestReplaceExistingViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	view := sql.NewView(createView.Name, nil, "", "")
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.Database().Name(), view)
	require.NoError(err)

	createView.IsReplace = true

	sess := sql.NewBaseSession()
	sess.SetViewRegistry(viewReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err = DefaultBuilder.buildNodeExec(ctx, createView, nil)
	require.NoError(err)

	expectedView := createView.View()
	actualView, ok := ctx.GetViewRegistry().View(createView.Database().Name(), createView.Name)
	require.True(ok)
	require.Equal(expectedView, actualView)
}
