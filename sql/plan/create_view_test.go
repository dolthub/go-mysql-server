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

package plan

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func newCreateView(db memory.MemoryDatabase, isReplace bool) *CreateView {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int32},
		{Name: "s", Source: "mytable", Type: sql.Text},
	}), nil)

	db.AddTable("db", table)

	subqueryAlias := NewSubqueryAlias("myview", "select i",
		NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Int32, table.Name(), "i", true),
			},
			NewUnresolvedTable("dual", ""),
		),
	)

	createView := NewCreateView(db, subqueryAlias.Name(), nil, subqueryAlias, isReplace)

	return createView
}

// Tests that CreateView works as expected and that the view is registered in
// the catalog when RowIter is called
func TestCreateViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	ctx := sql.NewContext(context.Background())
	_, err := createView.RowIter(ctx, nil)
	require.NoError(err)

	expectedView := sql.NewView(createView.Name, createView.Child, createView.Definition.TextDefinition)
	actualView, err := ctx.GetViewRegistry().View(createView.database.Name(), createView.Name)
	require.NoError(err)
	require.Equal(expectedView, actualView)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingViewNative(t *testing.T) {
	createView := newCreateView(memory.NewDatabase("mydb"), false)

	ctx := sql.NewContext(context.Background())
	_, err := createView.RowIter(ctx, nil)
	require.NoError(t, err)

	ctx = sql.NewContext(context.Background())
	_, err = createView.RowIter(ctx, nil)
	require.Error(t, err)
	require.True(t, sql.ErrExistingView.Is(err))
}

// Tests that CreateView RowIter succeeds when the view exists and the
// IsReplace flag is set to true
func TestReplaceExistingViewNative(t *testing.T) {
	db := memory.NewDatabase("mydb")
	createView := newCreateView(db, false)

	ctx := sql.NewContext(context.Background())
	_, err := createView.RowIter(ctx, nil)
	require.NoError(t, err)

	expectedView := createView.Definition.TextDefinition
	view, ok, err := db.GetView(ctx, createView.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expectedView, view)

	// This is kind of nonsensical, but we just want to see if it gets stored correctly
	subqueryAlias := NewSubqueryAlias("myview", "select i + 1",
		NewProject(
			[]sql.Expression{
				expression.NewArithmetic(
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "i", true),
					expression.NewLiteral(1, sql.Int8),
					"+",
				),
			},
			NewUnresolvedTable("dual", ""),
		),
	)

	createView = NewCreateView(db, subqueryAlias.Name(), nil, subqueryAlias, true)
	_, err = createView.RowIter(ctx, nil)
	require.NoError(t, err)

	view, ok, err = db.GetView(ctx, createView.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, subqueryAlias.TextDefinition, view)
}

// Tests that CreateView works as expected and that the view is registered in
// the catalog when RowIter is called
func TestCreateViewNative(t *testing.T) {
	db := memory.NewDatabase("mydb")
	createView := newCreateView(db, false)

	ctx := sql.NewContext(context.Background())
	_, err := createView.RowIter(ctx, nil)
	require.NoError(t, err)

	actualView, ok, err := db.GetView(ctx, createView.Name)

	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, createView.Definition.TextDefinition, actualView)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	view := createView.View()
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.database.Name(), view)
	require.NoError(err)

	sess := sql.NewBaseSession()
	sess.SetViewRegistry(viewReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err = createView.RowIter(ctx, nil)
	require.Error(err)
	require.True(sql.ErrExistingView.Is(err))
}

// Tests that CreateView RowIter succeeds when the view exists and the
// IsReplace flag is set to true
func TestReplaceExistingViewWithRegistry(t *testing.T) {
	require := require.New(t)

	createView := newCreateView(memory.NewViewlessDatabase("mydb"), false)

	view := sql.NewView(createView.Name, nil, "")
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(createView.database.Name(), view)
	require.NoError(err)

	createView.IsReplace = true

	sess := sql.NewBaseSession()
	sess.SetViewRegistry(viewReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err = createView.RowIter(ctx, nil)
	require.NoError(err)

	expectedView := createView.View()
	actualView, err := ctx.GetViewRegistry().View(createView.database.Name(), createView.Name)
	require.NoError(err)
	require.Equal(expectedView, actualView)
}
