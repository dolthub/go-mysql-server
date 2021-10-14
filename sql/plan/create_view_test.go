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

func mockCreateView(isReplace bool) *CreateView {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int32},
		{Name: "s", Source: "mytable", Type: sql.Text},
	})

	db := memory.NewViewlessDatabase("db")
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

	createView := mockCreateView(false)
	viewReg := sql.NewViewRegistry()

	ctx := sql.NewContext(context.Background(), sql.WithViewRegistry(viewReg))
	_, err := createView.RowIter(ctx, nil)
	require.NoError(err)

	expectedView := sql.NewView(createView.Name, createView.Child, createView.Definition.TextDefinition)
	actualView, err := viewReg.View(createView.database.Name(), createView.Name)
	require.NoError(err)
	require.Equal(expectedView, actualView)
}

// Tests that CreateView RowIter returns an error when the view exists
func TestCreateExistingViewWithRegistry(t *testing.T) {
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
func TestReplaceExistingViewWithRegistry(t *testing.T) {
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
	require.Equal(expectedView, actualView)
}
