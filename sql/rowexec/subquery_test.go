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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSubquery(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db, "", sql.PrimaryKeySchema{}, nil)
	require.NoError(table.Insert(ctx, nil))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral("one", types.LongText),
		},
		plan.NewResolvedTable(table, nil, nil),
	), "select 'one'").WithExecBuilder(DefaultBuilder)

	value, err := subquery.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(value, "one")
}

func TestSubqueryTooManyRows(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db, "", sql.PrimaryKeySchema{}, nil)
	require.NoError(table.Insert(ctx, nil))
	require.NoError(table.Insert(ctx, nil))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral("one", types.LongText),
		},
		plan.NewResolvedTable(table, nil, nil),
	), "select 'one'").WithExecBuilder(DefaultBuilder)

	_, err := subquery.Eval(ctx, nil)
	require.Error(err)
}

func TestSubqueryMultipleRows(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "t", Source: "foo", Type: types.Text},
	}), nil)

	require.NoError(table.Insert(ctx, sql.Row{"one"}))
	require.NoError(table.Insert(ctx, sql.Row{"two"}))
	require.NoError(table.Insert(ctx, sql.Row{"three"}))

	subquery := plan.NewSubquery(plan.NewProject(
		[]sql.Expression{
			expression.NewGetField(0, types.Text, "t", false),
		},
		plan.NewResolvedTable(table, nil, nil),
	), "select t from foo").WithExecBuilder(DefaultBuilder)

	values, err := subquery.EvalMultiple(ctx, nil)
	require.NoError(err)
	require.Equal(values, []interface{}{"one", "two", "three"})
}
