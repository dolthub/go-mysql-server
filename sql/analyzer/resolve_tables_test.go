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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestResolveTables(t *testing.T) {
	require := require.New(t)
	f := getRule(resolveTablesId)

	db := memory.NewHistoryDatabase("mydb")
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{{Name: "i", Type: types.Int32}}), db.GetForeignKeyCollection())
	db.AddTableAsOf("mytable", table, "2019-01-01")

	a := NewBuilder(sql.NewDatabaseProvider(db)).AddPostAnalyzeRule(f.Id, f.Apply).Build()
	ctx := sql.NewEmptyContext()
	ctx.SetCurrentDatabase("mydb")

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable", "")
	analyzed, _, err := f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table, db, nil), analyzed)

	notAnalyzed = plan.NewUnresolvedTable("MyTable", "")
	analyzed, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table, db, nil), analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant", "")
	analyzed, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, _, err = f.Apply(ctx, a, plan.NewResolvedTable(table, db, nil), nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table, db, nil), analyzed)

	notAnalyzed = plan.NewUnresolvedTableAsOf("myTable", "", expression.NewLiteral("2019-01-01", types.LongText))
	analyzed, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table, db, "2019-01-01"), analyzed)

	notAnalyzed = plan.NewUnresolvedTableAsOf("myTable", "", expression.NewLiteral("2019-01-02", types.LongText))
	analyzed, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.Error(err)
}

func TestResolveTablesNoCurrentDB(t *testing.T) {
	require := require.New(t)
	f := getRule(resolveTablesId)

	db := memory.NewDatabase("mydb")
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{{Name: "i", Type: types.Int32}}), db.GetForeignKeyCollection())
	db.AddTable("mytable", table)

	a := NewBuilder(sql.NewDatabaseProvider(db)).AddPostAnalyzeRule(f.Id, f.Apply).Build()
	ctx := sql.NewEmptyContext()

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable", "")
	_, _, err := f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.Error(err)
	require.True(sql.ErrNoDatabaseSelected.Is(err), "wrong error kind")

	notAnalyzed = plan.NewUnresolvedTable("mytable", "doesNotExist")
	_, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.Error(err)
	require.True(sql.ErrDatabaseNotFound.Is(err), "wrong error kind")
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule(resolveTablesId)

	db := memory.NewDatabase("mydb")
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{{Name: "i", Type: types.Int32}}), db.GetForeignKeyCollection())
	table2 := memory.NewTable("my_other_table", sql.NewPrimaryKeySchema(sql.Schema{{Name: "i", Type: types.Int32}}), db.GetForeignKeyCollection())
	db.AddTable("mytable", table)

	db2 := memory.NewDatabase("my_other_db")
	db2.AddTable("my_other_table", table2)

	a := NewBuilder(sql.NewDatabaseProvider(db, db2)).AddPostAnalyzeRule(f.Id, f.Apply).Build()
	ctx := sql.NewEmptyContext()
	ctx.SetCurrentDatabase("mydb")

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, types.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable", ""),
	)
	analyzed, _, err := f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, types.Int32, "i", true)},
		plan.NewResolvedTable(table, db, nil),
	)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, types.Int32, "i", true)},
		plan.NewUnresolvedTable("my_other_table", "my_other_db"),
	)
	analyzed, _, err = f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(err)
	expected = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, types.Int32, "i", true)},
		plan.NewResolvedTable(table2, db2, nil),
	)
	require.Equal(expected, analyzed)
}
