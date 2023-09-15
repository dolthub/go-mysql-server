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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	tables := db.Tables()
	_, ok := tables["testTable"]
	require.False(ok)

	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: types.Text},
		{Name: "c2", Type: types.Int32},
	})

	ctx := newContext(pro)
	require.NoError(createTable(t, ctx, db, "testTable", s, plan.IfNotExistsAbsent, plan.IsTempTableAbsent))

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s.Schema)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}

	require.Error(createTable(t, ctx, db, "testTable", s, plan.IfNotExistsAbsent, plan.IsTempTableAbsent))
	require.NoError(createTable(t, ctx, db, "testTable", s, plan.IfNotExists, plan.IsTempTableAbsent))
}

func TestDropTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: types.Text},
		{Name: "c2", Type: types.Int32},
	})

	require.NoError(createTable(t, ctx, db, "testTable1", s, plan.IfNotExistsAbsent, plan.IsTempTableAbsent))
	require.NoError(createTable(t, ctx, db, "testTable2", s, plan.IfNotExistsAbsent, plan.IsTempTableAbsent))
	require.NoError(createTable(t, ctx, db, "testTable3", s, plan.IfNotExistsAbsent, plan.IsTempTableAbsent))

	d := plan.NewDropTable([]sql.Node{
		plan.NewResolvedTable(memory.NewTable(db.BaseDatabase, "testTable1", s, db.GetForeignKeyCollection()), db, nil),
		plan.NewResolvedTable(memory.NewTable(db.BaseDatabase, "testTable2", s, db.GetForeignKeyCollection()), db, nil),
	}, false)
	rows, err := DefaultBuilder.Build(ctx, d, nil)
	require.NoError(err)

	r, err := rows.Next(ctx)
	require.Nil(err)
	require.Equal(sql.NewRow(types.NewOkResult(0)), r)

	r, err = rows.Next(ctx)
	require.Equal(io.EOF, err)

	_, ok := db.Tables()["testTable1"]
	require.False(ok)
	_, ok = db.Tables()["testTable2"]
	require.False(ok)
	_, ok = db.Tables()["testTable3"]
	require.True(ok)

	d = plan.NewDropTable([]sql.Node{plan.NewResolvedTable(memory.NewTable(db.Database(), "testTable1", s, db.GetForeignKeyCollection()), db, nil)}, false)
	_, err = DefaultBuilder.Build(ctx, d, nil)
	require.Error(err)

	d = plan.NewDropTable([]sql.Node{plan.NewResolvedTable(memory.NewTable(db.Database(), "testTable3", s, db.GetForeignKeyCollection()), db, nil)}, false)
	_, err = DefaultBuilder.Build(ctx, d, nil)
	require.NoError(err)

	_, ok = db.Tables()["testTable3"]
	require.False(ok)
}

func createTable(t *testing.T, ctx *sql.Context, db sql.Database, name string, schema sql.PrimaryKeySchema, ifNotExists plan.IfNotExistsOption, temporary plan.TempTableOption) error {
	c := plan.NewCreateTable(db, name, ifNotExists, temporary, &plan.TableSpec{Schema: schema})

	rows, err := DefaultBuilder.Build(ctx, c, nil)
	if err != nil {
		return err
	}

	r, err := rows.Next(ctx)
	require.Nil(t, err)
	require.Equal(t, sql.NewRow(types.NewOkResult(0)), r)

	r, err = rows.Next(ctx)
	require.Equal(t, io.EOF, err)
	return nil
}
