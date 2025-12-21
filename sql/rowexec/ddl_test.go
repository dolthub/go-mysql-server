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
	"github.com/dolthub/go-mysql-server/test"
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
	require.NoError(createTable(t, ctx, db, "testTable", s, false, false))

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s.Schema)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}

	require.Error(createTable(t, ctx, db, "testTable", s, false, false))
	require.NoError(createTable(t, ctx, db, "testTable", s, true, false))
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

	require.NoError(createTable(t, ctx, db, "testTable1", s, false, false))
	require.NoError(createTable(t, ctx, db, "testTable2", s, false, false))
	require.NoError(createTable(t, ctx, db, "testTable3", s, false, false))

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

func createTable(t *testing.T, ctx *sql.Context, db sql.Database, name string, schema sql.PrimaryKeySchema, ifNotExists, temporary bool) error {
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

// TestCreateSchemaPostgresMode tests that CREATE SCHEMA returns an error when no database is selected
// in PostgreSQL mode (when search_path session variable exists).
// See: https://github.com/dolthub/doltgresql/issues/1863
func TestCreateSchemaPostgresMode(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("testdb")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))
	ctx := newContext(pro)

	// Register search_path system variable to simulate PostgreSQL mode
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		&sql.MysqlSystemVariable{
			Name:    "search_path",
			Type:    types.NewSystemStringType("search_path"),
			Default: "public",
		},
	})

	// Initialize the session variable so GetSessionVariable can find it
	err := ctx.Session.InitSessionVariable(ctx, "search_path", "public")
	require.NoError(err)

	// Ensure no current database is set (simulating no database selected)
	ctx.SetCurrentDatabase("")

	// Create a CreateSchema plan node with Catalog set
	createSchema := plan.NewCreateSchema("test_schema", false, sql.Collation_Default)
	createSchema.Catalog = catalog

	// Build and execute - should return ErrNoDatabaseSelected in PostgreSQL mode
	_, err = DefaultBuilder.Build(ctx, createSchema, nil)
	require.Error(err)
	require.True(sql.ErrNoDatabaseSelected.Is(err), "expected ErrNoDatabaseSelected, got: %v", err)
}
