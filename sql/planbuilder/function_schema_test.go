// Copyright 2026 Dolthub, Inc.
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

package planbuilder

import (
	"context"
	"strings"
	"testing"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestFunctionSchema tests that only a function's own qualifier is passed to the function provider. An unqualified
// name is left for the provider to resolve, whatever the schema of the tables that the statement references.
func TestFunctionSchema(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		schemas []string
	}{
		{
			name:    "select from a table in another schema",
			query:   "select abs(x), s.abs(x) from t where abs(x) = 1",
			schemas: []string{"", "s", ""},
		},
		{
			name:    "alter a table in another schema",
			query:   "alter table t add column y int default (abs(1))",
			schemas: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memory.NewDatabase("mydb")
			pro := memory.NewDBProvider(db)
			ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))
			ctx.SetCurrentDatabase("mydb")
			schemaDb := &functionSchemaTestDatabase{Database: db, schema: memory.NewDatabase("s")}
			schemaDb.schema.AddTable("t", memory.NewTable(ctx, schemaDb.schema, "t", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "x", Type: types.Int64, Source: "t"},
			}), nil))

			cat := &functionSchemaTestCatalog{
				MapCatalog: sql.MapCatalog{
					Databases: map[string]sql.Database{"mydb": schemaDb},
					Funcs:     function.NewRegistry(),
				},
			}

			stmt, err := ast.Parse(tt.query)
			require.NoError(t, err)
			// The MySQL parser has no syntax for a table's schema, so the statement's table is put in schema s here
			switch stmt := stmt.(type) {
			case *ast.Select:
				tableExpr := stmt.From[0].(*ast.AliasedTableExpr)
				tableName := tableExpr.Expr.(ast.TableName)
				tableName.SchemaQualifier = ast.NewTableIdent("s")
				tableExpr.Expr = tableName
			case *ast.AlterTable:
				stmt.Table.SchemaQualifier = ast.NewTableIdent("s")
				for _, ddl := range stmt.Statements {
					ddl.Table.SchemaQualifier = ast.NewTableIdent("s")
				}
			}

			b := New(ctx, cat, nil)
			b.build(nil, stmt, tt.query)
			require.ElementsMatch(t, tt.schemas, cat.schemas)
		})
	}
}

// functionSchemaTestDatabase is a database with a single schema, named s.
type functionSchemaTestDatabase struct {
	*memory.Database
	schema *memory.Database
}

var _ sql.SchemaDatabase = (*functionSchemaTestDatabase)(nil)

// SupportsDatabaseSchemas implements the interface sql.SchemaDatabase.
func (d *functionSchemaTestDatabase) SupportsDatabaseSchemas() bool {
	return true
}

// GetSchema implements the interface sql.SchemaDatabase.
func (d *functionSchemaTestDatabase) GetSchema(_ *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	if !strings.EqualFold(schemaName, "s") {
		return nil, false, nil
	}
	return functionSchemaTestSchema{Database: d.schema}, true, nil
}

// CreateSchema implements the interface sql.SchemaDatabase.
func (d *functionSchemaTestDatabase) CreateSchema(_ *sql.Context, schemaName string) error {
	return sql.ErrDatabaseSchemasNotSupported.New(d.Name())
}

// AllSchemas implements the interface sql.SchemaDatabase.
func (d *functionSchemaTestDatabase) AllSchemas(_ *sql.Context) ([]sql.DatabaseSchema, error) {
	return []sql.DatabaseSchema{functionSchemaTestSchema{Database: d.schema}}, nil
}

// DropSchema implements the interface sql.SchemaDatabase.
func (d *functionSchemaTestDatabase) DropSchema(_ *sql.Context, schemaName string) error {
	return sql.ErrDatabaseSchemasNotSupported.New(d.Name())
}

// functionSchemaTestSchema is the schema s of a functionSchemaTestDatabase.
type functionSchemaTestSchema struct {
	*memory.Database
}

// SchemaName implements the interface sql.DatabaseSchema.
func (s functionSchemaTestSchema) SchemaName() string {
	return "s"
}

// functionSchemaTestCatalog records the schema of every function that is looked up.
type functionSchemaTestCatalog struct {
	sql.MapCatalog
	schemas []string
}

// Function implements the interface sql.FunctionProvider.
func (c *functionSchemaTestCatalog) Function(ctx *sql.Context, schema, name string) (sql.Function, bool) {
	c.schemas = append(c.schemas, schema)
	return c.MapCatalog.Function(ctx, "", name)
}
