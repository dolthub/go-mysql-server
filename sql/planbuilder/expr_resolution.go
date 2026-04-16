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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

// NewBuilderForColumnDefaultResolution creates a Builder suitable for resolving column default
// and generated expressions in a schema (e.g. UnresolvedColumnDefault placeholders). It uses a minimal
// catalog backed only by the built-in function registry — sufficient because generated column
// expressions may only reference columns of the same table and built-in SQL functions.
func NewBuilderForColumnDefaultResolution(ctx *sql.Context, overrides sql.EngineOverrides) *Builder {
	return New(ctx, &exprResolutionCatalog{
		functions: function.NewRegistry(),
		overrides: overrides,
	}, nil)
}

// exprResolutionCatalog is a minimal sql.Catalog implementation used exclusively for
// resolving column default / generated expressions. Only the methods actually invoked
// by planbuilder.Builder.ResolveSchemaDefaults are implemented; everything else is a no-op.
type exprResolutionCatalog struct {
	functions function.Registry
	overrides sql.EngineOverrides
}

var _ sql.Catalog = (*exprResolutionCatalog)(nil)

func (c *exprResolutionCatalog) Function(ctx *sql.Context, name string) (sql.Function, bool) {
	return c.functions.Function(ctx, name)
}

func (c *exprResolutionCatalog) TableFunction(_ *sql.Context, _ string) (sql.TableFunction, bool) {
	return nil, false
}

func (c *exprResolutionCatalog) WithTableFunctions(_ ...sql.TableFunction) (sql.TableFunctionProvider, error) {
	return c, nil
}

func (c *exprResolutionCatalog) AuthorizationHandler() sql.AuthorizationHandler {
	return sql.NoopAuthorizationHandler{}
}

func (c *exprResolutionCatalog) Overrides() sql.EngineOverrides {
	return c.overrides
}

func (c *exprResolutionCatalog) AllDatabases(_ *sql.Context) []sql.Database { return nil }

func (c *exprResolutionCatalog) HasDatabase(_ *sql.Context, _ string) bool { return false }

func (c *exprResolutionCatalog) Database(_ *sql.Context, _ string) (sql.Database, error) {
	return nil, sql.ErrDatabaseNotFound.New("")
}

func (c *exprResolutionCatalog) CreateCollatedDatabase(_ *sql.Context, _ string, _ sql.CollationID) error {
	return nil
}

func (c *exprResolutionCatalog) CreateDatabase(_ *sql.Context, _ string, _ sql.CollationID) error {
	return nil
}

func (c *exprResolutionCatalog) RemoveDatabase(_ *sql.Context, _ string) error { return nil }

func (c *exprResolutionCatalog) Table(_ *sql.Context, _, _ string) (sql.Table, sql.Database, error) {
	return nil, nil, sql.ErrTableNotFound.New("")
}

func (c *exprResolutionCatalog) DatabaseTable(_ *sql.Context, _ sql.Database, _ string) (sql.Table, sql.Database, error) {
	return nil, nil, sql.ErrTableNotFound.New("")
}

func (c *exprResolutionCatalog) TableAsOf(_ *sql.Context, _, _ string, _ interface{}) (sql.Table, sql.Database, error) {
	return nil, nil, sql.ErrTableNotFound.New("")
}

func (c *exprResolutionCatalog) DatabaseTableAsOf(_ *sql.Context, _ sql.Database, _ string, _ interface{}) (sql.Table, sql.Database, error) {
	return nil, nil, sql.ErrTableNotFound.New("")
}

func (c *exprResolutionCatalog) LockTable(_ *sql.Context, _ string) {}

func (c *exprResolutionCatalog) UnlockTables(_ *sql.Context, _ uint32) error { return nil }

func (c *exprResolutionCatalog) ExternalStoredProcedure(_ *sql.Context, _ string, _ int) (*sql.ExternalStoredProcedureDetails, error) {
	return nil, nil
}

func (c *exprResolutionCatalog) ExternalStoredProcedures(_ *sql.Context, _ string) ([]sql.ExternalStoredProcedureDetails, error) {
	return nil, nil
}

func (c *exprResolutionCatalog) GetTableStats(_ *sql.Context, _ string, _ sql.Table) ([]sql.Statistic, error) {
	return nil, nil
}

func (c *exprResolutionCatalog) AnalyzeTable(_ *sql.Context, _ sql.Table, _ string) error { return nil }

func (c *exprResolutionCatalog) SetStats(_ *sql.Context, _ sql.Statistic) error { return nil }

func (c *exprResolutionCatalog) GetStats(_ *sql.Context, _ sql.StatQualifier, _ []string) (sql.Statistic, bool) {
	return nil, false
}

func (c *exprResolutionCatalog) DropStats(_ *sql.Context, _ sql.StatQualifier, _ []string) error {
	return nil
}

func (c *exprResolutionCatalog) DropDbStats(_ *sql.Context, _ string, _ bool) error { return nil }

func (c *exprResolutionCatalog) RowCount(_ *sql.Context, _ string, _ sql.Table) (uint64, error) {
	return 0, nil
}

func (c *exprResolutionCatalog) DataLength(_ *sql.Context, _ string, _ sql.Table) (uint64, error) {
	return 0, nil
}
