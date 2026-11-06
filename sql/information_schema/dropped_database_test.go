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

package information_schema_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// dropPoint selects the step at which a droppedDatabase starts reporting ErrDatabaseNotFound.
type dropPoint int32

const (
	// dropNever behaves like the underlying database, so that it can be populated.
	dropNever dropPoint = iota
	// dropAtEnumeration fails every database-level call, including listing table names.
	dropAtEnumeration
	// dropAtTableLookup lists table names, but fails to resolve any of them.
	dropAtTableLookup
	// dropAtTableCall resolves tables, but fails their index, check and foreign key calls.
	dropAtTableCall
)

// droppedDatabase simulates a database that another session drops while information_schema is visiting it.
type droppedDatabase struct {
	*memory.Database
	at dropPoint
}

var _ sql.ViewDatabase = droppedDatabase{}
var _ sql.TriggerDatabase = droppedDatabase{}
var _ sql.EventDatabase = droppedDatabase{}

func (d droppedDatabase) is(p dropPoint) bool { return d.at == p }

func (d droppedDatabase) gone() error { return sql.ErrDatabaseNotFound.New(d.Name()) }

func (d droppedDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	if d.is(dropAtEnumeration) {
		return nil, d.gone()
	}
	return d.Database.GetTableNames(ctx)
}

func (d droppedDatabase) GetTableInsensitive(ctx *sql.Context, name string) (sql.Table, bool, error) {
	switch {
	case d.is(dropAtEnumeration), d.is(dropAtTableLookup):
		return nil, false, d.gone()
	case d.is(dropAtTableCall):
		tbl, ok, err := d.Database.GetTableInsensitive(ctx, name)
		if err != nil || !ok {
			return tbl, ok, err
		}
		return droppedTable{tbl.(*memory.Table), d}, true, nil
	}
	return d.Database.GetTableInsensitive(ctx, name)
}

func (d droppedDatabase) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	if d.is(dropAtEnumeration) {
		return nil, d.gone()
	}
	return d.Database.AllViews(ctx)
}

func (d droppedDatabase) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	if d.is(dropAtEnumeration) {
		return nil, d.gone()
	}
	return d.Database.GetTriggers(ctx)
}

func (d droppedDatabase) GetEvents(ctx *sql.Context) ([]sql.EventDefinition, interface{}, error) {
	if d.is(dropAtEnumeration) {
		return nil, nil, d.gone()
	}
	return d.Database.GetEvents(ctx)
}

// droppedTable is a table whose per-table metadata calls fail because its database was dropped.
type droppedTable struct {
	*memory.Table
	db droppedDatabase
}

func (t droppedTable) GetIndexes(*sql.Context) ([]sql.Index, error) { return nil, t.db.gone() }

func (t droppedTable) GetChecks(*sql.Context) ([]sql.CheckDefinition, error) { return nil, t.db.gone() }

func (t droppedTable) GetDeclaredForeignKeys(*sql.Context) ([]sql.ForeignKeyConstraint, error) {
	return nil, t.db.gone()
}

func TestInformationSchemaToleratesDroppedDatabase(t *testing.T) {
	gones := map[string]dropPoint{
		"gone_enum":  dropAtEnumeration,
		"gone_table": dropAtTableLookup,
		"gone_call":  dropAtTableCall,
	}
	healthy := memory.NewDatabase("healthy")
	other := memory.NewDatabase("healthy_other")
	plain := map[string]*memory.Database{"healthy": healthy, "healthy_other": other}
	for name := range gones {
		plain[name] = memory.NewDatabase(name)
	}

	newQuery := func(pro *memory.DbProvider, e *sqle.Engine) func(db, q string) ([]sql.Row, error) {
		return func(db, q string) ([]sql.Row, error) {
			client := sql.Client{Address: "localhost", User: "root"}
			session := memory.NewSession(sql.NewBaseSessionWithClientServerWithVariables("address", client, 1, e.Variables()), pro)
			ctx := e.NewContext(context.Background(), sql.WithSession(session))
			ctx.SetCurrentDatabase(db)
			_, iter, _, err := e.Query(ctx, q)
			if err != nil {
				return nil, err
			}
			return sql.RowIterToRows(ctx, iter)
		}
	}

	// Populate the databases first: every one gets tables with an index, a check and a foreign key, so that the
	// per-table calls run for it.
	var all []sql.Database
	for _, db := range plain {
		all = append(all, db)
	}
	setupPro := memory.NewDBProvider(all...)
	setupEngine := sqle.NewDefault(setupPro)
	setupEngine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
	setup := newQuery(setupPro, setupEngine)
	for db := range plain {
		for _, q := range []string{
			"CREATE TABLE t (i INT PRIMARY KEY, j INT, UNIQUE KEY uj (j), CONSTRAINT ci CHECK (j > 0))",
			"CREATE TABLE child (i INT PRIMARY KEY, j INT, CONSTRAINT fk FOREIGN KEY (j) REFERENCES t (j))",
		} {
			_, err := setup(db, q)
			require.NoError(t, err, q)
		}
	}
	for _, q := range []string{
		"CREATE VIEW v AS SELECT i FROM t",
		"CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.j = 1",
	} {
		_, err := setup("healthy", q)
		require.NoError(t, err, q)
	}

	// A foreign key whose parent table lives in a database that is later dropped.
	_, err := setup("healthy_other", "CREATE TABLE xchild (i INT PRIMARY KEY, j INT, CONSTRAINT xfk FOREIGN KEY (j) REFERENCES gone_table.t (j))")
	require.NoError(t, err)

	// Then serve the same databases, with the doomed ones wrapped.
	dbs := []sql.Database{healthy, other}
	for name, at := range gones {
		d := droppedDatabase{plain[name], at}
		dbs = append(dbs, d)
	}
	pro := memory.NewDBProvider(dbs...)
	e := sqle.NewDefault(pro)
	e.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
	// information_schema reads through the engine's catalog, which holds the wrapped databases, while the memory session
	// commits table data through the plain provider; that is why the two differ.
	query := newQuery(setupPro, e)

	tests := []struct {
		table  string
		column string
		count  int64
	}{
		{"tables", "table_schema", 3},
		{"columns", "table_schema", 5},
		{"statistics", "table_schema", 4},
		{"views", "table_schema", 1},
		{"key_column_usage", "table_schema", 4},
		{"table_constraints", "table_schema", 5},
		{"check_constraints", "constraint_schema", 1},
		{"referential_constraints", "constraint_schema", 1},
		{"table_constraints_extensions", "constraint_schema", 4},
		{"triggers", "trigger_schema", 1},
		{"events", "event_schema", 0},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			rows, err := query("healthy", "SELECT COUNT(*) FROM information_schema."+tt.table+" WHERE "+tt.column+" = 'healthy'")
			require.NoError(t, err)
			require.Equal(t, []sql.Row{{tt.count}}, rows)
		})
	}

	t.Run("foreign key to dropped parent database", func(t *testing.T) {
		rows, err := query("healthy", "SELECT constraint_name, unique_constraint_name FROM information_schema.referential_constraints WHERE constraint_schema = 'healthy_other' AND constraint_name = 'xfk'")
		require.NoError(t, err)
		require.Equal(t, []sql.Row{{"xfk", nil}}, rows)
	})

	t.Run("direct access still fails", func(t *testing.T) {
		_, err := query("healthy", "SHOW TABLES FROM gone_enum")
		require.Error(t, err)
		require.True(t, sql.ErrDatabaseNotFound.Is(err), "unexpected error: %v", err)
	})
}
