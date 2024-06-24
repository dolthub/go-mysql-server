// Copyright 2024 Dolthub, Inc.
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

package performance_schema

import "github.com/dolthub/go-mysql-server/sql"

const (
	PerformanceSchemaDatabaseName = "performance_schema"
)

type performanceSchemaDatabase struct {
	name   string
	tables map[string]sql.Table
}

var _ sql.Database = (*performanceSchemaDatabase)(nil)

func NewPerformanceSchemaDatabase() sql.Database {
	return &performanceSchemaDatabase{
		name:   PerformanceSchemaDatabaseName,
		tables: map[string]sql.Table{
			// TODO: Add global_variables table
			// TODO: Add user_variables_by_thread table
		},
	}
}

// Name implements the sql.Database interface.
func (db performanceSchemaDatabase) Name() string {
	return db.name
}

// GetTableInsensitive implements the sql.Database interface.
func (db performanceSchemaDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, db.tables)
	return tbl, ok, nil
}

// GetTableNames implements the sql.Database interface.
func (db performanceSchemaDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(db.tables))
	for k := range db.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}
