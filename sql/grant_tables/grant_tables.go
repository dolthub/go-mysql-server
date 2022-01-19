// Copyright 2022 Dolthub, Inc.
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

package grant_tables

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// GrantTables are the collection of tables that are used with any user or privilege-related operations.
// https://dev.mysql.com/doc/refman/8.0/en/grant-tables.html
type GrantTables struct {
	user *grantTable
	//TODO: add the rest of these tables
	//db               *grantTable
	//global_grants    *grantTable
	//tables_priv      *grantTable
	//columns_priv     *grantTable
	//procs_priv       *grantTable
	//proxies_priv     *grantTable
	//default_roles    *grantTable
	//role_edges       *grantTable
	//password_history *grantTable
}

var _ sql.Database = (*GrantTables)(nil)

// CreateEmptyGrantTables returns a collection of Grant Tables that do not contain any data.
func CreateEmptyGrantTables() *GrantTables {
	grantTables := &GrantTables{
		user: newGrantTable(userTblName, userTblSchema, UserPrimaryKey{}),
	}
	addDefaultRootUser(grantTables.user)
	return grantTables
}

// Name implements the interface sql.Database.
func (g *GrantTables) Name() string {
	return "mysql"
}

// GetTableInsensitive implements the interface sql.Database.
func (g *GrantTables) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case "user":
		return g.user, true, nil
	default:
		return nil, false, nil
	}
}

// GetTableNames implements the interface sql.Database.
func (g *GrantTables) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{"user"}, nil
}

// Persist passes along all changes to the integrator.
func (g *GrantTables) Persist(ctx *sql.Context) error {
	//TODO: add the UserPersist interface, using this as a stand-in so I won't forget to put it where it needs to go
	return nil
}

// UserTable returns the user table.
func (g *GrantTables) UserTable() *grantTable {
	return g.user
}

// columnTemplate takes in a column as a template, and returns a new column with a different name based on the given
// template.
func columnTemplate(name string, source string, isPk bool, template *sql.Column) *sql.Column {
	newCol := *template
	if newCol.Default != nil {
		newCol.Default = &(*newCol.Default)
	}
	newCol.Name = name
	newCol.Source = source
	newCol.PrimaryKey = isPk
	return &newCol
}

// mustDefault enforces that no error occurred when constructing the column default value.
func mustDefault(expr sql.Expression, outType sql.Type, representsLiteral bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	colDef, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return colDef
}

type dummyPartition struct{}

var _ sql.Partition = dummyPartition{}

// Key implements the interface sql.Partition.
func (d dummyPartition) Key() []byte {
	return nil
}
