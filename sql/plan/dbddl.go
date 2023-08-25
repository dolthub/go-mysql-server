// Copyright 2021 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CreateDB creates an in memory database that lasts the length of the process only.
type CreateDB struct {
	Catalog     sql.Catalog
	DbName      string
	IfNotExists bool
	Collation   sql.CollationID
}

var _ sql.Node = (*CreateDB)(nil)
var _ sql.CollationCoercible = (*CreateDB)(nil)

func (c *CreateDB) Resolved() bool {
	return true
}

func (c *CreateDB) IsReadOnly() bool {
	return false
}

func (c *CreateDB) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " if not exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.CreateStr, ifNotExists, c.DbName)
}

func (c *CreateDB) Schema() sql.Schema {
	return types.OkResultSchema
}

func (c *CreateDB) Children() []sql.Node {
	return nil
}

func (c *CreateDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateDB) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Create))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CreateDB) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Database returns the name of the database that will be used.
func (c *CreateDB) Database() string {
	return c.DbName
}

func NewCreateDatabase(dbName string, ifNotExists bool, collation sql.CollationID) *CreateDB {
	return &CreateDB{
		DbName:      dbName,
		IfNotExists: ifNotExists,
		Collation:   collation,
	}
}

// DropDB removes a databases from the Catalog and updates the active database if it gets removed itself.
type DropDB struct {
	Catalog  sql.Catalog
	DbName   string
	IfExists bool
	// EventScheduler is used to notify EventSchedulerStatus of database deletion,
	// so the events of this database in the scheduler will be removed.
	EventScheduler sql.EventScheduler
}

var _ sql.Node = (*DropDB)(nil)
var _ sql.CollationCoercible = (*DropDB)(nil)
var _ sql.EventSchedulerStatement = (*DropDB)(nil)

func (d *DropDB) Resolved() bool {
	return true
}

func (d *DropDB) IsReadOnly() bool {
	return false
}

func (d *DropDB) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = " if exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.DropStr, ifExists, d.DbName)
}

func (d *DropDB) Schema() sql.Schema {
	return types.OkResultSchema
}

func (d *DropDB) Children() []sql.Node {
	return nil
}

func (d *DropDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// WithEventScheduler is used to drop all events from EventSchedulerStatus for DROP DATABASE.
func (d *DropDB) WithEventScheduler(scheduler sql.EventScheduler) sql.Node {
	na := *d
	na.EventScheduler = scheduler
	return &na
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropDB) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Drop))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DropDB) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func NewDropDatabase(dbName string, ifExists bool) *DropDB {
	return &DropDB{
		DbName:   dbName,
		IfExists: ifExists,
	}
}

// AlterDB alters a database from the Catalog.
type AlterDB struct {
	Catalog   sql.Catalog
	dbName    string
	Collation sql.CollationID
}

var _ sql.Node = (*AlterDB)(nil)
var _ sql.CollationCoercible = (*AlterDB)(nil)

// Resolved implements the interface sql.Node.
func (c *AlterDB) Resolved() bool {
	return true
}

func (c *AlterDB) IsReadOnly() bool {
	return false
}

// String implements the interface sql.Node.
func (c *AlterDB) String() string {
	var dbName string
	if len(c.dbName) > 0 {
		dbName = fmt.Sprintf(" %s", c.dbName)
	}
	return fmt.Sprintf("%s database%s collate %s", sqlparser.AlterStr, dbName, c.Collation.Name())
}

// Schema implements the interface sql.Node.
func (c *AlterDB) Schema() sql.Schema {
	return types.OkResultSchema
}

// Children implements the interface sql.Node.
func (c *AlterDB) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (c *AlterDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (c *AlterDB) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(c.Database(ctx), "", "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*AlterDB) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Database returns the name of the database that will be used.
func (c *AlterDB) Database(ctx *sql.Context) string {
	if len(c.dbName) == 0 {
		return ctx.GetCurrentDatabase()
	}
	return c.dbName
}

// NewAlterDatabase returns a new AlterDB.
func NewAlterDatabase(dbName string, collation sql.CollationID) *AlterDB {
	return &AlterDB{
		dbName:    dbName,
		Collation: collation,
	}
}

// GetDatabaseCollation returns a database's collation. Also handles when a database does not explicitly support collations.
func GetDatabaseCollation(ctx *sql.Context, db sql.Database) sql.CollationID {
	collatedDb, ok := db.(sql.CollatedDatabase)
	if !ok {
		return sql.Collation_Default
	}
	collation := collatedDb.GetCollation(ctx)
	if collation == sql.Collation_Unspecified {
		return sql.Collation_Default
	}
	return collation
}
