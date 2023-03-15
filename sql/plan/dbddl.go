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
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CreateDB creates an in memory database that lasts the length of the process only.
type CreateDB struct {
	Catalog     sql.Catalog
	dbName      string
	IfNotExists bool
	Collation   sql.CollationID
}

func (c *CreateDB) Resolved() bool {
	return true
}

func (c *CreateDB) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " if not exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.CreateStr, ifNotExists, c.dbName)
}

func (c *CreateDB) Schema() sql.Schema {
	return types.OkResultSchema
}

func (c *CreateDB) Children() []sql.Node {
	return nil
}

func (c *CreateDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := c.Catalog.HasDB(ctx, c.dbName)
	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}

	if exists {
		if c.IfNotExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbCreateExists,
				Message: fmt.Sprintf("Can't create database %s; database exists ", c.dbName),
			})

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrDatabaseExists.New(c.dbName)
		}
	}

	collation := c.Collation
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	err := c.Catalog.CreateDatabase(ctx, c.dbName, collation)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(rows...), nil
}

func (c *CreateDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateDB) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Create))
}

// Database returns the name of the database that will be used.
func (c *CreateDB) Database() string {
	return c.dbName
}

func NewCreateDatabase(dbName string, ifNotExists bool, collation sql.CollationID) *CreateDB {
	return &CreateDB{
		dbName:      dbName,
		IfNotExists: ifNotExists,
		Collation:   collation,
	}
}

// DropDB removes a databases from the Catalog and updates the active database if it gets removed itself.
type DropDB struct {
	Catalog  sql.Catalog
	dbName   string
	IfExists bool
}

func (d *DropDB) Resolved() bool {
	return true
}

func (d *DropDB) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = " if exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.DropStr, ifExists, d.dbName)
}

func (d *DropDB) Schema() sql.Schema {
	return types.OkResultSchema
}

func (d *DropDB) Children() []sql.Node {
	return nil
}

func (d *DropDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := d.Catalog.HasDB(ctx, d.dbName)
	if !exists {
		if d.IfExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbDropExists,
				Message: fmt.Sprintf("Can't drop database %s; database doesn't exist ", d.dbName),
			})

			rows := []sql.Row{{types.OkResult{RowsAffected: 0}}}

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrDatabaseNotFound.New(d.dbName)
		}
	}

	err := d.Catalog.RemoveDatabase(ctx, d.dbName)
	if err != nil {
		return nil, err
	}

	// Unsets the current database. Database name is case-insensitive.
	if strings.ToLower(ctx.GetCurrentDatabase()) == strings.ToLower(d.dbName) {
		ctx.SetCurrentDatabase("")
		ctx.Session.SetTransactionDatabase("")
	}

	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}

	return sql.RowsToRowIter(rows...), nil
}

func (d *DropDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropDB) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Drop))
}

func NewDropDatabase(dbName string, ifExists bool) *DropDB {
	return &DropDB{
		dbName:   dbName,
		IfExists: ifExists,
	}
}

// AlterDB alters a database from the Catalog.
type AlterDB struct {
	Catalog   sql.Catalog
	dbName    string
	Collation sql.CollationID
}

// Resolved implements the interface sql.Node.
func (c *AlterDB) Resolved() bool {
	return true
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

// RowIter implements the interface sql.Node.
func (c *AlterDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	dbName := c.Database(ctx)

	if !c.Catalog.HasDB(ctx, dbName) {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}
	db, err := c.Catalog.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}
	collatedDb, ok := db.(sql.CollatedDatabase)
	if !ok {
		return nil, sql.ErrDatabaseCollationsNotSupported.New(dbName)
	}

	collation := c.Collation
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	if err = collatedDb.SetCollation(ctx, collation); err != nil {
		return nil, err
	}

	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}
	return sql.RowsToRowIter(rows...), nil
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
