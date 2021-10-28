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

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

// CreateDB creates an in memory database that lasts the length of the process only.
type CreateDB struct {
	Catalog     sql.Catalog
	dbName      string
	IfNotExists bool
}

func (c CreateDB) Resolved() bool {
	return true
}

func (c CreateDB) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " if not exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.CreateStr, ifNotExists, c.dbName)
}

func (c CreateDB) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (c CreateDB) Children() []sql.Node {
	return nil
}

func (c CreateDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := c.Catalog.HasDB(c.dbName)
	rows := []sql.Row{{sql.OkResult{RowsAffected: 1}}}

	if exists {
		if c.IfNotExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbCreateExists,
				Message: fmt.Sprintf("Can't create database %s; database exists ", c.dbName),
			})

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrCannotCreateDatabaseExists.New(c.dbName)
		}
	}

	err := c.Catalog.CreateDatabase(ctx, c.dbName)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(rows...), nil
}

func (c CreateDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

func NewCreateDatabase(dbName string, ifNotExists bool) *CreateDB {
	return &CreateDB{
		dbName:      dbName,
		IfNotExists: ifNotExists,
	}
}

// DropDB removes a databases from the Catalog and updates the active database if it gets removed itself.
type DropDB struct {
	Catalog  sql.Catalog
	dbName   string
	IfExists bool
}

func (d DropDB) Resolved() bool {
	return true
}

func (d DropDB) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = " if exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.DropStr, ifExists, d.dbName)
}

func (d DropDB) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (d DropDB) Children() []sql.Node {
	return nil
}

func (d DropDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := d.Catalog.HasDB(d.dbName)
	if !exists {
		if d.IfExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbDropExists,
				Message: fmt.Sprintf("Can't drop database %s; database doesn't exist ", d.dbName),
			})

			rows := []sql.Row{{sql.OkResult{RowsAffected: 0}}}

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrCannotDropDatabaseDoesntExist.New(d.dbName)
		}
	}

	err := d.Catalog.RemoveDatabase(ctx, d.dbName)
	if err != nil {
		return nil, err
	}

	// Unsets the current database
	if ctx.GetCurrentDatabase() == d.dbName {
		ctx.SetCurrentDatabase("")
	}

	rows := []sql.Row{{sql.OkResult{RowsAffected: 1}}}

	return sql.RowsToRowIter(rows...), nil
}

func (d DropDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

func NewDropDatabase(dbName string, ifExists bool) *DropDB {
	return &DropDB{
		dbName:   dbName,
		IfExists: ifExists,
	}
}
