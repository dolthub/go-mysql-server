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
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

type CreateDB struct {
	Catalog     *sql.Catalog
	dbName      string
	IfNotExists bool
	Collate     string
	Charset     string
}

func (c CreateDB) Resolved() bool {
	return true
}

func (c CreateDB) String() string {
	ifExists := ""
	if c.IfNotExists {
		ifExists = " if exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.CreateStr, ifExists, c.dbName)
}

func (c CreateDB) Schema() sql.Schema {
	return nil
}

func (c CreateDB) Children() []sql.Node {
	return nil
}

func (c CreateDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := c.Catalog.HasDB(c.dbName)
	if c.IfNotExists && exists {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Note",
			Code:    1007,
			Message: fmt.Sprintf("Can't create database %s; database exists ", c.dbName),
		})

		return sql.RowsToRowIter(), nil
	} else if exists {
		return nil, sql.ErrDatabaseExists.New(c.dbName)
	}

	db := memory.NewDatabase(c.dbName)
	c.Catalog.AddDatabase(db)

	return sql.RowsToRowIter(), nil
}

func (c CreateDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

func NewCreateDatabase(dbName string, ifNotExists bool, collate string, charset string) *CreateDB {
	return &CreateDB{
		dbName:      dbName,
		IfNotExists: ifNotExists,
		Collate:     collate,
		Charset:     charset,
	}
}

type DropDB struct {
	Catalog *sql.Catalog
	dbName	string
	IfExists bool
	Collate  string
	Charset  string
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
	return nil
}

func (d DropDB) Children() []sql.Node {
	return nil
}

func (d DropDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	exists := d.Catalog.HasDB(d.dbName)
	if d.IfExists && !exists {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Note",
			Code:    1007,
			Message: fmt.Sprintf("Can't drop database %s; database doesn't exist ", d.dbName),
		})

		return sql.RowsToRowIter(), nil
	} else if !exists {
		return nil, sql.ErrDatabaseDoesntExists.New(d.dbName)
	}

	d.Catalog.DropDatabase(d.dbName)

	// Unsets the current database
	if ctx.GetCurrentDatabase() == d.dbName {
		ctx.SetCurrentDatabase("")
	}

	return sql.RowsToRowIter(), nil
}

func (d DropDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

func NewDropDatabase(dbName string, ifExists bool, collate string, charset string) *DropDB {
	return &DropDB{
		dbName: dbName,
		IfExists: ifExists,
		Collate: collate,
		Charset: charset,
	}
}
