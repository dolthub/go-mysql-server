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

/// DBDDDL nodes have a reference to an inmemory database
type dbddlNode struct {
	db sql.Database
}

// Resolved implements the Resolvable interface.
func (c *dbddlNode) Resolved() bool {
	_, ok := c.db.(sql.Database) // TODO: Wtf am i doing here
	return ok
}

// Database implements the sql.Databaser interface.
func (c *dbddlNode) Database() sql.Database {
	return c.db
}

// Schema implements the Node interface.
func (*dbddlNode) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (*dbddlNode) Children() []sql.Node { return nil }

type CreateDB struct {
	dbddlNode
	IfExists bool
	Collate  string
	Charset  string
}

func (c CreateDB) Resolved() bool {
	return c.dbddlNode.Resolved()
}

func (c CreateDB) String() string {
	ifExists := ""
	if c.IfExists {
		ifExists = " if exists"
	}
	return fmt.Sprintf("%s database%s %v", sqlparser.CreateStr, ifExists, c.db.Name())
}

func (c CreateDB) Schema() sql.Schema {
	return nil
}

func (c CreateDB) Children() []sql.Node {
	return nil
}

func (c CreateDB) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (c CreateDB) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

func NewCreateDatabase(dbName string, ifExists bool, collate string, charset string) *CreateDB {
	db := memory.NewDatabase(dbName)

	return &CreateDB{
		dbddlNode: dbddlNode{db: db},
		IfExists: ifExists,
		Collate: collate,
		Charset: charset,
	}
}