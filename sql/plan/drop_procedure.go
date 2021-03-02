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

	"github.com/dolthub/go-mysql-server/sql"
)

type DropProcedure struct {
	db            sql.Database
	IfExists      bool
	ProcedureName string
}

var _ sql.Databaser = (*DropProcedure)(nil)
var _ sql.Node = (*DropProcedure)(nil)

// NewDropProcedure creates a new *DropProcedure node.
func NewDropProcedure(db sql.Database, procedureName string, ifExists bool) *DropProcedure {
	return &DropProcedure{
		db:            db,
		IfExists:      ifExists,
		ProcedureName: strings.ToLower(procedureName),
	}
}

// Resolved implements the sql.Node interface.
func (d *DropProcedure) Resolved() bool {
	_, ok := d.db.(sql.UnresolvedDatabase)
	return !ok
}

// String implements the sql.Node interface.
func (d *DropProcedure) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = "IF EXISTS "
	}
	return fmt.Sprintf("DROP PROCEDURE %s%s", ifExists, d.ProcedureName)
}

// Schema implements the sql.Node interface.
func (d *DropProcedure) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (d *DropProcedure) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface.
func (d *DropProcedure) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	procDb, ok := d.db.(sql.StoredProcedureDatabase)
	if !ok {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrStoredProceduresNotSupported.New(d.ProcedureName)
		}
	}
	err := procDb.DropStoredProcedure(ctx, d.ProcedureName)
	if d.IfExists && sql.ErrStoredProcedureDoesNotExist.Is(err) {
		return sql.RowsToRowIter(), nil
	} else if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the sql.Node interface.
func (d *DropProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// Database implements the sql.Databaser interface.
func (d *DropProcedure) Database() sql.Database {
	return d.db
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropProcedure) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}
