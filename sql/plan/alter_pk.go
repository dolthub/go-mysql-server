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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

type PKAction byte

const (
	PrimaryKeyAction_Create PKAction = iota
	PrimaryKeyAction_Drop
)

// ErrNotPrimaryKeyAlterable is return when a table cannot be determines to be primary key alterable
var ErrNotPrimaryKeyAlterable = errors.NewKind("error: table is not primary key alterable")

type AlterPK struct {
	Action  PKAction
	Table   sql.Node
	Columns []sql.IndexColumn
}

func NewAlterCreatePk(table sql.Node, columns []sql.IndexColumn) *AlterPK {
	return &AlterPK{
		Action:  PrimaryKeyAction_Create,
		Table:   table,
		Columns: columns,
	}
}

func NewAlterDropPk(table sql.Node) *AlterPK {
	return &AlterPK{
		Action: PrimaryKeyAction_Drop,
		Table:  table,
	}
}

func (a AlterPK) Resolved() bool {
	return a.Table.Resolved()
}

func (a AlterPK) String() string {
	action := "add"
	if a.Action == PrimaryKeyAction_Drop {
		action = "drop"
	}

	return fmt.Sprintf("alter table %s %s primary key", a.Table.String(), action)
}

func (a AlterPK) Schema() sql.Schema {
	return nil
}

func (a AlterPK) Children() []sql.Node {
	return []sql.Node{a.Table}
}

func getPrimaryKeyAlterable(node sql.Node) (sql.PrimaryKeyAlterableTable, error) {
	switch node := node.(type) {
	case sql.PrimaryKeyAlterableTable:
		return node, nil
	case *ResolvedTable:
		return getPrimaryKeyAlterableTable(node.Table)
	case sql.TableWrapper:
		return getPrimaryKeyAlterableTable(node.Underlying())
	default:
		return nil, ErrNotPrimaryKeyAlterable.New()
	}
}

func getPrimaryKeyAlterableTable(t sql.Table) (sql.PrimaryKeyAlterableTable, error) {
	switch t := t.(type) {
	case sql.PrimaryKeyAlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getPrimaryKeyAlterableTable(t.Underlying())
	default:
		return nil, ErrNotPrimaryKeyAlterable.New()
	}
}

func (a AlterPK) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	pkAlterable, err := getPrimaryKeyAlterable(a.Table)
	if err != nil {
		return nil, err
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		if hasPrimaryKeys(pkAlterable) {
			return sql.RowsToRowIter(), sql.ErrMultiplePrimaryKeysDefined.New()
		}

		for _, c := range a.Columns {
			if !pkAlterable.Schema().Contains(c.Name, pkAlterable.Name()) {
				return sql.RowsToRowIter(), sql.ErrKeyColumnDoesNotExist.New(c.Name)
			}
		}

		err = pkAlterable.CreatePrimaryKey(ctx, a.Columns)
	case PrimaryKeyAction_Drop:
		err = pkAlterable.DropPrimaryKey(ctx)
	}

	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func hasPrimaryKeys(table sql.Table) bool {
	for _, c := range table.Schema() {
		if c.PrimaryKey {
			return true
		}
	}

	return false
}

func (a AlterPK) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		return NewAlterCreatePk(children[0], a.Columns), nil
	case PrimaryKeyAction_Drop:
		return NewAlterDropPk(children[0]), nil
	default:
		return nil, ErrIndexActionNotImplemented.New(a.Action)
	}
}
