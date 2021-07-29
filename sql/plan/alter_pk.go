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

import "github.com/dolthub/go-mysql-server/sql"

type PKAction byte

const (
	PrimaryKeyAction_Create PKAction = iota
	PrimaryKeyAction_Drop
)

type AlterPK struct {
	Action PKAction

	Table sql.Node

	Columns []sql.IndexColumn
}

func NewAlterCreatePk(table sql.Node, columns []sql.IndexColumn) *AlterPK {
	return &AlterPK{
		Action: PrimaryKeyAction_Create,
		Table: table,
		Columns: columns,
	}
}

func NewAlterDropPk(table sql.Node) *AlterPK {
	return &AlterPK{
		Action: PrimaryKeyAction_Create,
		Table: table,
	}
}

func (a AlterPK) Resolved() bool {
	return a.Table.Resolved()
}

func (a AlterPK) String() string {
	return "TODO" // TODO:
}

func (a AlterPK) Schema() sql.Schema {
	return nil
}

func (a AlterPK) Children() []sql.Node {
	return []sql.Node{a.Table}
}

func (a AlterPK) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("implement me")
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
