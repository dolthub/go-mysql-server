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

// DropConstraint is a temporary node to handle dropping a named constraint on a table. The type of the constraint is
// not known, and is determined during analysis.
type DropConstraint struct {
	UnaryNode
	Name string
}

func (d *DropConstraint) String() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("DropConstraint(%s)", d.Name)
	_ = tp.WriteChildren(d.UnaryNode.Child.String())
	return tp.String()
}

func (d *DropConstraint) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("DropConstraint is a placeholder node, but RowIter was called")
}

func (d DropConstraint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	nd := &d
	nd.UnaryNode = UnaryNode{children[0]}
	return nd, nil
}

// NewDropConstraint returns a new DropConstraint node
func NewDropConstraint(table *UnresolvedTable, name string) *DropConstraint {
	return &DropConstraint{
		UnaryNode: UnaryNode{table},
		Name:      name,
	}
}
