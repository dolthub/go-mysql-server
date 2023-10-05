// Copyright 2023 Dolthub, Inc.
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

// VirtualColumnTable is a sql.TableNode that combines a ResolvedTable with a Project, the latter of which is used 
// to add the values of virtual columns to the table.
type VirtualColumnTable struct {
	*ResolvedTable
	Projections []sql.Expression
}

var _ sql.TableNode = (*VirtualColumnTable)(nil)

// NewVirtualColumnTable creates a new VirtualColumnTable.
func NewVirtualColumnTable(table *ResolvedTable, projections []sql.Expression) *VirtualColumnTable {
	return &VirtualColumnTable{ResolvedTable: table, Projections: projections}
}

func (v *VirtualColumnTable) Children() []sql.Node {
	return []sql.Node{v.ResolvedTable}
}

func (v VirtualColumnTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 1)
	}
	v.ResolvedTable = children[0].(*ResolvedTable)
	return &v, nil
}

// WithExpressions implements the Expressioner interface.
func (v *VirtualColumnTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(v.Projections) {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(exprs), len(v.Projections))
	}

	return NewVirtualColumnTable(v.ResolvedTable, exprs), nil
}

func (v *VirtualColumnTable) Expressions() []sql.Expression {
	return v.Projections
}


func (v VirtualColumnTable) WithComment(s string) sql.Node {
	v.ResolvedTable = v.ResolvedTable.WithComment(s).(*ResolvedTable)
	return &v
}

func (v *VirtualColumnTable) Debug() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("VirtualColumnTable")
	var exprs = make([]string, len(v.Projections))
	for i, expr := range v.Projections {
		exprs[i] = expr.String()
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, v.ResolvedTable.String())

	return pr.String()
}

func (v *VirtualColumnTable) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("VirtualColumnTable")
	var exprs = make([]string, len(v.Projections))
	for i, expr := range v.Projections {
		exprs[i] = sql.DebugString(expr)
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, sql.DebugString(v.ResolvedTable))

	return pr.String()
}
