// Copyright 2022 Dolthub, Inc.
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

// Into is a node to wrap the top-level node in a query plan so that any result will set user-defined or others
// variables given
type Into struct {
	UnaryNode
	IntoVars []sql.Expression
}

var _ sql.Node = (*Into)(nil)
var _ sql.CollationCoercible = (*Into)(nil)

func NewInto(child sql.Node, variables []sql.Expression) *Into {
	return &Into{
		UnaryNode: UnaryNode{child},
		IntoVars:  variables,
	}
}

// Schema implements the Node interface.
func (i *Into) Schema() sql.Schema {
	// SELECT INTO does not return results directly (only through SQL vars or files),
	// so it's result schema is always empty.
	return nil
}

func (i *Into) IsReadOnly() bool {
	return i.Child.IsReadOnly()
}

func (i *Into) String() string {
	p := sql.NewTreePrinter()
	var vars = make([]string, len(i.IntoVars))
	for j, v := range i.IntoVars {
		vars[j] = fmt.Sprintf(v.String())
	}
	_ = p.WriteNode("Into(%s)", strings.Join(vars, ", "))
	_ = p.WriteChildren(i.Child.String())
	return p.String()
}

func (i *Into) DebugString() string {
	p := sql.NewTreePrinter()
	var vars = make([]string, len(i.IntoVars))
	for j, v := range i.IntoVars {
		vars[j] = sql.DebugString(v)
	}
	_ = p.WriteNode("Into(%s)", strings.Join(vars, ", "))
	_ = p.WriteChildren(sql.DebugString(i.Child))
	return p.String()
}

func (i *Into) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}

	return NewInto(children[0], i.IntoVars), nil
}

// CheckPrivileges implements the interface sql.Node.
func (i *Into) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return i.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (i *Into) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, i.Child)
}

// WithExpressions implements the sql.Expressioner interface.
func (i *Into) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(i.IntoVars) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(exprs), len(i.IntoVars))
	}

	return NewInto(i.Child, exprs), nil
}

// Expressions implements the sql.Expressioner interface.
func (i *Into) Expressions() []sql.Expression {
	return i.IntoVars
}
