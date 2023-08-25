// Copyright 2020-2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
)

// Describe is a node that describes its children.
type Describe struct {
	UnaryNode
}

var _ sql.Node = (*Describe)(nil)
var _ sql.CollationCoercible = (*Describe)(nil)

// NewDescribe creates a new Describe node.
func NewDescribe(child sql.Node) *Describe {
	return &Describe{UnaryNode{child}}
}

// Schema implements the Node interface.
func (d *Describe) Schema() sql.Schema {
	return sql.Schema{{
		Name: "name",
		Type: VarChar25000,
	}, {
		Name: "type",
		Type: VarChar25000,
	}}
}

func (d *Describe) IsReadOnly() bool {
	return true
}

// WithChildren implements the Node interface.
func (d *Describe) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewDescribe(children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *Describe) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return d.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Describe) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (d Describe) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Describe")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

// DescribeQuery returns the description of the query plan.
type DescribeQuery struct {
	UnaryNode
	Format string
}

var _ sql.Node = (*DescribeQuery)(nil)
var _ sql.CollationCoercible = (*DescribeQuery)(nil)

func (d *DescribeQuery) Resolved() bool {
	return d.Child.Resolved()
}

func (d *DescribeQuery) IsReadOnly() bool {
	return true
}

func (d *DescribeQuery) Children() []sql.Node {
	return nil
}

func (d *DescribeQuery) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(node), 0)
	}
	return d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DescribeQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return d.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DescribeQuery) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// DescribeSchema is the schema returned by a DescribeQuery node.
var DescribeSchema = sql.Schema{
	{Name: "plan", Type: VarChar25000},
}

// NewDescribeQuery creates a new DescribeQuery node.
func NewDescribeQuery(format string, child sql.Node) *DescribeQuery {
	return &DescribeQuery{UnaryNode{Child: child}, format}
}

// Schema implements the Node interface.
func (d *DescribeQuery) Schema() sql.Schema {
	return DescribeSchema
}

func (d *DescribeQuery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	if d.Format == "debug" {
		_ = pr.WriteChildren(sql.DebugString(d.Child))
	} else {
		_ = pr.WriteChildren(d.Child.String())
	}
	return pr.String()
}

func (d *DescribeQuery) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	_ = pr.WriteChildren(sql.DebugString(d.Child))
	return pr.String()
}

// Query returns the query node being described
func (d *DescribeQuery) Query() sql.Node {
	return d.Child
}

// WithQuery returns a copy of this node with the query node given
func (d *DescribeQuery) WithQuery(child sql.Node) sql.Node {
	return NewDescribeQuery(d.Format, child)
}
