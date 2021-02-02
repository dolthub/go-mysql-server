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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// Describe is a node that describes its children.
type Describe struct {
	UnaryNode
}

// NewDescribe creates a new Describe node.
func NewDescribe(child sql.Node) *Describe {
	return &Describe{UnaryNode{child}}
}

// Schema implements the Node interface.
func (d *Describe) Schema() sql.Schema {
	return sql.Schema{{
		Name: "name",
		Type: sql.LongText,
	}, {
		Name: "type",
		Type: sql.LongText,
	}}
}

// RowIter implements the Node interface.
func (d *Describe) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &describeIter{schema: d.Child.Schema()}, nil
}

// WithChildren implements the Node interface.
func (d *Describe) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewDescribe(children[0]), nil
}

func (d Describe) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Describe")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

type describeIter struct {
	schema sql.Schema
	i      int
}

func (i *describeIter) Next() (sql.Row, error) {
	if i.i >= len(i.schema) {
		return nil, io.EOF
	}

	f := i.schema[i.i]
	i.i++
	return sql.NewRow(f.Name, f.Type.String()), nil
}

func (i *describeIter) Close() error {
	return nil
}

// DescribeQuery returns the description of the query plan.
type DescribeQuery struct {
	UnaryNode
	Format string
}

// DescribeSchema is the schema returned by a DescribeQuery node.
var DescribeSchema = sql.Schema{
	{Name: "plan", Type: sql.LongText},
}

// NewDescribeQuery creates a new DescribeQuery node.
func NewDescribeQuery(format string, child sql.Node) *DescribeQuery {
	return &DescribeQuery{UnaryNode{Child: child}, format}
}

// Schema implements the Node interface.
func (d *DescribeQuery) Schema() sql.Schema {
	return DescribeSchema
}

// RowIter implements the Node interface.
func (d *DescribeQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	for _, l := range strings.Split(d.Child.String(), "\n") {
		if strings.TrimSpace(l) != "" {
			rows = append(rows, sql.NewRow(l))
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

func (d *DescribeQuery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	_ = pr.WriteChildren(d.Child.String())
	return pr.String()
}

func (d *DescribeQuery) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	_ = pr.WriteChildren(sql.DebugString(d.Child))
	return pr.String()
}

// WithChildren implements the Node interface.
func (d *DescribeQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewDescribeQuery(d.Format, children[0]), nil
}
