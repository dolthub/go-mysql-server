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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	Projections []sql.Expression
	CanDefer    bool
}

var _ sql.Expressioner = (*Project)(nil)
var _ sql.Node = (*Project)(nil)
var _ sql.Projector = (*Project)(nil)
var _ sql.CollationCoercible = (*Project)(nil)

// NewProject creates a new projection.
func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Projections: expressions,
	}
}

// findDefault finds the matching GetField in the node's Schema and fills the default value in the column.
func findDefault(node sql.Node, gf *expression.GetField) *sql.ColumnDefaultValue {
	colSet := sql.NewColSet()
	switch n := node.(type) {
	case TableIdNode:
		colSet = n.Columns()
	case *GroupBy:
		return findDefault(n.Child, gf)
	case *HashLookup:
		return findDefault(n.Child, gf)
	case *Filter:
		return findDefault(n.Child, gf)
	case *JoinNode:
		if defVal := findDefault(n.Left(), gf); defVal != nil {
			return defVal
		}
		if defVal := findDefault(n.Right(), gf); defVal != nil {
			return defVal
		}
		return nil
	default:
		return nil
	}

	if !colSet.Contains(gf.Id()) {
		return nil
	}
	firstColId, ok := colSet.Next(1)
	if !ok {
		return nil
	}

	sch := node.Schema()
	idx := gf.Id() - firstColId
	if idx < 0 || int(idx) >= len(sch) {
		return nil
	}
	return sch[idx].Default
}

func unwrapGetField(expr sql.Expression) *expression.GetField {
	switch e := expr.(type) {
	case *expression.GetField:
		return e
	case *expression.Alias:
		return unwrapGetField(e.Child)
	default:
		return nil
	}
}

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Projections))
	for i, expr := range p.Projections {
		s[i] = transform.ExpressionToColumn(expr, AliasSubqueryString(expr))
		if gf := unwrapGetField(expr); gf != nil {
			s[i].Default = findDefault(p.Child, gf)
		}
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expression.ExpressionsResolved(p.Projections...)
}

func (p *Project) IsReadOnly() bool {
	return p.Child.IsReadOnly()
}

// Describe implements the sql.Describable interface.
func (p *Project) Describe(options sql.DescribeOptions) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Project")
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = sql.Describe(expr, options)
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, sql.Describe(p.Child, options))
	return pr.String()
}

// String implements the fmt.Stringer interface.
func (p *Project) String() string {
	return p.Describe(sql.DescribeOptions{
		Analyze:   false,
		Estimates: false,
		Debug:     false,
	})
}

// DebugString implements the sql.DebugStringer interface.
func (p *Project) DebugString() string {
	return p.Describe(sql.DescribeOptions{
		Analyze:   false,
		Estimates: false,
		Debug:     true,
	})
}

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// ProjectedExprs implements sql.Projector
func (p *Project) ProjectedExprs() []sql.Expression {
	return p.Projections
}

// WithChildren implements the Node interface.
func (p *Project) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	np := *p
	np.Child = children[0]
	return &np, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *Project) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return p.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (p *Project) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, p.Child)
}

// WithExpressions implements the Expressioner interface.
func (p *Project) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(p.Projections) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), len(p.Projections))
	}
	np := *p
	np.Projections = exprs
	return &np, nil
}

func (p *Project) WithCanDefer(canDefer bool) *Project {
	np := *p
	np.CanDefer = canDefer
	return &np
}
