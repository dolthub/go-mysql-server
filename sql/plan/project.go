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
	"strings"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	// Expression projected.
	Projections []sql.Expression
}

// NewProject creates a new projection.
func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Projections: expressions,
	}
}

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Projections))
	for i, e := range p.Projections {
		s[i] = expression.ExpressionToColumn(e)
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expression.ExpressionsResolved(p.Projections...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Project", opentracing.Tag{
		Key:   "projections",
		Value: len(p.Projections),
	})

	i, err := p.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, &iter{
		p:         p,
		childIter: i,
		ctx:       ctx,
		row:       row,
	}), nil
}

func (p *Project) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}
	_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *Project) DebugString() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = sql.DebugString(expr)
	}
	_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// WithChildren implements the Node interface.
func (p *Project) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	return NewProject(p.Projections, children[0]), nil
}

// WithExpressions implements the Expressioner interface.
func (p *Project) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(p.Projections) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), len(p.Projections))
	}

	return NewProject(exprs, p.Child), nil
}

type iter struct {
	p         *Project
	childIter sql.RowIter
	row       sql.Row
	ctx       *sql.Context
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}

	return ProjectRow(i.ctx, i.p.Projections, childRow)
}

func (i *iter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

// ProjectRow evaluates a set of projections.
func ProjectRow(
	s *sql.Context,
	projections []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var err error
	var secondPass []int
	var fields sql.Row
	for i, expr := range projections {
		// Default values that are expressions may reference other fields, thus they must evaluate after all other exprs.
		// Also default expressions may not refer to other columns that come after them if they also have a default expr.
		// This ensures that all columns referenced by expressions will have already been evaluated.
		// Since literals do not reference other columns, they're evaluated on the first pass.
		if defaultVal, ok := expr.(*sql.ColumnDefaultValue); ok {
			if !defaultVal.IsLiteral() {
				fields = append(fields, nil)
				secondPass = append(secondPass, i)
				continue
			}
		}
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	for _, index := range secondPass {
		fields[index], err = projections[index].Eval(s, fields)
		if err != nil {
			return nil, err
		}
	}
	return sql.NewRow(fields...), nil
}
