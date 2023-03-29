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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	// Expression projected.
	Projections []sql.Expression
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

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Projections))
	for i, e := range p.Projections {
		s[i] = transform.ExpressionToColumn(e)
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
	span, ctx := ctx.Span("plan.Project", trace.WithAttributes(
		attribute.Int("projections", len(p.Projections)),
	))

	i, err := p.Child.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &projectIter{
		p:         p.Projections,
		childIter: i,
	}), nil
}

func (p *Project) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Project")
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, p.Child.String())
	return pr.String()
}

func (p *Project) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Project")
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = sql.DebugString(expr)
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, sql.DebugString(p.Child))

	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// ProjectedExprs implements sql.Projector
func (p *Project) ProjectedExprs() []sql.Expression {
	return p.Projections
}

// WithProjectedExprs implements sql.Projector
func (p *Project) WithProjectedExprs(exprs ...sql.Expression) (sql.Projector, error) {
	node, err := p.WithExpressions(exprs...)
	return node.(sql.Projector), err
}

// WithChildren implements the Node interface.
func (p *Project) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	return NewProject(p.Projections, children[0]), nil
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

	return NewProject(exprs, p.Child), nil
}

type projectIter struct {
	p         []sql.Expression
	childIter sql.RowIter
}

func (i *projectIter) Next(ctx *sql.Context) (sql.Row, error) {
	childRow, err := i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	return ProjectRow(ctx, i.p, childRow)
}

func (i *projectIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

// ProjectRow evaluates a set of projections.
func ProjectRow(
	ctx *sql.Context,
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
		if defaultVal, ok := expr.(*sql.ColumnDefaultValue); ok && !defaultVal.IsLiteral() {
			fields = append(fields, nil)
			secondPass = append(secondPass, i)
			continue
		}
		f, fErr := expr.Eval(ctx, row)
		if fErr != nil {
			return nil, fErr
		}
		fields = append(fields, f)
	}
	for _, index := range secondPass {
		fields[index], err = projections[index].Eval(ctx, fields)
		if err != nil {
			return nil, err
		}
	}
	return sql.NewRow(fields...), nil
}
