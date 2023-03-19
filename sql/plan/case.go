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

	"github.com/dolthub/vitess/go/mysql"

	"github.com/gabereiser/go-mysql-server/sql"
)

// CaseStatement represents CASE statements, which are different from CASE expressions. These are intended for use in
// triggers and stored procedures. Specifically, this implements CASE statements when comparing each conditional to a
// value. The version of CASE that does not compare each conditional to a value is functionally equivalent to a series
// of IF/ELSE statements, and therefore we simply use an IfElseBlock.
type CaseStatement struct {
	Expr   sql.Expression
	IfElse *IfElseBlock
}

var _ sql.Node = (*CaseStatement)(nil)
var _ sql.DebugStringer = (*CaseStatement)(nil)
var _ sql.Expressioner = (*CaseStatement)(nil)

// NewCaseStatement creates a new *NewCaseStatement or *IfElseBlock node.
func NewCaseStatement(caseExpr sql.Expression, ifConditionals []*IfConditional, elseStatement sql.Node) sql.Node {
	if elseStatement == nil {
		elseStatement = elseCaseError{}
	}
	ifElse := &IfElseBlock{
		IfConditionals: ifConditionals,
		Else:           elseStatement,
	}
	if caseExpr != nil {
		return &CaseStatement{
			Expr:   caseExpr,
			IfElse: ifElse,
		}
	}
	return ifElse
}

// Resolved implements the interface sql.Node.
func (c *CaseStatement) Resolved() bool {
	return c.Expr.Resolved() && c.IfElse.Resolved()
}

// String implements the interface sql.Node.
func (c *CaseStatement) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("CASE %s", c.Expr.String())
	_ = p.WriteChildren(c.IfElse.String())
	return p.String()
}

// DebugString implements the sql.DebugStringer interface.
func (c *CaseStatement) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("CASE %s", sql.DebugString(c.Expr))
	_ = p.WriteChildren(sql.DebugString(c.IfElse))
	return p.String()
}

// Schema implements the interface sql.Node.
func (c *CaseStatement) Schema() sql.Schema {
	return c.IfElse.Schema()
}

// Children implements the interface sql.Node.
func (c *CaseStatement) Children() []sql.Node {
	return c.IfElse.Children()
}

// WithChildren implements the interface sql.Node.
func (c *CaseStatement) WithChildren(children ...sql.Node) (sql.Node, error) {
	newIfElseNode, err := c.IfElse.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	newIfElse, ok := newIfElseNode.(*IfElseBlock)
	if !ok {
		return nil, fmt.Errorf("%T: expected child %T but got %T", c, c.IfElse, newIfElseNode)
	}

	return &CaseStatement{
		Expr:   c.Expr,
		IfElse: newIfElse,
	}, nil
}

// Expressions implements the interface sql.Node.
func (c *CaseStatement) Expressions() []sql.Expression {
	return []sql.Expression{c.Expr}
}

// WithExpressions implements the interface sql.Node.
func (c *CaseStatement) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(exprs), 1)
	}

	return &CaseStatement{
		Expr:   exprs[0],
		IfElse: c.IfElse,
	}, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *CaseStatement) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return c.IfElse.CheckPrivileges(ctx, opChecker)
}

// RowIter implements the interface sql.Node.
func (c *CaseStatement) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	caseValue, err := c.Expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	for _, ifConditional := range c.IfElse.IfConditionals {
		whenValue, err := ifConditional.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		comparison, err := c.Expr.Type().Compare(caseValue, whenValue)
		if err != nil {
			return nil, err
		}
		if comparison != 0 {
			continue
		}

		return c.constructRowIter(ctx, row, ifConditional, ifConditional.Body)
	}

	// All conditions failed so we run the else
	return c.constructRowIter(ctx, row, c.IfElse.Else, c.IfElse.Else)
}

// constructRowIter is a helper function to create the sql.RowIter from the RowIter function.
func (c *CaseStatement) constructRowIter(ctx *sql.Context, row sql.Row, iterNode sql.Node, bodyNode sql.Node) (sql.RowIter, error) {
	// All conditions failed so we run the else
	branchIter, err := iterNode.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
	// node and schema will be a better representation of the iterated rows.
	if blockRowIter, ok := branchIter.(BlockRowIter); ok {
		return blockRowIter, nil
	}
	return &ifElseIter{
		branchIter: branchIter,
		sch:        bodyNode.Schema(),
		branchNode: bodyNode,
	}, nil
}

type elseCaseError struct{}

var _ sql.Node = elseCaseError{}

// Resolved implements the interface sql.Node.
func (e elseCaseError) Resolved() bool {
	return true
}

// String implements the interface sql.Node.
func (e elseCaseError) String() string {
	return "ELSE CASE ERROR"
}

// Schema implements the interface sql.Node.
func (e elseCaseError) Schema() sql.Schema {
	return nil
}

// Children implements the interface sql.Node.
func (e elseCaseError) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (e elseCaseError) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(e, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (e elseCaseError) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// RowIter implements the interface sql.Node.
func (e elseCaseError) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return elseCaseErrorIter{}, nil
}

type elseCaseErrorIter struct{}

var _ sql.RowIter = elseCaseErrorIter{}

// Next implements the interface sql.RowIter.
func (e elseCaseErrorIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, mysql.NewSQLError(1339, "20000", "Case not found for CASE statement")
}

// Close implements the interface sql.RowIter.
func (e elseCaseErrorIter) Close(context *sql.Context) error {
	return nil
}
