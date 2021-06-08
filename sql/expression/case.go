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

package expression

import (
	"bytes"

	"github.com/dolthub/go-mysql-server/sql"
)

// CaseBranch is a single branch of a case expression.
type CaseBranch struct {
	Cond  sql.Expression
	Value sql.Expression
}

// Case is an expression that returns the value of one of its branches when a
// condition is met.
type Case struct {
	Expr     sql.Expression
	Branches []CaseBranch
	Else     sql.Expression
}

// NewCase returns an new Case expression.
func NewCase(expr sql.Expression, branches []CaseBranch, elseExpr sql.Expression) *Case {
	return &Case{expr, branches, elseExpr}
}

// From the description of operator typing here:
// https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#operator_case
func combinedCaseBranchType(left, right sql.Type) sql.Type {
	if left == sql.Null {
		return right
	}
	if right == sql.Null {
		return left
	}
	if sql.IsTextOnly(left) && sql.IsTextOnly(right) {
		return sql.LongText
	}
	if sql.IsTextBlob(left) && sql.IsTextBlob(right) {
		return sql.LongBlob
	}
	if sql.IsTime(left) && sql.IsTime(right) {
		if left == right {
			return left
		}
		return sql.Datetime
	}
	if sql.IsNumber(left) && sql.IsNumber(right) {
		if left == sql.Float64 || right == sql.Float64 {
			return sql.Float64
		}
		if left == sql.Float32 || right == sql.Float32 {
			return sql.Float32
		}
		if sql.IsDecimal(left) || sql.IsDecimal(right) {
			return sql.MustCreateDecimalType(65, 10)
		}
		if left == sql.Uint64 && sql.IsSigned(right) ||
			right == sql.Uint64 && sql.IsSigned(left) {
			return sql.MustCreateDecimalType(65, 10)
		}
		if !sql.IsSigned(left) && !sql.IsSigned(right) {
			return sql.Uint64
		} else {
			return sql.Int64
		}
	}
	return sql.LongText
}

// Type implements the sql.Expression interface.
func (c *Case) Type() sql.Type {
	curr := sql.Null
	for _, b := range c.Branches {
		curr = combinedCaseBranchType(curr, b.Value.Type())
	}
	if c.Else != nil {
		curr = combinedCaseBranchType(curr, c.Else.Type())
	}
	return curr
}

// IsNullable implements the sql.Expression interface.
func (c *Case) IsNullable() bool {
	for _, b := range c.Branches {
		if b.Value.IsNullable() {
			return true
		}
	}

	return c.Else == nil || c.Else.IsNullable()
}

// Resolved implements the sql.Expression interface.
func (c *Case) Resolved() bool {
	if (c.Expr != nil && !c.Expr.Resolved()) ||
		(c.Else != nil && !c.Else.Resolved()) {
		return false
	}

	for _, b := range c.Branches {
		if !b.Cond.Resolved() || !b.Value.Resolved() {
			return false
		}
	}

	return true
}

// Children implements the sql.Expression interface.
func (c *Case) Children() []sql.Expression {
	var children []sql.Expression

	if c.Expr != nil {
		children = append(children, c.Expr)
	}

	for _, b := range c.Branches {
		children = append(children, b.Cond, b.Value)
	}

	if c.Else != nil {
		children = append(children, c.Else)
	}

	return children
}

// Eval implements the sql.Expression interface.
func (c *Case) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.Case")
	defer span.Finish()

	var expr interface{}
	var err error
	if c.Expr != nil {
		expr, err = c.Expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	t := c.Type()

	for _, b := range c.Branches {
		var cond sql.Expression
		if expr != nil {
			cond = NewEquals(NewLiteral(expr, c.Expr.Type()), b.Cond)
		} else {
			cond = b.Cond
		}

		res, err := sql.EvaluateCondition(ctx, cond, row)
		if err != nil {
			return nil, err
		}

		if sql.IsTrue(res) {
			bval, err := b.Value.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			return t.Convert(bval)
		}
	}

	if c.Else != nil {
		val, err := c.Else.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		return t.Convert(val)
	}

	return nil, nil
}

// WithChildren implements the Expression interface.
func (c *Case) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	var expected = len(c.Branches) * 2
	if c.Expr != nil {
		expected++
	}

	if c.Else != nil {
		expected++
	}

	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), expected)
	}

	var expr, elseExpr sql.Expression
	if c.Expr != nil {
		expr = children[0]
		children = children[1:]
	}

	if c.Else != nil {
		elseExpr = children[len(children)-1]
		children = children[:len(children)-1]
	}

	var branches []CaseBranch
	for i := 0; i < len(children); i += 2 {
		branches = append(branches, CaseBranch{
			Cond:  children[i],
			Value: children[i+1],
		})
	}

	return NewCase(expr, branches, elseExpr), nil
}

func (c *Case) String() string {
	var buf bytes.Buffer

	buf.WriteString("CASE ")
	if c.Expr != nil {
		buf.WriteString(c.Expr.String())
	}

	for _, b := range c.Branches {
		buf.WriteString(" WHEN ")
		buf.WriteString(b.Cond.String())
		buf.WriteString(" THEN ")
		buf.WriteString(b.Value.String())
	}

	if c.Else != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(c.Else.String())
	}

	buf.WriteString(" END")
	return buf.String()
}

func (c *Case) DebugString() string {
	var buf bytes.Buffer

	buf.WriteString("CASE ")
	if c.Expr != nil {
		buf.WriteString(sql.DebugString(c.Expr))
	}

	for _, b := range c.Branches {
		buf.WriteString(" WHEN ")
		buf.WriteString(sql.DebugString(b.Cond))
		buf.WriteString(" THEN ")
		buf.WriteString(sql.DebugString(b.Value))
	}

	if c.Else != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(sql.DebugString(c.Else))
	}

	buf.WriteString(" END")
	return buf.String()
}
