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
	"github.com/dolthub/go-mysql-server/sql/types"
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

var _ sql.Expression = (*Case)(nil)
var _ sql.CollationCoercible = (*Case)(nil)

// NewCase returns an new Case expression.
func NewCase(expr sql.Expression, branches []CaseBranch, elseExpr sql.Expression) *Case {
	return &Case{expr, branches, elseExpr}
}

// From the description of operator typing here:
// https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#operator_case
func combinedCaseBranchType(left, right sql.Type) sql.Type {
	if left == types.Null {
		return right
	}
	if right == types.Null {
		return left
	}
	if types.IsTextOnly(left) && types.IsTextOnly(right) {
		return types.LongText
	}
	if types.IsTextBlob(left) && types.IsTextBlob(right) {
		return types.LongBlob
	}
	if types.IsTime(left) && types.IsTime(right) {
		if left == right {
			return left
		}
		return types.Datetime
	}
	if types.IsNumber(left) && types.IsNumber(right) {
		if left == types.Float64 || right == types.Float64 {
			return types.Float64
		}
		if left == types.Float32 || right == types.Float32 {
			return types.Float32
		}
		if types.IsDecimal(left) || types.IsDecimal(right) {
			return types.MustCreateDecimalType(65, 10)
		}
		if left == types.Uint64 && types.IsSigned(right) ||
			right == types.Uint64 && types.IsSigned(left) {
			return types.MustCreateDecimalType(65, 10)
		}
		if !types.IsSigned(left) && !types.IsSigned(right) {
			return types.Uint64
		} else {
			return types.Int64
		}
	}
	if types.IsJSON(left) && types.IsJSON(right) {
		return types.JSON
	}
	return types.LongText
}

// Type implements the sql.Expression interface.
func (c *Case) Type() sql.Type {
	curr := types.Null
	for _, b := range c.Branches {
		curr = combinedCaseBranchType(curr, b.Value.Type())
	}
	if c.Else != nil {
		curr = combinedCaseBranchType(curr, c.Else.Type())
	}
	return curr
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Case) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// This should be calculated during the expression's evaluation, but that's not possible with the
	// current abstraction
	return c.Type().CollationCoercibility(ctx)
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
	defer span.End()

	t := c.Type()

	for _, b := range c.Branches {
		var cond sql.Expression
		if c.Expr != nil {
			cond = NewEquals(c.Expr, b.Cond)
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
func (c *Case) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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
