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

package sql

import (
	"fmt"
)

// ColumnDefaultValue is an expression representing the default value of a column. May represent both a default literal
// and a default expression. A nil pointer of this type represents an implicit default value and is thus valid, so all
// method calls will return without error.
type ColumnDefaultValue struct {
	// Expression is the expression representing this default value
	Expression
	// outType converts the output of the expression into this type, when not nil
	outType Type
	// literal indicates whether the default value is a literal value or expression
	literal bool
	// returnNil indicates whether a nil value from the default value expression is returned as null or an error
	returnNil bool
	// parenthesized indicates whether the value was specified in parens or not; this is typically the opposite of the literal field,
	// but they can both be false in the case of now/current_timestamp for datetimes and timestamps.
	parenthesized bool
}

var _ Expression = (*ColumnDefaultValue)(nil)

// NewColumnDefaultValue returns a new ColumnDefaultValue expression.
func NewColumnDefaultValue(expr Expression, outType Type, representsLiteral bool, parenthesized bool, mayReturnNil bool) (*ColumnDefaultValue, error) {
	return &ColumnDefaultValue{
		Expression:    expr,
		outType:       outType,
		literal:       representsLiteral,
		returnNil:     mayReturnNil,
		parenthesized: parenthesized,
	}, nil
}

// NewUnresolvedColumnDefaultValue returns a column default
func NewUnresolvedColumnDefaultValue(expr string) *ColumnDefaultValue {
	return &ColumnDefaultValue{
		Expression: UnresolvedColumnDefault{exprString: expr},
	}
}

// Children implements sql.Expression
func (e *ColumnDefaultValue) Children() []Expression {
	if e == nil {
		return nil
	}
	return []Expression{e.Expression}
}

// Eval implements sql.Expression
func (e *ColumnDefaultValue) Eval(ctx *Context, r Row) (interface{}, error) {
	if e == nil {
		return nil, nil
	}

	val, err := e.Expression.Eval(ctx, r)
	if err != nil {
		return nil, err
	}

	if val == nil && !e.returnNil {
		return nil, ErrColumnDefaultReturnedNull.New()
	}

	if e.outType != nil {
		if val, err = e.outType.Convert(val); err != nil {
			return nil, ErrIncompatibleDefaultType.New()
		}
	}

	return val, nil
}

// IsLiteral returns whether this expression represents a literal default value (otherwise it's an expression default value).
func (e *ColumnDefaultValue) IsLiteral() bool {
	if e == nil {
		return true // we return the literal nil, hence true
	}
	return e.literal
}

// IsParenthesized returns whether this column default was specified in parentheses, using the expression default value form.
// It is almost always the opposite of IsLiteral, but there is one edge case where matching MySQL's behavior require that
// we can distinguish between a non-literal value in parens and a non-literal value not in parens. The edge case is using
// now/current_timestamp as a column default; now/current_timestamp can be specified without parens for datetime/timestamp
// fields, but it must be enclosed in parens to be used as the default for other types.
func (e *ColumnDefaultValue) IsParenthesized() bool {
	if e == nil {
		return false // we return the literal nil, hence false
	}
	return e.parenthesized
}

// IsNullable implements sql.Expression
func (e *ColumnDefaultValue) IsNullable() bool {
	if e == nil {
		return true
	}
	if !e.returnNil {
		return false
	}
	return e.Expression.IsNullable()
}

// Resolved implements sql.Expression
func (e *ColumnDefaultValue) Resolved() bool {
	if e == nil {
		return true
	}
	if e.outType == nil {
		return false
	}
	return e.Expression.Resolved()
}

// String implements sql.Expression
func (e *ColumnDefaultValue) String() string {
	//TODO: currently (2+2)/2 will, when output as a string, give (2 + 2 / 2), which is clearly wrong
	if e == nil {
		return ""
	}

	// https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
	// The default value specified in a DEFAULT clause can be a literal constant or an expression. With one exception,
	// enclose expression default values within parentheses to distinguish them from literal constant default values.
	if e.literal {
		return e.Expression.String()
	} else {
		return fmt.Sprintf("(%s)", e.Expression.String())
	}
}

func (e *ColumnDefaultValue) DebugString() string {
	if e == nil {
		return ""
	}

	if e.literal {
		return DebugString(e.Expression)
	} else if e.parenthesized {
		return fmt.Sprintf("parenthesized(%s)", DebugString(e.Expression))
	} else {
		return fmt.Sprintf("(%s)", DebugString(e.Expression))
	}
}

// Type implements sql.Expression
func (e *ColumnDefaultValue) Type() Type {
	if e == nil {
		return Null
	}
	if e.outType == nil {
		return e.Expression.Type()
	}
	return e.outType
}

// WithChildren implements sql.Expression
func (e *ColumnDefaultValue) WithChildren(children ...Expression) (Expression, error) {
	if e == nil && len(children) == 0 {
		return e, nil
	}
	if len(children) != 1 {
		return nil, ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	if e == nil {
		isLiteral := len(children[0].Children()) == 0 //impossible to know, best guess
		return NewColumnDefaultValue(children[0], e.outType, isLiteral, !isLiteral, true)
	} else {
		return NewColumnDefaultValue(children[0], e.outType, e.literal, e.parenthesized, e.returnNil)
	}
}

// CheckType validates that the ColumnDefaultValue has the correct type.
func (e *ColumnDefaultValue) CheckType(ctx *Context) error {
	if e.outType != nil && e.literal {
		val, err := e.Expression.Eval(ctx, nil)
		if err != nil {
			return err
		}
		if val == nil && !e.returnNil {
			return ErrIncompatibleDefaultType.New()
		}
		_, err = e.outType.Convert(val)
		if err != nil {
			return ErrIncompatibleDefaultType.Wrap(err)
		}
	}
	return nil
}

type UnresolvedColumnDefault struct {
	exprString string
}

func (u UnresolvedColumnDefault) Resolved() bool {
	return false
}

func (u UnresolvedColumnDefault) String() string {
	return u.exprString
}

func (u UnresolvedColumnDefault) Type() Type {
	panic("UnresolvedColumnDefault is a placeholder node, but Type() was called")
}

func (u UnresolvedColumnDefault) IsNullable() bool {
	return true
}

func (u UnresolvedColumnDefault) Eval(ctx *Context, row Row) (interface{}, error) {
	panic("UnresolvedColumnDefault is a placeholder node, but Eval() was called")
}

func (u UnresolvedColumnDefault) Children() []Expression {
	return nil
}

func (u UnresolvedColumnDefault) WithChildren(children ...Expression) (Expression, error) {
	if len(children) != 0 {
		return nil, ErrInvalidChildrenNumber.New(u, len(children), 0)
	}
	return u, nil
}
