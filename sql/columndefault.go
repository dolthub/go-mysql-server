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
	Expression      // the expression representing this default value
	outType    Type // if non-nil, converts the output of the expression into this type
	literal    bool // whether the default value is a literal or expression
	returnNil  bool // if the expression returns a nil value, then this determines whether the result is returned or an error is returned
}

var _ Expression = (*ColumnDefaultValue)(nil)

// NewColumnDefaultValue returns a new ColumnDefaultValue expression.
func NewColumnDefaultValue(expr Expression, outType Type, representsLiteral bool, mayReturnNil bool) (*ColumnDefaultValue, error) {
	colDefault := &ColumnDefaultValue{
		Expression: expr,
		outType:    outType,
		literal:    representsLiteral,
		returnNil:  mayReturnNil,
	}
	if err := colDefault.checkType(outType); err != nil {
		return nil, err
	}
	return colDefault, nil
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
	if e.outType != nil {
		val, err = e.outType.Convert(val)
		if err != nil {
			return nil, err
		}
	}
	if val == nil && !e.returnNil {
		return nil, ErrColumnDefaultReturnedNull.New()
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
func (e *ColumnDefaultValue) WithChildren(ctx *Context, children ...Expression) (Expression, error) {
	if len(children) != 1 {
		return nil, ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	if e == nil {
		return NewColumnDefaultValue(children[0], e.outType, len(children[0].Children()) == 0, true) //impossible to know, best guess
	} else {
		return NewColumnDefaultValue(children[0], e.outType, e.literal, e.returnNil)
	}
}

// WithType returns a new default value that converts all resulting values from the internal expression into the given type.
// If the internal expression results in a value that cannot be converted to the given type, then an error is returned.
func (e *ColumnDefaultValue) WithType(outType Type) (*ColumnDefaultValue, error) {
	if e == nil {
		return nil, nil
	}
	if err := e.checkType(outType); err != nil {
		return nil, err
	}
	return NewColumnDefaultValue(e.Expression, outType, e.literal, e.returnNil)
}

func (e *ColumnDefaultValue) checkType(outType Type) error {
	if outType != nil && e.literal {
		val, err := e.Expression.Eval(NewEmptyContext(), nil) // since it's a literal, we can use an empty context
		if err != nil {
			return err
		}
		if val == nil && !e.returnNil {
			return ErrIncompatibleDefaultType.New()
		}
		_, err = outType.Convert(val)
		if err != nil {
			return ErrIncompatibleDefaultType.New()
		}
	}
	return nil
}
