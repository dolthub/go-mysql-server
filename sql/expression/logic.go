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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// And checks whether two expressions are true.
type And struct {
	BinaryExpression
}

// NewAnd creates a new And expression.
func NewAnd(left, right sql.Expression) sql.Expression {
	return &And{BinaryExpression{Left: left, Right: right}}
}

// JoinAnd joins several expressions with And.
func JoinAnd(exprs ...sql.Expression) sql.Expression {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := NewAnd(exprs[0], exprs[1])
		for _, e := range exprs[2:] {
			result = NewAnd(result, e)
		}
		return result
	}
}

func (a *And) String() string {
	return fmt.Sprintf("(%s AND %s)", a.Left, a.Right)
}

func (a *And) DebugString() string {
	return fmt.Sprintf("(%s AND %s)", sql.DebugString(a.Left), sql.DebugString(a.Right))
}

// Type implements the Expression interface.
func (*And) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (a *And) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, err := a.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if lval != nil {
		lvalBool, err := sql.ConvertToBool(lval)
		if err == nil && lvalBool == false {
			return false, nil
		}
	}

	rval, err := a.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if rval != nil {
		rvalBool, err := sql.ConvertToBool(rval)
		if err == nil && rvalBool == false {
			return false, nil
		}
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	return true, nil
}

// WithChildren implements the Expression interface.
func (a *And) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
	}
	return NewAnd(children[0], children[1]), nil
}

// Or checks whether one of the two given expressions is true.
type Or struct {
	BinaryExpression
}

// NewOr creates a new Or expression.
func NewOr(left, right sql.Expression) sql.Expression {
	return &Or{BinaryExpression{Left: left, Right: right}}
}

func (o *Or) String() string {
	return fmt.Sprintf("(%s OR %s)", o.Left, o.Right)
}

func (o *Or) DebugString() string {
	return fmt.Sprintf("%s OR %s", sql.DebugString(o.Left), sql.DebugString(o.Right))
}

// Type implements the Expression interface.
func (*Or) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (o *Or) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, err := o.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if lval != nil {
		lvalBool, err := sql.ConvertToBool(lval)
		if err == nil && lvalBool {
			return true, nil
		}
	}

	if lval == true {
		return true, nil
	}

	rval, err := o.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if rval != nil {
		rvalBool, err := sql.ConvertToBool(rval)
		if err == nil && rvalBool {
			return true, nil
		}
	}

	if lval == nil && rval == nil {
		return nil, nil
	}

	return rval == true, nil
}

// WithChildren implements the Expression interface.
func (o *Or) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(children), 2)
	}
	return NewOr(children[0], children[1]), nil
}
