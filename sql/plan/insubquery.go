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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// InSubquery is an expression that checks an expression is in the result of a subquery. It's in the plan package,
// instead of the expression package, because Subquery is itself in the plan package (because it functions more like a
// plan node than an expression in its evaluation).
type InSubquery struct {
	expression.BinaryExpression
}

var _ sql.Expression = (*InSubquery)(nil)

// Type implements sql.Expression
func (in *InSubquery) Type() sql.Type {
	return sql.Boolean
}

// NewInSubquery creates an InSubquery expression.
func NewInSubquery(left sql.Expression, right sql.Expression) *InSubquery {
	return &InSubquery{expression.BinaryExpression{Left: left, Right: right}}
}

var nilKey, _ = sql.HashOf(sql.NewRow(nil))

// Eval implements the Expression interface.
func (in *InSubquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left.Type().Promote()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// The NULL handling for IN expressions is tricky. According to
	// https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in:
	// To comply with the SQL standard, IN() returns NULL not only if the expression on the left hand side is NULL, but
	// also if no match is found in the list and one of the expressions in the list is NULL.
	// However, there's a strange edge case. NULL IN (empty list) return 0, not NULL.
	leftNull := left == nil

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	switch right := in.Right.(type) {
	case *Subquery:
		if leftElems > 1 {
			// TODO: support more than one element in IN
			return nil, expression.ErrInvalidOperandColumns.New(leftElems, 1)
		}

		typ := right.Type()
		values, err := right.HashMultiple(ctx, row)
		if err != nil {
			return nil, err
		}

		// NULL IN (list) returns NULL. NULL IN (empty list) returns 0
		if leftNull {
			if values.Size() == 0 {
				return false, nil
			}
			return nil, nil
		}

		key, err := sql.HashOf(sql.NewRow(left))
		if err != nil {
			return nil, err
		}

		val, notFoundErr := values.Get(key)
		if notFoundErr != nil {
			if _, nilValNotFoundErr := values.Get(nilKey); nilValNotFoundErr == nil {
				return nil, nil
			}
			return false, nil
		}

		val, err = typ.Convert(val)
		if err != nil {
			return nil, err
		}

		cmp, err := typ.Compare(left, val)
		if err != nil {
			return nil, err
		}

		return cmp == 0, nil

	default:
		return nil, expression.ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *InSubquery) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewInSubquery(children[0], children[1]), nil
}

func (in *InSubquery) String() string {
	return fmt.Sprintf("(%s IN %s)", in.Left, in.Right)
}

func (in *InSubquery) DebugString() string {
	return fmt.Sprintf("(%s IN %s)", sql.DebugString(in.Left), sql.DebugString(in.Right))
}

// Children implements the Expression interface.
func (in *InSubquery) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}

// NewNotInSubquery creates a new NotInSubquery expression.
func NewNotInSubquery(left sql.Expression, right sql.Expression) sql.Expression {
	return expression.NewNot(NewInSubquery(left, right))
}
