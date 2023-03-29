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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InSubquery is an expression that checks an expression is in the result of a subquery. It's in the plan package,
// instead of the expression package, because Subquery is itself in the plan package (because it functions more like a
// plan node than an expression in its evaluation).
type InSubquery struct {
	expression.BinaryExpression
}

var _ sql.Expression = (*InSubquery)(nil)
var _ sql.CollationCoercible = (*InSubquery)(nil)

// Type implements sql.Expression
func (in *InSubquery) Type() sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*InSubquery) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// NewInSubquery creates an InSubquery expression.
func NewInSubquery(left sql.Expression, right sql.Expression) *InSubquery {
	return &InSubquery{expression.BinaryExpression{Left: left, Right: right}}
}

var nilKey, _ = sql.HashOf(sql.NewRow(nil))

// Eval implements the Expression interface.
func (in *InSubquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left.Type().Promote()
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
		if types.NumColumns(typ) != types.NumColumns(right.Type()) {
			return nil, sql.ErrInvalidOperandColumns.New(types.NumColumns(typ), types.NumColumns(right.Type()))
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

		// convert left to right's type
		nLeft, err := typ.Convert(left)
		if err != nil {
			return false, nil
		}

		key, err := sql.HashOf(sql.NewRow(nLeft))
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
			return false, nil
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
func (in *InSubquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewInSubquery(children[0], children[1]), nil
}

func (in *InSubquery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("InSubquery")
	children := []string{fmt.Sprintf("left: %s", in.Left), fmt.Sprintf("right: %s", in.Right)}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (in *InSubquery) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("InSubquery")
	children := []string{fmt.Sprintf("left: %s", sql.DebugString(in.Left)), fmt.Sprintf("right: %s", sql.DebugString(in.Right))}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// Children implements the Expression interface.
func (in *InSubquery) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}

// Dispose implements sql.Disposable
func (in *InSubquery) Dispose() {
	if sq, ok := in.Right.(*Subquery); ok {
		sq.Dispose()
	}
}

// NewNotInSubquery creates a new NotInSubquery expression.
func NewNotInSubquery(left sql.Expression, right sql.Expression) sql.Expression {
	return expression.NewNot(NewInSubquery(left, right))
}
