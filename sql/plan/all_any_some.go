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

type AllExpr struct {
	Sq *Subquery
}

var _ sql.Expression = (*AllExpr)(nil)

func NewAllExpr(expr sql.Expression) (*AllExpr, error) {
	if sq, ok := expr.(*Subquery); ok {
		return &AllExpr{Sq: sq}, nil
	}
	return nil, fmt.Errorf("attempted to create all expression over non-subquery, %T", expr)
}

// Resolved implements the Expression interface.
func (a *AllExpr) Resolved() bool {
	panic("all expr is a placeholder")
}

// String implements the Expression interface.
func (a *AllExpr) String() string {
	panic("all expr is a placeholder")
}

// Type implements the Expression interface.
func (a *AllExpr) Type() sql.Type {
	panic("all expr is a placeholder")
}

// IsNullable implements the Expression interface.
func (a *AllExpr) IsNullable() bool {
	panic("all expr is a placeholder")
}

// Eval implements the Expression interface.
func (a *AllExpr) Eval(ctx *sql.Context, row sql.Row) ( interface{},  error) {
	panic("all expr is a placeholder")
}

// Children implements the Expression interface.
func (a *AllExpr) Children() []sql.Expression {
	panic("all expr is a placeholder")
}

// WithChildren implements the Expression interface.
func (a *AllExpr) WithChildren(children ...sql.Expression) ( sql.Expression,  error) {
	panic("all expr is a placeholder")
}


// All is an expression that checks an expression is inside a list of expressions.
type All struct {
	Comparator string    // TODO: make this an enum
	Left  sql.Expression // TODO: expression?
	Right sql.Expression
}


var _ sql.Expression = (*All)(nil)
var _ sql.CollationCoercible = (*All)(nil)

// NewAll creates All expression.
func NewAll(cmp string, left, right sql.Expression) *All {
	return &All{
		Comparator: cmp,
		Left:  left,
		Right: right,
	}
}

// Resolved implements the Expression interface.
func (a *All) Resolved() bool {
	return a.Left.Resolved() && a.Right.Resolved()
}

// String implements the Expression interface.
func (a *All) String() string {
	return "TODO: implement this"
}

// Type implements the Expression interface.
func (a *All) Type() sql.Type {
	return types.Boolean
}

// IsNullable implements the Expression interface.
func (a *All) IsNullable() bool {
	return true
}

// Eval implements the Expression interface.
func (a *All) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sq := a.Right.(*Subquery)
	rows, err := sq.EvalMultiple(ctx, row)
	if err != nil {
		return nil, err
	}

	switch a.Comparator {
	case "=":
		gf := expression.NewGetField(0, a.Right.Type(), "t", false)
		eq := expression.NewEquals(a.Left, gf)
		for _, r := range rows {
			row := sql.Row{r}
			res, err := eq.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			if res == false {
				return false, nil
			}
		}
		return true, nil
	default:
		panic(fmt.Sprintf("unhandled comparator: %s", a.Comparator))
	}
}

// Children implements the Expression interface.
func (a *All) Children() []sql.Expression {
	return []sql.Expression{a.Left, a.Right}
}

// WithChildren implements the Expression interface.
func (a *All) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
	}
	return NewAll(a.Comparator, children[0], children[1]), nil
}

// CollationCoercibility implements the sql.CollationCoercible interface.
func (a *All) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}
