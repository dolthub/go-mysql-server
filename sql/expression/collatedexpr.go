// Copyright 2022 Dolthub, Inc.
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

// CollatedExpression represents an expression (returning a string or byte slice) that carries a collation (which
// implicitly also carries a character set). This does not handle any encoding or decoding of the character set, as this
// is strictly for collations.
type CollatedExpression struct {
	expr      sql.Expression
	collation sql.CollationID
}

var _ sql.Expression = (*CollatedExpression)(nil)
var _ sql.DebugStringer = (*CollatedExpression)(nil)

// NewCollatedExpression creates a new CollatedExpression expression. If the given expression is already a
// CollatedExpression, then the previous collation is overriden with the given one.
func NewCollatedExpression(expr sql.Expression, collation sql.CollationID) *CollatedExpression {
	if collatedExpr, ok := expr.(*CollatedExpression); ok {
		return &CollatedExpression{
			expr:      collatedExpr.expr,
			collation: collation,
		}
	}
	return &CollatedExpression{
		expr:      expr,
		collation: collation,
	}
}

// Resolved implements the sql.Expression interface.
func (ce *CollatedExpression) Resolved() bool {
	return ce.expr.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (ce *CollatedExpression) IsNullable() bool {
	return ce.expr.IsNullable()
}

// Type implements the sql.Expression interface.
func (ce *CollatedExpression) Type() sql.Type {
	return ce.expr.Type()
}

// Eval implements the sql.Expression interface.
func (ce *CollatedExpression) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if !sql.IsText(ce.expr.Type()) {
		return nil, sql.ErrCollatedExprWrongType.New()
	}
	return ce.expr.Eval(ctx, row)
}

func (ce *CollatedExpression) String() string {
	return fmt.Sprintf("%s COLLATE %s", ce.expr.String(), ce.collation.String())
}

// DebugString implements the sql.DebugStringer interface.
func (ce *CollatedExpression) DebugString() string {
	var innerDebugStr string
	if debugExpr, ok := ce.expr.(sql.DebugStringer); ok {
		innerDebugStr = debugExpr.DebugString()
	} else {
		innerDebugStr = ce.expr.String()
	}
	return fmt.Sprintf("%s COLLATE %s", innerDebugStr, ce.collation.String())
}

// WithChildren implements the sql.Expression interface.
func (ce *CollatedExpression) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ce, len(children), 1)
	}
	return &CollatedExpression{
		expr:      children[0],
		collation: ce.collation,
	}, nil
}

// Children implements the sql.Expression interface.
func (ce *CollatedExpression) Children() []sql.Expression {
	return []sql.Expression{ce.expr}
}

// Child returns the inner expression.
func (ce *CollatedExpression) Child() sql.Expression {
	return ce.expr
}
