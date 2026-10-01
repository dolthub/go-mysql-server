// Copyright 2024 Dolthub, Inc.
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

package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AnyValue is a scalar function that suppresses ONLY_FULL_GROUP_BY validation
// for its child expression and returns the child's evaluated value.
type AnyValue struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*AnyValue)(nil)
var _ sql.CollationCoercible = (*AnyValue)(nil)

// NewAnyValue creates a new AnyValue expression.
func NewAnyValue(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &AnyValue{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements [sql.FunctionExpression].
func (a *AnyValue) FunctionName() string {
	return "any_value"
}

// Description implements [sql.FunctionExpression].
func (a *AnyValue) Description() string {
	return "suppresses ONLY_FULL_GROUP_BY rejection for an expression and returns its value."
}

// Type implements [sql.Expression].
func (a *AnyValue) Type(ctx *sql.Context) sql.Type {
	return a.Child.Type(ctx)
}

// IsNullable implements [sql.Expression].
func (a *AnyValue) IsNullable(ctx *sql.Context) bool {
	return a.Child.IsNullable(ctx)
}

// CollationCoercibility implements [sql.CollationCoercible].
func (a *AnyValue) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	return sql.GetCoercibility(ctx, a.Child)
}

// String implements [sql.Expression].
func (a *AnyValue) String() string {
	return fmt.Sprintf("ANY_VALUE(%s)", a.Child.String())
}

// DebugString implements [sql.Expression].
func (a *AnyValue) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("ANY_VALUE(%s)", sql.DebugString(ctx, a.Child))
}

// WithChildren implements [sql.Expression].
func (a *AnyValue) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("any_value", 1, len(children))
	}
	return NewAnyValue(ctx, children[0]), nil
}

// Eval implements [sql.Expression].
func (a *AnyValue) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return a.Child.Eval(ctx, row)
}
