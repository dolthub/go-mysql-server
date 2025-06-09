// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Oct function provides a string representation for the octal value of N, where N is a decimal number.
type Oct struct {
	n sql.Expression
}

var _ sql.FunctionExpression = (*Oct)(nil)
var _ sql.CollationCoercible = (*Oct)(nil)

func NewOct(n sql.Expression) sql.Expression { return &Oct{n} }

func (o *Oct) FunctionName() string {
	return "oct"
}

func (o *Oct) Description() string {
	return "returns a string representation for octal value of N, where N is a decimal number."
}

func (o *Oct) Type() sql.Type {
	return types.LongText
}

func (o *Oct) IsNullable() bool {
	return o.n.IsNullable()
}

func (o *Oct) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Convert a decimal (base 10) number to octal (base 8)
	return NewConv(
		o.n,
		expression.NewLiteral(10, types.Int64),
		expression.NewLiteral(8, types.Int64),
	).Eval(ctx, row)
}

func (o *Oct) Resolved() bool {
	return o.n.Resolved()
}

func (o *Oct) Children() []sql.Expression {
	return []sql.Expression{o.n}
}

func (o *Oct) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(children), 1)
	}
	return NewOct(children[0]), nil
}

func (o *Oct) String() string {
	return fmt.Sprintf("%s(%s)", o.FunctionName(), o.n)
}

func (*Oct) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4 // strings with collations
}
