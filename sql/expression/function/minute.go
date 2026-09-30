// Copyright 2026 Dolthub, Inc.
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

// Minute is a function that returns the minute of a date.
type Minute struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Minute)(nil)
var _ sql.CollationCoercible = (*Minute)(nil)

// NewMinute creates a new Minute UDF.
func NewMinute(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Minute{expression.UnaryExpressionStub{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (m *Minute) FunctionName() string {
	return "minute"
}

// Description implements sql.FunctionExpression
func (m *Minute) Description() string {
	return "returns the minutes of the given date."
}

// String implements the sql.Expression interface.
func (m *Minute) String() string { return fmt.Sprintf("%s(%s)", m.FunctionName(), m.Child) }

// Type implements the Expression interface.
func (m *Minute) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Minute) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (m *Minute) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return getDatePart(ctx, m.UnaryExpressionStub, row, minute)
}

// WithChildren implements the Expression interface.
func (m *Minute) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMinute(ctx, children[0]), nil
}
