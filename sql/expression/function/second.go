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

// Second is a function that returns the second of a date.
type Second struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Second)(nil)
var _ sql.CollationCoercible = (*Second)(nil)

// NewSecond creates a new Second UDF.
func NewSecond(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Second{expression.UnaryExpressionStub{Child: date}}
}

// Name implements sql.FunctionExpression
func (s *Second) Name() string {
	return "second"
}

// Description implements sql.FunctionExpression
func (s *Second) Description() string {
	return "returns the seconds of the given date."
}

// String implements the sql.Expression interface.
func (s *Second) String() string { return fmt.Sprintf("%s(%s)", s.Name(), s.Child) }

// Type implements the Expression interface.
func (s *Second) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Second) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (s *Second) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return getDatePart(ctx, s.UnaryExpressionStub, row, second)
}

// WithChildren implements the Expression interface.
func (s *Second) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSecond(ctx, children[0]), nil
}
