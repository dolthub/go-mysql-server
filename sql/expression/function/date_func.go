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
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Date a function takes the DATE part out from a datetime expression.
type Date struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Date)(nil)
var _ sql.CollationCoercible = (*Date)(nil)

// Name implements sql.FunctionExpression
func (d *Date) Name() string {
	return "date"
}

// Description implements sql.FunctionExpression
func (d *Date) Description() string {
	return "returns the date part of the given date."
}

// NewDate returns a new Date node.
func NewDate(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Date{expression.UnaryExpressionStub{Child: date}}
}

// String implements the sql.Expression interface.
func (d *Date) String() string { return fmt.Sprintf("DATE(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Date) Type(ctx *sql.Context) sql.Type { return types.Date }

// IsNullable implements the Expression interface
func (d *Date) IsNullable(ctx *sql.Context) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Date) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *Date) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := d.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	date, _, err := types.Date.Convert(ctx, val)
	if err != nil && (sql.ErrTruncatedIncorrect.Is(err) || sql.ErrIncorrectValue.Is(err)) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}

	switch v := date.(type) {
	case time.Time:
		if !types.ZeroTime.Equal(v) {
			return v, nil
		}
		if err == nil {
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(types.Date.String(), date).Error())
		}
	}

	return nil, nil
}

// WithChildren implements the Expression interface.
func (d *Date) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDate(ctx, children[0]), nil
}
