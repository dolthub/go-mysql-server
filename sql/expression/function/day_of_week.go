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

package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DayOfWeek is a function that returns the day of the week from a date where
// 1 = Sunday, ..., 7 = Saturday.
type DayOfWeek struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*DayOfWeek)(nil)
var _ sql.CollationCoercible = (*DayOfWeek)(nil)

// NewDayOfWeek creates a new DayOfWeek UDF.
func NewDayOfWeek(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &DayOfWeek{expression.UnaryExpressionStub{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *DayOfWeek) FunctionName() string {
	return "dayofweek"
}

// Description implements sql.FunctionExpression
func (d *DayOfWeek) Description() string {
	return "returns the day of the week of the given date."
}

// String implements the sql.Expression interface.
func (d *DayOfWeek) String() string { return fmt.Sprintf("DAYOFWEEK(%s)", d.Child) }

// Type implements the Expression interface.
func (d *DayOfWeek) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// IsNullable implements the Expression interface
func (d *DayOfWeek) IsNullable(ctx *sql.Context) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DayOfWeek) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *DayOfWeek) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpressionStub, row, dayOfWeek)
}

// WithChildren implements the Expression interface.
func (d *DayOfWeek) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayOfWeek(ctx, children[0]), nil
}
