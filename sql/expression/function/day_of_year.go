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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DayOfYear is a function that returns the day of the year from a date.
type DayOfYear struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*DayOfYear)(nil)
var _ sql.CollationCoercible = (*DayOfYear)(nil)

// NewDayOfYear creates a new DayOfYear UDF.
func NewDayOfYear(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &DayOfYear{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "DAYOFYEAR", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (d *DayOfYear) Description() string {
	return "returns the day of the year of the given date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DayOfYear) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *DayOfYear) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return getDatePart(ctx, d.UnaryExpressionStub, row, dayOfYear)
}

// WithChildren implements the Expression interface.
func (d *DayOfYear) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayOfYear(ctx, children[0]), nil
}
