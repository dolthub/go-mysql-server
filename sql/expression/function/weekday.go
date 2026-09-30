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
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Weekday is a function that returns the weekday of a date where 0 = Monday,
// ..., 6 = Sunday.
type Weekday struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Weekday)(nil)
var _ sql.CollationCoercible = (*Weekday)(nil)

// NewWeekday creates a new Weekday UDF.
func NewWeekday(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Weekday{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "WEEKDAY", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (d *Weekday) Description() string {
	return "returns the weekday of the given date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Weekday) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *Weekday) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	dt, ok := val.(time.Time)
	if !ok {
		return nil, nil
	}
	if types.ZeroTime.Equal(dt) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(types.Datetime.String(), val).Error())
		return nil, nil
	}
	return (int(dt.Weekday()) + 6) % 7, nil
}

// WithChildren implements the Expression interface.
func (d *Weekday) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewWeekday(ctx, children[0]), nil
}
