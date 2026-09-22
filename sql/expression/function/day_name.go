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

// DayName implements the DAYNAME function
type DayName struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*DayName)(nil)

// NewDayName creates a new DayName UDF.
func NewDayName(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &DayName{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(arg, "DAYNAME", types.Text),
	}
}

// Description implements sql.FunctionExpression
func (d *DayName) Description() string {
	return "returns the name of the weekday."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DayName) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}

// Eval implements the sql.Expression interface.
func (d *DayName) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	dt, ok := val.(time.Time)
	if !ok {
		return nil, nil
	}
	if types.ZeroTime.Equal(dt) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New("datetime", dt).Error())
		return nil, nil
	}
	return dt.Weekday().String(), nil
}

// WithChildren implements the sql.Expression interface.
func (d *DayName) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayName(ctx, children[0]), nil
}
