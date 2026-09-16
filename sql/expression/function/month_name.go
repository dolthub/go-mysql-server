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
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// MonthName implements the MONTHNAME function
type MonthName struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*MonthName)(nil)
var _ sql.CollationCoercible = (*MonthName)(nil)

// NewMonthName creates a new MonthName UDF.
func NewMonthName(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &MonthName{NewUnaryDatetimeFunc(arg, "MONTHNAME", types.Text)}
}

// Description implements sql.FunctionExpression
func (d *MonthName) Description() string {
	return "returns the name of the month."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*MonthName) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}

// IsNullable implements the Expression interface
func (d *MonthName) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements the sql.Expression interface.
func (d *MonthName) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	switch v := val.(type) {
	case time.Time:
		if !v.Equal(types.ZeroTime) {
			return v.Month().String(), nil
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New("datetime", v).Error())
	}
	return nil, nil
}

// WithChildren implements the sql.Expression interface.
func (d *MonthName) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewMonthName(ctx, children[0]), nil
}
