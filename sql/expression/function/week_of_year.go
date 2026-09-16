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

// WeekOfYear implements the weekofyear function
type WeekOfYear struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*WeekOfYear)(nil)
var _ sql.CollationCoercible = (*WeekOfYear)(nil)

// NewWeekOfYear creates a new WeekOfYear UDF.
func NewWeekOfYear(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &WeekOfYear{NewUnaryDatetimeFunc(arg, "WEEKOFYEAR", types.Uint64)}
}

// Description implements sql.FunctionExpression
func (m *WeekOfYear) Description() string {
	return "returns the calendar week of the date (1-53)."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*WeekOfYear) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the sql.Expression interface.
func (m *WeekOfYear) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := m.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	switch v := val.(type) {
	case time.Time:
		if v.Equal(types.ZeroTime) {
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(val).Error())
			return nil, nil
		}
		_, wk := v.ISOWeek()
		return wk, nil
	case nil:
		return nil, nil
	default:
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(val).Error())
		return nil, nil
	}
}

// WithChildren implements the sql.Expression interface.
func (m *WeekOfYear) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewWeekOfYear(ctx, children[0]), nil
}
