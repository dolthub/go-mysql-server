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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Day is a function that returns the day of a date.
type Day struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Day)(nil)
var _ sql.CollationCoercible = (*Day)(nil)

// NewDay creates a new Day UDF.
func NewDay(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Day{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "DAY", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (d *Day) Description() string {
	return "returns the day of the month (0-31)."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Day) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *Day) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	dt, ok := val.(time.Time)
	if !ok {
		return nil, nil
	}
	if types.ZeroTime.Equal(dt) {
		return 0, nil
	}
	return dt.Day(), nil
}

// WithChildren implements the Expression interface.
func (d *Day) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDay(ctx, children[0]), nil
}
