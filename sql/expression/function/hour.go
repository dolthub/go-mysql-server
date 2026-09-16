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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Hour is a function that returns the hour of a date.
type Hour struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Hour)(nil)
var _ sql.CollationCoercible = (*Hour)(nil)

// NewHour creates a new Hour UDF.
func NewHour(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Hour{expression.UnaryExpressionStub{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (h *Hour) FunctionName() string {
	return "hour"
}

// Description implements sql.FunctionExpression
func (h *Hour) Description() string {
	return "returns the hours of the given date."
}

// String implements the sql.Expression interface.
func (h *Hour) String() string { return fmt.Sprintf("%s(%s)", h.FunctionName(), h.Child) }

// Type implements the Expression interface.
func (h *Hour) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Hour) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (h *Hour) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := h.Child.Eval(ctx, row)
	if err != nil || val == nil {
		return nil, err
	}
	timespan, err := types.Time.ConvertToTimespan(val)
	if err == nil {
		return hourFromTimespan(timespan), nil
	}
	val, _, err = types.DatetimeMaxPrecision.Convert(ctx, val)
	if err != nil {
		return nil, err
	}
	dt := val.(time.Time)
	return dt.Hour(), nil
}

// hourFromTimespan returns the number of whole hours in |timespan|, ignoring its sign.
func hourFromTimespan(timespan types.Timespan) int {
	microseconds := timespan.AsMicroseconds()
	if microseconds < 0 {
		microseconds = -microseconds
	}
	return int(microseconds / int64(time.Hour/time.Microsecond))
}

// WithChildren implements the Expression interface.
func (h *Hour) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewHour(ctx, children[0]), nil
}
