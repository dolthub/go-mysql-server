// Copyright 2020-2026 Dolthub, Inc.
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
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Time is a function takes the Time part out from a datetime expression.
type Time struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Time)(nil)
var _ sql.CollationCoercible = (*Time)(nil)

// NewTime returns a new Date node.
func NewTime(ctx *sql.Context, time sql.Expression) sql.Expression {
	return &Time{expression.UnaryExpressionStub{Child: time}}
}

// FunctionName implements sql.FunctionExpression
func (t *Time) FunctionName() string {
	return "time"
}

// Description implements sql.FunctionExpression
func (t *Time) Description() string {
	return "extracts the time part of a time or datetime expression and returns it as a string"
}

// String implements the sql.Expression interface.
func (t *Time) String() string {
	return fmt.Sprintf("TIME(%s)", t.Child)
}

// Type implements the Expression interface.
func (t *Time) Type(ctx *sql.Context) sql.Type {
	if t.Child == nil {
		return types.TimeMaxPrecision
	}
	childType := t.Child.Type(ctx)
	if tt, ok := childType.(types.TimeType); ok {
		return types.MustCreateTimespanType(tt.Precision())
	}
	if dt, ok := childType.(sql.DatetimeType); ok {
		if types.IsDateType(childType) {
			return types.Time
		}
		return types.MustCreateTimespanType(dt.Precision())
	}
	if dec, ok := childType.(sql.DecimalType); ok {
		scale := int(dec.Scale())
		if scale < 0 {
			scale = 0
		} else if scale > types.MaxDatetimePrecision {
			scale = types.MaxDatetimePrecision
		}
		return types.MustCreateTimespanType(scale)
	}
	if types.IsInteger(childType) {
		return types.Time
	}
	if lit, ok := t.Child.(*expression.Literal); ok {
		if s, ok := lit.Val.(string); ok {
			return types.MustCreateTimespanType(inferTimePrecisionFromString(s))
		}
		if b, ok := lit.Val.([]byte); ok {
			return types.MustCreateTimespanType(inferTimePrecisionFromString(string(b)))
		}
	}
	return types.TimeMaxPrecision
}

func inferTimePrecisionFromString(s string) int {
	lastDot := strings.LastIndex(s, ".")
	if lastDot == -1 {
		return 0
	}
	frac := s[lastDot+1:]
	digits := 0
	for i := 0; i < len(frac); i++ {
		if frac[i] >= '0' && frac[i] <= '9' {
			digits++
		} else {
			break
		}
	}
	if digits > types.MaxDatetimePrecision {
		return types.MaxDatetimePrecision
	}
	return digits
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Time) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (t *Time) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	v, err := t.UnaryExpressionStub.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	// convert to date
	dateVal, _, err := types.DatetimeMaxPrecision.Convert(ctx, v)
	if err == nil {
		date := dateVal.(time.Time)
		h, m, s := date.Clock()
		us := date.Nanosecond() / 1000
		return types.Timespan(1000000*(3600*h+60*m+s) + us), nil
	}

	// convert to time
	val, _, err := types.TimeMaxPrecision.Convert(ctx, v)
	if err != nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		return nil, nil
	}
	return val, nil
}

// WithChildren implements the Expression interface.
func (t *Time) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewTime(ctx, children[0]), nil
}
