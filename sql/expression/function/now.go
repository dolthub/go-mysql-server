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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// maxCurrTimestampPrecision is the largest fractional seconds precision the current timestamp functions accept.
const maxCurrTimestampPrecision = 6

// Now is a function that returns the current time.
type Now struct {
	// prec stores the requested precision for fractional seconds.
	prec sql.Expression
	// alwaysUseExactTime controls whether the NOW() function gets the current time, or
	// uses a cached value that records the starting time of the query. By default, a
	// cached time is used, but in some cases (such as the SYSDATE() function), the func
	// needs to always return the exact current time of each function invocation().
	alwaysUseExactTime bool
}

// IsNonDeterministic implements the sql.NonDeterministicExpression interface.
func (n *Now) IsNonDeterministic() bool {
	return true
}

var _ sql.FunctionExpression = (*Now)(nil)
var _ sql.CollationCoercible = (*Now)(nil)

// NewNow returns a new Now node.
func NewNow(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	n := &Now{}
	// parser should make it impossible to pass in more than one argument
	if len(args) > 0 {
		n.prec = args[0]
	}
	return n, nil
}

// subSecondPrecision returns the fractional seconds of |t| truncated to |precision| digits, including the leading decimal point.
func subSecondPrecision(t time.Time, precision int) string {
	if precision == 0 {
		return ""
	}

	s := fmt.Sprintf(".%09d", t.Nanosecond())
	return s[:precision+1]
}

// fractionOfSecString returns the fractional seconds of |t| in microseconds, including the leading decimal point and with trailing zeros removed.
func fractionOfSecString(t time.Time) string {
	s := fmt.Sprintf("%09d", t.Nanosecond())
	s = s[:6]

	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != '0' {
			break
		}

		s = s[:i]
	}

	if len(s) == 0 {
		return ""
	}

	return "." + s
}

// Name implements sql.FunctionExpression
func (n *Now) Name() string {
	return "now"
}

// Description implements sql.FunctionExpression
func (n *Now) Description() string {
	return "returns the current timestamp."
}

// Type implements the sql.Expression interface.
func (n *Now) Type(ctx *sql.Context) sql.Type {
	// TODO: precision
	if n.prec == nil {
		return types.Datetime
	}
	return types.DatetimeMaxPrecision
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Now) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// String implements the sql.Expression interface.
func (n *Now) String() string {
	name := "NOW"
	if n.alwaysUseExactTime {
		name = "SYSDATE"
	}
	if n.prec == nil {
		return name + "()"
	}

	return fmt.Sprintf("%s(%s)", name, n.prec.String())
}

// IsNullable implements the sql.Expression interface.
func (n *Now) IsNullable(ctx *sql.Context) bool { return false }

// Resolved implements the sql.Expression interface.
func (n *Now) Resolved() bool {
	if n.prec == nil {
		return true
	}
	return n.prec.Resolved()
}

// Children implements the sql.Expression interface.
func (n *Now) Children() []sql.Expression {
	if n.prec == nil {
		return nil
	}
	return []sql.Expression{n.prec}
}

// Eval implements the sql.Expression interface.
func (n *Now) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	// Cannot evaluate with nil context
	if ctx == nil {
		return nil, fmt.Errorf("cannot Eval Now with nil context")
	}

	// The timestamp must be in the session time zone
	sessionTimeZone, err := SessionTimeZone(ctx)
	if err != nil {
		return nil, err
	}

	// For NOW(), we use the cached QueryTime, so that all NOW() calls in a query return the same value.
	// SYSDATE() requires that we use the *exact* current time, and not use the cached version.
	currentTime := ctx.QueryTime()
	if n.alwaysUseExactTime {
		currentTime = sql.Now()
	}

	// If no arguments, just return with 0 precision
	// The way the parser is implemented 0 should always be passed in; have this here just in case
	if n.prec == nil {
		t, ok := sql.ConvertTimeZone(currentTime, sql.SystemTimezoneOffset(), sessionTimeZone)
		if !ok {
			return nil, sql.ErrInvalidTimeZone.New(sessionTimeZone)
		}
		tt := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
		return tt, nil
	}

	// Should syntax error before this; check anyway
	if types.IsNull(ctx, n.prec) {
		return nil, ErrTimeUnexpectedlyNil.New(n.Name())
	}

	// Evaluate precision
	prec, err := n.prec.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Should syntax error before this; check anyway
	if prec == nil {
		return nil, ErrTimeUnexpectedlyNil.New(n.Name())
	}

	// Must receive integer
	// Should syntax error before this; check anyway
	fsp, ok := types.CoalesceInt(prec)
	if !ok {
		return nil, sql.ErrInvalidArgumentType.New(n.Name())
	}

	// Parse and return answer
	if fsp > maxCurrTimestampPrecision {
		return nil, ErrTooHighPrecision.New(fsp, n.Name(), maxCurrTimestampPrecision)
	} else if fsp < 0 {
		// Should syntax error before this; check anyway
		return nil, sql.ErrInvalidArgumentType.New(n.Name())
	}

	// Get the timestamp
	t, ok := sql.ConvertTimeZone(currentTime, sql.SystemTimezoneOffset(), sessionTimeZone)
	if !ok {
		return nil, sql.ErrInvalidTimeZone.New(sessionTimeZone)
	}

	// Calculate precision
	precision := 1
	for i := 0; i < 9-fsp; i++ {
		precision *= 10
	}

	// Round down nano based on precision
	nano := precision * (t.Nanosecond() / precision)

	// Generate a new timestamp
	tt := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), nano, time.UTC)

	return tt, nil
}

// WithChildren implements the Expression interface.
func (n *Now) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	ret, err := NewNow(ctx, children...)
	if err != nil {
		return nil, err
	}
	ret.(*Now).alwaysUseExactTime = n.alwaysUseExactTime
	return ret, nil
}
