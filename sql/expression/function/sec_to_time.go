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
	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SecToTime implements the SEC_TO_TIME function
type SecToTime struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*SecToTime)(nil)
var _ sql.CollationCoercible = (*SecToTime)(nil)

// NewSecToTime creates a new SecToTime expression.
func NewSecToTime(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &SecToTime{
		UnaryFunc: NewUnaryFunc(arg, "SEC_TO_TIME", types.TimeMaxPrecision),
	}
}

// Description implements sql.FunctionExpression
func (s *SecToTime) Description() string {
	return "converts seconds to a time value in 'hh:mm:ss' format."
}

// Type implements the sql.Expression interface.
func (s *SecToTime) Type(ctx *sql.Context) sql.Type {
	return types.MustCreateTimespanType(secondsPrecision(ctx, s.Child))
}

// IsNullable implements the sql.Expression interface.
func (s *SecToTime) IsNullable(ctx *sql.Context) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*SecToTime) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the sql.Expression interface.
func (s *SecToTime) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	secs, ok, err := evalDecimal(ctx, s.Child, row)
	if err != nil || !ok {
		return nil, err
	}
	res, clipped := secondsToTimespan(secs)
	if clipped {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New("time", secs.Text('f')).Error())
	}
	return res, nil
}

// WithChildren implements the sql.Expression interface.
func (s *SecToTime) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSecToTime(ctx, children[0]), nil
}

// secondsPrecision returns the fractional seconds precision of a TIME value built from a number of seconds given by
// |arg|: 0 for integers, the scale for decimals, and the maximum precision for anything else.
func secondsPrecision(ctx *sql.Context, arg sql.Expression) int {
	switch t := arg.Type(ctx).(type) {
	case sql.DecimalType:
		return min(int(t.Scale()), types.MaxDatetimePrecision)
	case sql.NumberType:
		if t.IsFloat() {
			return types.MaxDatetimePrecision
		}
		return 0
	default:
		return types.MaxDatetimePrecision
	}
}

// evalDecimal evaluates |expr| against |row| and converts the result to a decimal, recording a warning on |ctx| for
// values that are truncated during the conversion. It returns (nil, false, nil) if the evaluated value is SQL NULL.
func evalDecimal(ctx *sql.Context, expr sql.Expression, row sql.Row) (*apd.Decimal, bool, error) {
	v, err := expr.Eval(ctx, row)
	if err != nil || v == nil {
		return nil, false, err
	}
	var d any
	// Blob type here is not interpreted as a hexadecimal
	if typ := expr.Type(ctx); types.IsBlobType(typ) {
		d, _, err = types.InternalDecimalType.Convert(ctx, v)
	} else {
		d, _, err = types.TypeAwareConversion(ctx, v, typ, types.InternalDecimalType)
	}
	if err != nil {
		if !sql.ErrTruncatedIncorrect.Is(err) {
			return nil, false, err
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}
	dec, ok := d.(*apd.Decimal)
	if !ok {
		// A JSON null converts to nil, which MySQL reads as 0.
		dec = new(apd.Decimal)
	}
	return dec, true, nil
}

// secondsToTimespan converts |secs| to a Timespan, rounded to microseconds. Values outside the TIME range are clipped
// to the nearest bound, which is reported by the returned bool.
func secondsToTimespan(secs *apd.Decimal) (types.Timespan, bool) {
	micros := new(apd.Decimal).Set(secs)
	micros.Exponent += 6
	rounded, err := sql.DecimalRound(micros, 0)
	if err == nil {
		if v, err := rounded.Int64(); err == nil && v >= int64(types.MinTimespan) && v <= int64(types.MaxTimespan) {
			return types.Timespan(v), false
		}
	}
	if secs.Negative {
		return types.MinTimespan, true
	}
	return types.MaxTimespan, true
}
