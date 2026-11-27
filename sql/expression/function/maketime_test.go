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
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestMakeTime(t *testing.T) {
	integer := func(v int64) sql.Expression { return expression.NewLiteral(v, types.Int64) }
	decimal := func(coeff int64, exp int32, precision, scale uint8) sql.Expression {
		return expression.NewLiteral(apd.New(coeff, exp), types.MustCreateDecimalType(precision, scale))
	}
	null := expression.NewLiteral(nil, types.Null)

	for _, tt := range []struct {
		name                 string
		hour, minute, second sql.Expression
		expected             any
		precision            int
		warning              string
	}{
		{"time", integer(1), integer(2), integer(3), types.Timespan(3723_000000), 0, ""},
		{"negative hour", integer(-1), integer(2), integer(3), types.Timespan(-3723_000000), 0, ""},
		{"more than a day", integer(25), integer(0), integer(0), types.Timespan(90000_000000), 0, ""},
		{"fractional seconds", integer(1), integer(2), decimal(35, -1, 2, 1), types.Timespan(3723_500000), 1, ""},
		{"seconds rounded to microseconds", integer(1), integer(2), decimal(31234567, -7, 8, 7), types.Timespan(3723_123457), 6, ""},
		{"rounding carries into the next minute", integer(1), integer(2), decimal(599999999, -7, 9, 7), types.Timespan(3780_000000), 6, ""},
		{"float seconds", integer(1), integer(2), expression.NewLiteral(3.0, types.Float64), types.Timespan(3723_000000), 6, ""},
		{"string arguments", expression.NewLiteral("1", types.LongText), expression.NewLiteral("2", types.LongText), expression.NewLiteral("3.25", types.LongText), types.Timespan(3723_250000), 6, ""},
		{"hour and minute are rounded", decimal(16, -1, 2, 1), decimal(24, -1, 2, 1), integer(3), types.Timespan(7323_000000), 0, ""},
		{"negative hour rounded to zero", decimal(-4, -1, 1, 1), integer(2), integer(3), types.Timespan(123_000000), 0, ""},
		{"string hour is truncated", expression.NewLiteral("1.6", types.LongText), integer(0), integer(0), types.Timespan(3600_000000), 0, "Truncated incorrect bigint value: '1.6'"},
		{"null hour", null, integer(1), integer(1), nil, 0, ""},
		{"null minute", integer(1), null, integer(1), nil, 0, ""},
		{"null second", integer(1), integer(1), null, nil, 6, ""},
		{"minute too large", integer(1), integer(60), integer(0), nil, 0, ""},
		{"minute rounded too large", integer(1), decimal(595, -1, 3, 1), integer(0), nil, 0, ""},
		{"negative minute", integer(1), integer(-1), integer(0), nil, 0, ""},
		{"huge minute", integer(1), expression.NewLiteral(1e30, types.Float64), integer(0), nil, 0, ""},
		{"second too large", integer(1), integer(2), integer(60), nil, 0, ""},
		{"negative second", integer(1), integer(2), integer(-1), nil, 0, ""},
		{"largest time", integer(838), integer(59), integer(59), types.MaxTimespan, 0, ""},
		{"hour too large", integer(839), integer(0), integer(0), types.MaxTimespan, 0, "Truncated incorrect time value: '839:00:00'"},
		{"hour too small", integer(-839), integer(0), integer(0), types.MinTimespan, 0, "Truncated incorrect time value: '-839:00:00'"},
		{"fraction above largest time", integer(838), integer(59), decimal(595, -1, 3, 1), types.MaxTimespan, 1, "Truncated incorrect time value: '838:59:59.5'"},
		{"single digit seconds in warning", integer(2147483648), integer(0), decimal(55, -1, 2, 1), types.MaxTimespan, 1, "Truncated incorrect time value: '2147483648:00:05.5'"},
		{"decimal seconds in warning are shown as given", integer(838), integer(59), decimal(599999999, -7, 9, 7), types.MaxTimespan, 6, "Truncated incorrect time value: '838:59:59.9999999'"},
		{"string seconds in warning use the result precision", integer(839), integer(0), expression.NewLiteral("5.00", types.LongText), types.MaxTimespan, 6, "Truncated incorrect time value: '839:00:05.000000'"},
		{"huge negative hour", integer(-1e18), integer(0), integer(0), types.MinTimespan, 0, "Truncated incorrect time value: '-1000000000000000000:00:00'"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f := NewMakeTime(ctx, tt.hour, tt.minute, tt.second)
			require.Equal(t, types.MustCreateTimespanType(tt.precision), f.Type(ctx))

			actual, err := f.Eval(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
			if tt.warning == "" {
				require.Empty(t, ctx.Warnings())
			} else {
				require.Len(t, ctx.Warnings(), 1)
				require.Equal(t, 1292, ctx.Warnings()[0].Code)
				require.Equal(t, tt.warning, ctx.Warnings()[0].Message)
			}
		})
	}
}
