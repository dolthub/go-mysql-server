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
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSecToTime(t *testing.T) {
	for _, tt := range []struct {
		name      string
		arg       sql.Expression
		expected  any
		precision int
		warning   string
	}{
		{"null", expression.NewLiteral(nil, types.Null), nil, 6, ""},
		{"integer", expression.NewLiteral(int64(3661), types.Int64), types.Timespan(3661_000000), 0, ""},
		{"negative integer", expression.NewLiteral(int64(-1), types.Int64), types.Timespan(-1_000000), 0, ""},
		{"more than a day", expression.NewLiteral(int64(90000), types.Int64), types.Timespan(90000_000000), 0, ""},
		{"decimal", expression.NewLiteral(apd.New(125, -2), types.MustCreateDecimalType(3, 2)), types.Timespan(1_250000), 2, ""},
		{"negative decimal", expression.NewLiteral(apd.New(-5, -1), types.MustCreateDecimalType(1, 1)), types.Timespan(-500000), 1, ""},
		{"decimal rounded to microseconds", expression.NewLiteral(apd.New(1123456789, -9), types.MustCreateDecimalType(10, 9)), types.Timespan(1_123457), 6, ""},
		{"half microsecond rounds away from zero", expression.NewLiteral(apd.New(-5, -7), types.MustCreateDecimalType(7, 7)), types.Timespan(-1), 6, ""},
		{"float", expression.NewLiteral(1.5, types.Float64), types.Timespan(1_500000), 6, ""},
		{"string", expression.NewLiteral("3661", types.LongText), types.Timespan(3661_000000), 6, ""},
		{"fractional string", expression.NewLiteral("1.5", types.LongText), types.Timespan(1_500000), 6, ""},
		{"binary string", expression.NewLiteral([]byte("3661"), types.LongBlob), types.Timespan(3661_000000), 6, ""},
		{"JSON null", expression.NewLiteral(types.JSONDocument{Val: nil}, types.JSON), types.Timespan(0), 6, ""},
		{"invalid string", expression.NewLiteral("abc", types.LongText), types.Timespan(0), 6, "Truncated incorrect decimal(65,30) value: 'abc'"},
		{"largest time", expression.NewLiteral(int64(3020399), types.Int64), types.MaxTimespan, 0, ""},
		{"above largest time", expression.NewLiteral(int64(3020400), types.Int64), types.MaxTimespan, 0, "Truncated incorrect time value: '3020400'"},
		{"fraction above largest time", expression.NewLiteral(apd.New(30203995, -1), types.MustCreateDecimalType(8, 1)), types.MaxTimespan, 1, "Truncated incorrect time value: '3020399.5'"},
		{"below smallest time", expression.NewLiteral(int64(-3020400), types.Int64), types.MinTimespan, 0, "Truncated incorrect time value: '-3020400'"},
		{"max uint64", expression.NewLiteral(uint64(math.MaxUint64), types.Uint64), types.MaxTimespan, 0, "Truncated incorrect time value: '18446744073709551615'"},
		{"min int64", expression.NewLiteral(int64(math.MinInt64), types.Int64), types.MinTimespan, 0, "Truncated incorrect time value: '-9223372036854775808'"},
		{"huge float", expression.NewLiteral(-1e30, types.Float64), types.MinTimespan, 6, "Truncated incorrect time value: '-1000000000000000000000000000000'"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f := NewSecToTime(ctx, tt.arg)
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
