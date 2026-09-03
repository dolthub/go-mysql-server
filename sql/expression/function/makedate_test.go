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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestMakeDate(t *testing.T) {
	tests := []struct {
		name string
		year any
		day  any
		exp  any
	}{
		{"standard date", 2021, 31, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC)},
		{"month rollover", 2021, 32, time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"end of year", 2021, 365, time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"leap year day 366", 2020, 366, time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"day 366 rollover", 2021, 366, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"day 0 null", 2021, 0, nil},
		{"negative day null", 2021, -1, nil},
		{"negative year null", -1, 1, nil},
		{"year 0 maps to 2000", 0, 1, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"two-digit year 03", 3, 1, time.Date(2003, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"two-digit year 70", 70, 1, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"two-digit year 99", 99, 1, time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"three-digit year 100", 100, 1, time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"max date 9999-12-31", 9999, 365, time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"year 9999 overflow", 9999, 366, nil},
		{"null year", nil, 1, nil},
		{"null day", 2021, nil, nil},
		{"string args coerced", "2021", "31", time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC)},
		{"year exceeds 9999", 11111111, 1, nil},
		{"float overflow day", 1, 8.381922e+307, nil},
		{"three-digit string year", "111", "1", time.Date(111, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"large negative year", -3030, 19, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f := NewMakeDate(ctx,
				expression.NewLiteral(tt.year, types.ApproximateTypeFromValue(tt.year)),
				expression.NewLiteral(tt.day, types.ApproximateTypeFromValue(tt.day)),
			)
			res, err := f.Eval(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, tt.exp, res)
		})
	}
}
