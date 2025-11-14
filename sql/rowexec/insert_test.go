// Copyright 2022 Dolthub, Inc.
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

package rowexec

import (
	"math"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestInsert(t *testing.T) {
	testCases := []struct {
		name      string
		colType   sql.Type
		value     interface{}
		valueType sql.Type
		expected  interface{}
		warning   bool
		ignore    bool
		err       bool
	}{
		{
			name:      "inserting a string into a integer defaults to a 0 (with ignore)",
			colType:   types.Int64,
			value:     "dadasd",
			valueType: types.Text,
			expected:  int64(0),
			warning:   true,
			ignore:    true,
		},
		{
			name:      "string too long gets truncated (with ignore)",
			colType:   types.MustCreateStringWithDefaults(sqltypes.VarChar, 2),
			value:     "dadsa",
			valueType: types.Text,
			expected:  "da",
			warning:   true,
			ignore:    true,
		},
		{
			name:      "inserting a string into a datetime results in 0 time (with ignore)",
			colType:   types.Datetime,
			value:     "dadasd",
			valueType: types.Text,
			expected:  types.ZeroTime,
			warning:   true,
			ignore:    true,
		},
		{
			name:      "inserting a negative into an unsigned int results in 0 (with ignore)",
			colType:   types.Uint64,
			value:     int64(-1),
			expected:  uint64(1<<64 - 1),
			valueType: types.Uint64,
			warning:   true,
			ignore:    true,
		},
		{
			name:    "inserting NaN into float results in error",
			colType: types.Float64,
			value:   math.NaN(),
			err:     true,
		},
		{
			name:    "inserting NaN into int results in error",
			colType: types.Int64,
			value:   math.NaN(),
			err:     true,
		},
		{
			name:    "inserting NaN into Decimal results in error",
			colType: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale),
			value:   math.NaN(),
			err:     true,
		},
		{
			name:    "inserting Infinity into float results in error",
			colType: types.Float64,
			value:   math.Inf(1),
			err:     true,
		},
		{
			name:    "inserting Infinity into int results in error",
			colType: types.Int64,
			value:   math.Inf(1),
			err:     true,
		},
		{
			name:    "inserting Infinity into Decimal results in error",
			colType: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale),
			value:   math.Inf(1),
			err:     true,
		},
		{
			name:    "inserting negative Infinity into float results in error",
			colType: types.Float64,
			value:   math.Inf(-1),
			err:     true,
		},
		{
			name:    "inserting negative Infinity into int results in error",
			colType: types.Int64,
			value:   math.Inf(-1),
			err:     true,
		},
		{
			name:    "inserting negative Infinity into Decimal results in error",
			colType: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale),
			value:   math.Inf(-1),
			err:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := memory.NewDatabase("foo")
			pro := memory.NewDBProvider(db)
			ctx := newContext(pro)

			table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c1", Source: "foo", Type: tc.colType},
			}), nil)

			insertPlan := plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewResolvedTable(table, nil, nil), plan.NewValues([][]sql.Expression{{
				expression.NewLiteral(tc.value, tc.valueType),
			}}), false, []string{"c1"}, []sql.Expression{}, tc.ignore)

			ri, err := DefaultBuilder.Build(ctx, insertPlan, nil)
			require.NoError(t, err)

			row, err := ri.Next(ctx)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				require.Equal(t, sql.Row{tc.expected}, row)

				var warningCnt int
				if tc.warning {
					warningCnt = 1
				}
				require.Equal(t, ctx.WarningCount(), uint16(warningCnt))
			}
		})
	}
}
