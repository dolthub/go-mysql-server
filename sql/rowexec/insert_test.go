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
			// This diverges from MySQL because NaN values are okay in Postgres
			name:     "inserting NaN into float results is okay",
			colType:  types.Float64,
			value:    math.NaN(),
			expected: math.NaN(),
		},
		{
			name:    "inserting NaN into int results in error",
			colType: types.Int64,
			value:   math.NaN(),
			err:     true,
		},
		{
			name:    "inserting NaN into unsigned int results in error",
			colType: types.Uint64,
			value:   math.NaN(),
			err:     true,
		},
		{
			name:     "inserting NaN into Decimal is okay",
			colType:  types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale),
			value:    math.NaN(),
			expected: types.DecimalFromFloat64(math.NaN()),
		},
		{
			// This diverges from MySQL because Infinity values are okay in Postgres
			name:     "inserting Infinity into float is okay",
			colType:  types.Float64,
			value:    math.Inf(1),
			expected: math.Inf(1),
		},
		{
			name:    "inserting Infinity into int results in error",
			colType: types.Int64,
			value:   math.Inf(1),
			err:     true,
		},
		{
			name:    "inserting Infinity into unsigned int results in error",
			colType: types.Uint64,
			value:   math.Inf(1),
			err:     true,
		},
		{
			// TODO: Postgres possibly allows Inf values for decimals (documentation unclear) but shopspring/decimal
			//  does not
			name:    "inserting Infinity into Decimal results in error",
			colType: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale),
			value:   math.Inf(1),
			err:     true,
		},
		{
			// This diverges from MySQL because Infinity values are okay in Postgres
			name:     "inserting negative Infinity into float results is okay",
			colType:  types.Float64,
			value:    math.Inf(-1),
			expected: math.Inf(-1),
		},
		{
			name:    "inserting negative Infinity into int results in error",
			colType: types.Int64,
			value:   math.Inf(-1),
			err:     true,
		},
		{
			name:    "inserting negative Infinity into unsigned int results in error",
			colType: types.Uint64,
			value:   math.Inf(-1),
			err:     true,
		},
		{
			// TODO: Postgres possibly allows Inf values for decimals (documentation unclear) but shopspring/decimal
			//  does not
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

			table := memory.NewTable(ctx, db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c1", Source: "foo", Type: tc.colType},
			}), nil)

			insertPlan := plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewResolvedTable(table, nil, nil), plan.NewValues([][]sql.Expression{{
				expression.NewLiteral(tc.value, tc.valueType),
			}}), false, []string{"c1"}, nil, tc.ignore)

			ri, err := DefaultBuilder.Build(ctx, insertPlan, nil)
			require.NoError(t, err)

			row, err := ri.Next(ctx)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				require.True(t, len(row) == 1)
				// math.NaN != math.NaN so using math.IsNaN is the only way math.NaN results can be checked
				if expectedFloat64, expectedIsFloat64 := tc.expected.(float64); expectedIsFloat64 && math.IsNaN(expectedFloat64) {
					resultFloat64, resultIsFloat64 := row[0].(float64)
					require.True(t, resultIsFloat64)
					require.True(t, math.IsNaN(resultFloat64))
				} else {
					require.Equal(t, sql.Row{tc.expected}, row)
				}

				var warningCnt int
				if tc.warning {
					warningCnt = 1
				}
				require.Equal(t, ctx.WarningCount(), uint16(warningCnt))
			}
		})
	}
}

// TestInsertOnDuplicateWhereLargeBatch verifies large filtered batches avoid excessive memory use and stack exhaustion.
func TestInsertOnDuplicateWhereLargeBatch(t *testing.T) {
	db := memory.NewDatabase("foo")
	provider := memory.NewDBProvider(db)
	ctx := newContext(provider)
	table := memory.NewTable(ctx, db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Source: "foo", Type: types.Int64, PrimaryKey: true},
	}), nil)

	insertOne := plan.NewInsertInto(db, plan.NewResolvedTable(table, nil, nil), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral(int64(1), types.Int64),
	}}), false, []string{"c1"}, nil, false)
	iter, err := DefaultBuilder.Build(ctx, insertOne, nil)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)

	const conflictCount = 100_000
	values := make([][]sql.Expression, conflictCount)
	for idx := range values {
		values[idx] = []sql.Expression{expression.NewLiteral(int64(1), types.Int64)}
	}
	setField := expression.NewSetField(
		expression.NewGetField(0, types.Int64, "c1", false),
		expression.NewGetField(1, types.Int64, "c1", false),
	)
	insertConflicts := plan.NewInsertInto(
		db,
		plan.NewResolvedTable(table, nil, nil),
		plan.NewValues(values),
		false,
		[]string{"c1"},
		plan.NewUpdateExprs([]sql.Expression{setField}, 1),
		false,
	)
	insertConflicts.OnDupWhere = expression.NewLiteral(false, types.Boolean)
	iter, err = DefaultBuilder.Build(ctx, insertConflicts, nil)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestInsertOnDuplicateReturning verifies that conflict updates project the updated row.
func TestInsertOnDuplicateReturning(t *testing.T) {
	db := memory.NewDatabase("foo")
	provider := memory.NewDBProvider(db)
	ctx := newContext(provider)
	table := memory.NewTable(ctx, db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Source: "foo", Type: types.Int64, PrimaryKey: true},
		{Name: "c2", Source: "foo", Type: types.Int64},
	}), nil)

	insertOne := plan.NewInsertInto(db, plan.NewResolvedTable(table, nil, nil), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral(int64(1), types.Int64),
		expression.NewLiteral(int64(1), types.Int64),
	}}), false, []string{"c1", "c2"}, nil, false)
	iter, err := DefaultBuilder.Build(ctx, insertOne, nil)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)

	setField := expression.NewSetField(
		expression.NewGetField(1, types.Int64, "c2", false),
		expression.NewGetField(3, types.Int64, "c2", false),
	)
	insertConflict := plan.NewInsertInto(
		db,
		plan.NewResolvedTable(table, nil, nil),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(2), types.Int64),
		}}),
		false,
		[]string{"c1", "c2"},
		plan.NewUpdateExprs([]sql.Expression{setField}, 1),
		false,
	)
	insertConflict.Returning = []sql.Expression{expression.NewGetField(1, types.Int64, "c2", false)}
	iter, err = DefaultBuilder.Build(ctx, insertConflict, nil)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(2)}}, rows)
}

// TestOnDuplicateUpdateAffectedRows verifies MySQL and PostgreSQL duplicate-update counting policies.
func TestOnDuplicateUpdateAffectedRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	schema := sql.Schema{{Name: "c1", Type: types.Int64}}
	tests := []struct {
		name                string
		row                 sql.Row
		countUpdateAsOneRow bool
		expected            int
	}{
		{name: "MySQL changed update", row: sql.Row{int64(1), int64(2)}, expected: 2},
		{name: "MySQL unchanged update", row: sql.Row{int64(1), int64(1)}, expected: 0},
		{name: "PostgreSQL changed update", row: sql.Row{int64(1), int64(2)}, countUpdateAsOneRow: true, expected: 1},
		{name: "PostgreSQL unchanged update", row: sql.Row{int64(1), int64(1)}, countUpdateAsOneRow: true, expected: 1},
		{name: "PostgreSQL insert", row: sql.Row{int64(1)}, countUpdateAsOneRow: true, expected: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &onDuplicateUpdateHandler{schema: schema, countUpdateAsOneRow: test.countUpdateAsOneRow}
			require.NoError(t, handler.handleRowUpdate(ctx, test.row))
			require.Equal(t, test.expected, handler.rowsAffected)
		})
	}
}
