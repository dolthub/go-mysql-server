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
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestUpdateIgnoreConversions(t *testing.T) {
	testCases := []struct {
		name      string
		colType   sql.Type
		value     interface{}
		valueType sql.Type
		expected  interface{}
	}{
		{
			name:      "inserting a string into a integer defaults to a 0",
			colType:   types.Int64,
			value:     "dadasd",
			valueType: types.Text,
			expected:  int64(0),
		},
		{
			name:      "string too long gets truncated",
			colType:   types.MustCreateStringWithDefaults(sqltypes.VarChar, 2),
			value:     "dadsa",
			valueType: types.Text,
			expected:  "da",
		},
		{
			name:      "inserting a string into a datetime results in 0 time",
			colType:   types.Datetime,
			value:     "dadasd",
			valueType: types.Text,
			expected:  types.ZeroTime,
		},
		{
			name:      "inserting a negative into an unsigned int results in 0",
			colType:   types.Uint64,
			value:     int64(-1),
			expected:  uint64(1<<64 - 1),
			valueType: types.Uint64,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := memory.NewDatabase("foo")
			pro := memory.NewDBProvider(db)
			ctx := newContext(pro)

			sch := sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c1", Source: "foo", Type: tc.colType, Nullable: true},
			})
			table := memory.NewTable(ctx, db, "foo", sch, nil)

			err := table.Insert(ctx, sql.Row{nil})
			require.NoError(t, err)

			// Run the UPDATE IGNORE
			sf := expression.NewSetField(expression.NewGetField(0, tc.colType, "c1", true), expression.NewLiteral(tc.value, tc.valueType))
			updateExpressions := plan.NewUpdateExprs([]sql.Expression{sf}, 1)
			updatePlan := plan.NewUpdate(ctx, plan.NewResolvedTable(table, nil, nil), true, updateExpressions)

			ri, err := DefaultBuilder.Build(ctx, updatePlan, nil)
			require.NoError(t, err)

			_, err = sql.RowIterToRows(ctx, ri)
			require.NoError(t, err)

			// Run a SELECT to see the updated data
			selectPlan := plan.NewProject(ctx, []sql.Expression{
				expression.NewGetField(0, tc.colType, "c1", true),
			}, plan.NewResolvedTable(table, nil, nil))

			ri, err = DefaultBuilder.Build(ctx, selectPlan, nil)
			require.NoError(t, err)

			rows, err := sql.RowIterToRows(ctx, ri)
			require.NoError(t, err)

			require.Equal(t, 1, len(rows))
			require.Equal(t, tc.expected, rows[0][0])
		})
	}
}

// rowUpdateApplierFunc lets these tests exercise the public override through the builder.
type rowUpdateApplierFunc func(*sql.Context, *sql.UpdateExprs, sql.Schema, sql.Row, bool) (sql.Row, error)

func (f rowUpdateApplierFunc) ApplyRowUpdate(ctx *sql.Context, exprs *sql.UpdateExprs, schema sql.Schema, row sql.Row, ignore bool) (sql.Row, error) {
	return f(ctx, exprs, schema, row, ignore)
}

func TestUpdateExpressionApplierOverride(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("error=%v", fail), func(t *testing.T) {
			db := memory.NewDatabase("foo")
			ctx := newContext(memory.NewDBProvider(db))
			schema := sql.Schema{{Name: "a", Source: "t", Type: types.Int64}}
			table := memory.NewTable(ctx, db, "t", sql.NewPrimaryKeySchema(schema), nil)
			require.NoError(t, table.Insert(ctx, sql.Row{int64(1)}))
			field := expression.NewGetField(0, types.Int64, "a", false)
			exprs := sql.NewUpdateExprs([]sql.Expression{
				expression.NewSetField(field, expression.NewLiteral(int64(2), types.Int64)),
			}, 1)
			source := plan.NewUpdateSource(ctx, plan.NewResolvedTable(table, nil, nil), true, exprs)
			calls := 0
			failure := fmt.Errorf("applier failed")
			applier := rowUpdateApplierFunc(func(actualCtx *sql.Context, actualExprs *sql.UpdateExprs, actualSchema sql.Schema, row sql.Row, ignore bool) (sql.Row, error) {
				calls++
				require.Same(t, ctx, actualCtx)
				require.Same(t, exprs, actualExprs)
				require.Equal(t, schema, actualSchema)
				require.Equal(t, sql.Row{int64(1)}, row)
				require.True(t, ignore)
				if fail {
					return nil, failure
				}
				return sql.Row{int64(42)}, nil
			})
			builder := NewBuilder(nil, sql.EngineOverrides{UpdateExpressionApplier: applier})
			iter, err := builder.Build(ctx, source, nil)
			require.NoError(t, err)
			row, err := iter.Next(ctx)
			if fail {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
				// The override supplies the new half without changing the original half.
				require.Equal(t, sql.Row{int64(1), int64(42)}, row)
			}
			require.Equal(t, 1, calls)
			require.NoError(t, iter.Close(ctx))
		})
	}
}
