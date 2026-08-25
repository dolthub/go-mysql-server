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
	"io"
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

// TestInsertIgnoreModeErrorScopes verifies the error classes suppressed by each insert-ignore mode.
func TestInsertIgnoreModeErrorScopes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	row := sql.Row{1}
	fkErr := sql.ErrForeignKeyChildViolation.New("fk", "child", "parent", "1")
	duplicateErr := sql.ErrPrimaryKeyViolation.New()

	t.Run("MySQL INSERT IGNORE suppresses foreign key errors", func(t *testing.T) {
		iter := &insertIter{ignore: true}
		_, ok := iter.ignoreOrClose(ctx, row, fkErr).(sql.IgnorableError)
		require.True(t, ok)
	})

	t.Run("duplicate-key-only mode returns foreign key errors", func(t *testing.T) {
		iter := &insertIter{ignore: true, ignoreMode: sql.InsertIgnoreModeDuplicateKeysOnly}
		_, ok := iter.ignoreOrClose(ctx, row, fkErr).(sql.WrappedInsertError)
		require.True(t, ok)
	})

	t.Run("duplicate-key-only mode suppresses duplicate key errors", func(t *testing.T) {
		iter := &insertIter{ignore: true, ignoreMode: sql.InsertIgnoreModeDuplicateKeysOnly}
		_, ok := iter.ignoreOrClose(ctx, row, duplicateErr).(sql.IgnorableError)
		require.True(t, ok)
	})
}

// TestInsertIgnoreTarget verifies that only conflicts on the selected unique key are suppressed.
func TestInsertIgnoreTarget(t *testing.T) {
	ctx := sql.NewEmptyContext()
	iter := &insertIter{
		schema:       sql.Schema{{Name: "id", Source: "t", Type: types.Int64, PrimaryKey: true}, {Name: "u", Source: "t", Type: types.Int64}},
		ignore:       true,
		ignoreMode:   sql.InsertIgnoreModeDuplicateKeysOnly,
		ignoreTarget: []string{"id"},
	}

	t.Run("matching target is ignored", func(t *testing.T) {
		err := sql.NewUniqueKeyErr("id", true, sql.Row{int64(1), int64(10)})
		_, ok := iter.ignoreOrClose(ctx, sql.Row{int64(1), int64(11)}, err).(sql.IgnorableError)
		require.True(t, ok)
	})
	t.Run("different unique key is returned", func(t *testing.T) {
		err := sql.NewUniqueKeyErr("u", false, sql.Row{int64(1), int64(10)})
		_, ok := iter.ignoreOrClose(ctx, sql.Row{int64(2), int64(10)}, err).(sql.WrappedInsertError)
		require.True(t, ok)
	})
	t.Run("crossed conflict probes target independently", func(t *testing.T) {
		db := memory.NewDatabase("db")
		ctx := newContext(memory.NewDBProvider(db))
		table := memory.NewTable(ctx, db, "t", sql.NewPrimaryKeySchema(iter.schema), nil)
		require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
			Name:       "u_idx",
			Columns:    []sql.IndexColumn{{Name: "u"}},
			Constraint: sql.IndexConstraint_Unique,
		}))
		inserter := table.Inserter(ctx)
		require.NoError(t, inserter.Insert(ctx, sql.Row{int64(1), int64(10)}))
		require.NoError(t, inserter.Insert(ctx, sql.Row{int64(2), int64(20)}))

		iter.inserter = inserter
		iter.ignoreTarget = []string{"u"}
		err := sql.NewUniqueKeyErr("id", true, sql.Row{int64(1), int64(10)})
		_, ok := iter.ignoreOrClose(ctx, sql.Row{int64(1), int64(20)}, err).(sql.IgnorableError)
		require.True(t, ok)
		require.NoError(t, inserter.Close(ctx))
	})
}

// TestInsertIgnoreModes verifies check-constraint handling for MySQL and duplicate-only modes.
func TestInsertIgnoreModes(t *testing.T) {
	tests := []struct {
		name string
		mode sql.InsertIgnoreMode
	}{
		{name: "MySQL INSERT IGNORE suppresses check violations"},
		{name: "duplicate-key-only mode returns check violations", mode: sql.InsertIgnoreModeDuplicateKeysOnly},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memory.NewDatabase("db")
			ctx := newContext(memory.NewDBProvider(db))
			table := memory.NewTable(ctx, db.BaseDatabase, "t", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "id", Source: "t", Type: types.Int64, PrimaryKey: true},
			}), nil)
			insert := plan.NewInsertInto(
				sql.UnresolvedDatabase(""),
				plan.NewResolvedTable(table, db, nil),
				plan.NewValues([][]sql.Expression{{expression.NewLiteral(int64(1), types.Int64)}}),
				false,
				[]string{"id"},
				nil,
				true,
			)
			insert.IgnoreMode = tt.mode
			insert = insert.WithChecks(sql.CheckConstraints{{
				Name:     "positive",
				Expr:     expression.NewLiteral(false, types.Boolean),
				Enforced: true,
			}}).(*plan.InsertInto)

			iter, err := DefaultBuilder.Build(ctx, insert, nil)
			require.NoError(t, err)
			_, err = iter.Next(ctx)
			if tt.mode == sql.InsertIgnoreModeDuplicateKeysOnly {
				wrappedErr, ok := err.(sql.WrappedInsertError)
				require.True(t, ok)
				require.True(t, sql.ErrCheckConstraintViolated.Is(wrappedErr.Cause))
				require.Error(t, iter.Close(ctx))
			} else {
				_, ok := err.(sql.IgnorableError)
				require.True(t, ok)
				require.NoError(t, iter.Close(ctx))
			}
		})
	}
}

// TestInsertDuplicateKeysOnlyReturnsConversionErrors verifies that PostgreSQL-style ignores do not hide conversion failures.
func TestInsertDuplicateKeysOnlyReturnsConversionErrors(t *testing.T) {
	db := memory.NewDatabase("db")
	ctx := newContext(memory.NewDBProvider(db))
	table := memory.NewTable(ctx, db.BaseDatabase, "t", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Source: "t", Type: types.Int8, PrimaryKey: true},
	}), nil)

	tests := []struct {
		name  string
		value any
	}{
		{name: "conversion", value: "not a number"},
		{name: "out of range", value: int64(128)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newContext(memory.NewDBProvider(db))
			insert := plan.NewInsertInto(
				sql.UnresolvedDatabase(""),
				plan.NewResolvedTable(table, db, nil),
				plan.NewValues([][]sql.Expression{{expression.NewLiteral(tt.value, types.LongText)}}),
				false, []string{"id"}, nil, true,
			)
			insert.IgnoreMode = sql.InsertIgnoreModeDuplicateKeysOnly

			iter, err := DefaultBuilder.Build(ctx, insert, nil)
			require.NoError(t, err)
			_, err = iter.Next(ctx)
			require.Error(t, err)
			require.Zero(t, ctx.WarningCount())
			require.Error(t, iter.Close(ctx))
		})
	}
}

// TestInsertIgnoreModeNullability verifies mode-specific handling of nulls in non-nullable columns.
func TestInsertIgnoreModeNullability(t *testing.T) {
	tests := []struct {
		name      string
		mode      sql.InsertIgnoreMode
		wantError bool
	}{
		{name: "MySQL mode substitutes zero value"},
		{name: "duplicate-key-only mode returns not null error", mode: sql.InsertIgnoreModeDuplicateKeysOnly, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			inserter := &recordingInserter{}
			iter := &insertIter{
				schema:     sql.Schema{{Name: "id", Type: types.Int64, PrimaryKey: true}},
				inserter:   inserter,
				rowSource:  sql.RowsToRowIter(sql.Row{nil}),
				ignore:     true,
				ignoreMode: tt.mode,
			}

			row, err := iter.Next(ctx)
			if tt.wantError {
				require.Error(t, err)
				wrapped, ok := err.(sql.WrappedInsertError)
				require.True(t, ok)
				require.True(t, sql.ErrInsertIntoNonNullableProvidedNull.Is(wrapped.Cause))
			} else {
				require.NoError(t, err)
				require.Equal(t, sql.Row{int64(0)}, row)
				require.Equal(t, uint16(1), ctx.WarningCount())
			}
		})
	}
}

// TestInsertDuplicateKeysOnlyReturnsNotNullErrorBeforeDuplicate verifies validation precedes duplicate probing.
func TestInsertDuplicateKeysOnlyReturnsNotNullErrorBeforeDuplicate(t *testing.T) {
	ctx := sql.NewEmptyContext()
	inserter := &recordingInserter{insertErr: sql.ErrPrimaryKeyViolation.New()}
	iter := &insertIter{
		schema:     sql.Schema{{Name: "id", Type: types.Int64, PrimaryKey: true}},
		inserter:   inserter,
		rowSource:  sql.RowsToRowIter(sql.Row{nil}),
		ignore:     true,
		ignoreMode: sql.InsertIgnoreModeDuplicateKeysOnly,
	}

	_, err := iter.Next(ctx)
	require.Error(t, err)
	wrapped, ok := err.(sql.WrappedInsertError)
	require.True(t, ok)
	require.True(t, sql.ErrInsertIntoNonNullableProvidedNull.Is(wrapped.Cause))
	require.Zero(t, inserter.insertCalls, "invalid rows must be rejected before duplicate detection")
}

// TestInsertDuplicateKeysOnlyStatementAtomicity verifies that a fatal row discards earlier statement edits.
func TestInsertDuplicateKeysOnlyStatementAtomicity(t *testing.T) {
	ctx := sql.NewEmptyContext()
	inserter := &recordingInserter{}
	insert := &insertIter{
		schema:     sql.Schema{{Name: "id", Type: types.Int64, PrimaryKey: true}},
		inserter:   inserter,
		rowSource:  sql.RowsToRowIter(sql.Row{int64(1)}, sql.Row{nil}),
		ignore:     true,
		ignoreMode: sql.InsertIgnoreModeDuplicateKeysOnly,
	}
	iter := plan.NewTableEditorIter(insert, inserter)

	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, sql.Row{int64(1)}, row)
	_, err = iter.Next(ctx)
	require.Error(t, err)
	require.Error(t, iter.Close(ctx))
	require.Empty(t, inserter.rows, "a later invalid row must roll back the whole statement")
}

// TestInsertDuplicateKeysOnlyPreflightsConflicts verifies ignored rows cause no editor side effects.
func TestInsertDuplicateKeysOnlyPreflightsConflicts(t *testing.T) {
	for _, tt := range []struct {
		name   string
		target []string
	}{
		{name: "explicit target", target: []string{"id"}},
		{name: "empty target"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			inserter := &recordingInserter{conflict: true}
			iter := &insertIter{
				schema:       sql.Schema{{Name: "id", Type: types.Int64, PrimaryKey: true}},
				inserter:     inserter,
				rowSource:    sql.RowsToRowIter(sql.Row{int64(1)}),
				ignore:       true,
				ignoreMode:   sql.InsertIgnoreModeDuplicateKeysOnly,
				ignoreTarget: tt.target,
			}

			_, err := iter.Next(ctx)
			_, ok := err.(sql.IgnorableError)
			require.True(t, ok)
			require.Zero(t, inserter.insertCalls, "preflighted conflicts must not reach a side-effecting editor")
			require.Equal(t, tt.target, inserter.checkedTarget)
		})
	}
}

// TestInsertDuplicateKeysOnlyReturningSkipsConflicts verifies RETURNING emits only inserted rows.
func TestInsertDuplicateKeysOnlyReturningSkipsConflicts(t *testing.T) {
	ctx := sql.NewEmptyContext()
	inserter := &recordingInserter{conflictingRows: map[int64]struct{}{1: {}}}
	iter := &insertIter{
		schema:       sql.Schema{{Name: "id", Type: types.Int64, PrimaryKey: true}},
		inserter:     inserter,
		rowSource:    sql.RowsToRowIter(sql.Row{int64(1)}, sql.Row{int64(2)}),
		ignore:       true,
		ignoreMode:   sql.InsertIgnoreModeDuplicateKeysOnly,
		returnExprs:  []sql.Expression{expression.NewGetField(0, types.Int64, "id", false)},
		returnSchema: sql.Schema{{Name: "id", Type: types.Int64}},
	}

	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, sql.Row{int64(2)}, row)
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []sql.Row{{int64(2)}}, inserter.rows)
}

type recordingInserter struct {
	rows            []sql.Row
	checkpoint      []sql.Row
	insertErr       error
	insertCalls     int
	conflict        bool
	conflictingRows map[int64]struct{}
	checkedTarget   []string
}

// HasUniqueKeyConflict implements sql.UniqueKeyConflictCheckingRowInserter.
func (r *recordingInserter) HasUniqueKeyConflict(_ *sql.Context, row sql.Row, columns []string) (bool, error) {
	r.checkedTarget = append([]string(nil), columns...)
	if len(r.conflictingRows) > 0 {
		_, ok := r.conflictingRows[row[0].(int64)]
		return ok, nil
	}
	return r.conflict, nil
}

// StatementBegin implements sql.RowInserter.
func (r *recordingInserter) StatementBegin(*sql.Context) {
	r.checkpoint = append([]sql.Row(nil), r.rows...)
}

// DiscardChanges implements sql.RowInserter.
func (r *recordingInserter) DiscardChanges(*sql.Context, error) error {
	r.rows = append([]sql.Row(nil), r.checkpoint...)
	return nil
}

// StatementComplete implements sql.RowInserter.
func (r *recordingInserter) StatementComplete(*sql.Context) error { return nil }

// Insert implements sql.RowInserter.
func (r *recordingInserter) Insert(_ *sql.Context, row sql.Row) error {
	r.insertCalls++
	if r.insertErr != nil {
		return r.insertErr
	}
	r.rows = append(r.rows, row.Copy())
	return nil
}

// Close implements sql.RowInserter.
func (r *recordingInserter) Close(*sql.Context) error { return nil }

var _ sql.RowInserter = (*recordingInserter)(nil)
var _ sql.UniqueKeyConflictCheckingRowInserter = (*recordingInserter)(nil)
