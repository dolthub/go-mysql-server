// Copyright 2023 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type updateIter struct {
	childIter sql.RowIter
	schema    sql.Schema
	updater   sql.RowUpdater
	checks    sql.CheckConstraints
	closed    bool
	ignore    bool
}

func (u *updateIter) Next(ctx *sql.Context) (sql.Row, error) {
	oldAndNewRow, err := u.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	oldRow, newRow := oldAndNewRow[:len(oldAndNewRow)/2], oldAndNewRow[len(oldAndNewRow)/2:]
	if equals, err := oldRow.Equals(newRow, u.schema); err == nil {
		if !equals {
			// apply check constraints
			for _, check := range u.checks {
				if !check.Enforced {
					continue
				}

				res, err := sql.EvaluateCondition(ctx, check.Expr, newRow)
				if err != nil {
					return nil, err
				}

				if sql.IsFalse(res) {
					return nil, u.ignoreOrError(ctx, newRow, sql.ErrCheckConstraintViolated.New(check.Name))
				}
			}

			err := u.validateNullability(ctx, newRow, u.schema)
			if err != nil {
				return nil, u.ignoreOrError(ctx, newRow, err)
			}

			err = u.updater.Update(ctx, oldRow, newRow)
			if err != nil {
				return nil, u.ignoreOrError(ctx, newRow, err)
			}
		}
	} else {
		return nil, err
	}

	return oldAndNewRow, nil
}

// Applies the update expressions given to the row given, returning the new resultant row. In the case that ignore is
// provided and there is a type conversion error, this function sets the value to the zero value as per the MySQL standard.
// TODO: a set of update expressions should probably be its own expression type with an Eval method that does this
func applyUpdateExpressionsWithIgnore(ctx *sql.Context, updateExprs []sql.Expression, tableSchema sql.Schema, row sql.Row, ignore bool) (sql.Row, error) {
	var ok bool
	prev := row
	for _, updateExpr := range updateExprs {
		val, err := updateExpr.Eval(ctx, prev)
		if err != nil {
			wtce, ok2 := err.(sql.WrappedTypeConversionError)
			if !ok2 || !ignore {
				return nil, err
			}

			cpy := prev.Copy()
			cpy[wtce.OffendingIdx] = wtce.OffendingVal // Needed for strings
			val = convertDataAndWarn(ctx, tableSchema, cpy, wtce.OffendingIdx, wtce.Err)
		}
		prev, ok = val.(sql.Row)
		if !ok {
			return nil, plan.ErrUpdateUnexpectedSetResult.New(val)
		}
	}
	return prev, nil
}

func (u *updateIter) validateNullability(ctx *sql.Context, row sql.Row, schema sql.Schema) error {
	for idx := 0; idx < len(row); idx++ {
		col := schema[idx]
		if !col.Nullable && row[idx] == nil {
			// In the case of an IGNORE we set the nil value to a default and add a warning
			if u.ignore {
				row[idx] = col.Type.Zero()
				_ = warnOnIgnorableError(ctx, row, sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)) // will always return nil
			} else {
				return sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)
			}

		}
	}
	return nil
}

func (u *updateIter) Close(ctx *sql.Context) error {
	if !u.closed {
		u.closed = true
		if err := u.updater.Close(ctx); err != nil {
			return err
		}
		return u.childIter.Close(ctx)
	}
	return nil
}

func (u *updateIter) ignoreOrError(ctx *sql.Context, row sql.Row, err error) error {
	if !u.ignore {
		return err
	}

	return warnOnIgnorableError(ctx, row, err)
}

func newUpdateIter(
	childIter sql.RowIter,
	schema sql.Schema,
	updater sql.RowUpdater,
	checks sql.CheckConstraints,
	ignore bool,
) sql.RowIter {
	if ignore {
		return plan.NewCheckpointingTableEditorIter(&updateIter{
			childIter: childIter,
			updater:   updater,
			schema:    schema,
			checks:    checks,
			ignore:    true,
		}, updater)
	} else {
		return plan.NewTableEditorIter(&updateIter{
			childIter: childIter,
			updater:   updater,
			schema:    schema,
			checks:    checks,
		}, updater)
	}
}

// updateJoinIter wraps the child UpdateSource projectIter and returns join row in such a way that updates per table row are
// done once.
type updateJoinIter struct {
	updateSourceIter sql.RowIter
	joinSchema       sql.Schema
	updaters         map[string]sql.RowUpdater
	caches           map[string]sql.KeyValueCache
	disposals        map[string]sql.DisposeFunc
	joinNode         sql.Node
}

var _ sql.RowIter = (*updateJoinIter)(nil)

func (u *updateJoinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		oldAndNewRow, err := u.updateSourceIter.Next(ctx)
		if err != nil {
			return nil, err
		}

		oldJoinRow, newJoinRow := oldAndNewRow[:len(oldAndNewRow)/2], oldAndNewRow[len(oldAndNewRow)/2:]

		tableToOldRowMap := plan.SplitRowIntoTableRowMap(oldJoinRow, u.joinSchema)
		tableToNewRowMap := plan.SplitRowIntoTableRowMap(newJoinRow, u.joinSchema)

		for tableName, _ := range u.updaters {
			oldTableRow := tableToOldRowMap[tableName]

			// Handle the case of row being ignored due to it not being valid in the join row.
			if isRightOrLeftJoin(u.joinNode) {
				works, err := u.shouldUpdateDirectionalJoin(ctx, oldJoinRow, oldTableRow)
				if err != nil {
					return nil, err
				}

				if !works {
					// rewrite the newJoinRow to ensure an update does not happen
					tableToNewRowMap[tableName] = oldTableRow
					continue
				}
			}

			// Determine whether this row in the table has already been updated
			cache := u.getOrCreateCache(ctx, tableName)
			hash, err := sql.HashOf(oldTableRow)
			if err != nil {
				return nil, err
			}

			_, err = cache.Get(hash)
			if sql.ErrKeyNotFound.Is(err) {
				cache.Put(hash, struct{}{})
				continue
			} else if err != nil {
				return nil, err
			}

			// If this row for the table has already been updated we rewrite the newJoinRow counterpart to ensure that this
			// returned row is not incorrectly counted by the update accumulator.
			tableToNewRowMap[tableName] = oldTableRow
		}

		newJoinRow = recreateRowFromMap(tableToNewRowMap, u.joinSchema)
		equals, err := oldJoinRow.Equals(newJoinRow, u.joinSchema)
		if err != nil {
			return nil, err
		}
		if !equals {
			return append(oldJoinRow, newJoinRow...), nil
		}
	}
}

func isRightOrLeftJoin(node sql.Node) bool {
	jn, ok := node.(*plan.JoinNode)
	if !ok {
		return false
	}
	return jn.JoinType().IsLeftOuter()
}

// shouldUpdateDirectionalJoin determines whether a table row should be updated in the context of a large right/left join row.
// A table row should only be updated if 1) It fits the join conditions (the intersection of the join) 2) It fits only
// the left or right side of the join (given the direction). A row of all nils that does not pass condition 1 must not
// be part of the update operation. This is follows the logic as established in the joinIter.
func (u *updateJoinIter) shouldUpdateDirectionalJoin(ctx *sql.Context, joinRow, tableRow sql.Row) (bool, error) {
	jn := u.joinNode.(*plan.JoinNode)
	if !jn.JoinType().IsLeftOuter() {
		return true, fmt.Errorf("expected left join")
	}

	// If the overall row fits the join condition it is fine (i.e. middle of the venn diagram).
	val, err := jn.JoinCond().Eval(ctx, joinRow)
	if err != nil {
		return true, err
	}
	if val.(bool) {
		return true, nil
	}

	for _, v := range tableRow {
		if v != nil {
			return true, nil
		}
	}

	// If the row is all nils we know it should not be updated as per the function description.
	return false, nil
}

func (u *updateJoinIter) Close(context *sql.Context) error {
	for _, disposeF := range u.disposals {
		disposeF()
	}

	return u.updateSourceIter.Close(context)
}

func (u *updateJoinIter) getOrCreateCache(ctx *sql.Context, tableName string) sql.KeyValueCache {
	potential, exists := u.caches[tableName]
	if exists {
		return potential
	}

	cache, disposal := ctx.Memory.NewHistoryCache()
	u.caches[tableName] = cache
	u.disposals[tableName] = disposal

	return cache
}

// recreateRowFromMap takes a join schema and row map and recreates the original join row.
func recreateRowFromMap(rowMap map[string]sql.Row, joinSchema sql.Schema) sql.Row {
	var ret sql.Row

	if len(joinSchema) == 0 {
		return ret
	}

	currentTable := joinSchema[0].Source
	ret = append(ret, rowMap[currentTable]...)

	for i := 1; i < len(joinSchema); i++ {
		c := joinSchema[i]

		if c.Source != currentTable {
			ret = append(ret, rowMap[c.Source]...)
			currentTable = c.Source
		}
	}

	return ret
}
