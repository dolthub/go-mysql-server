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
	"io"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/google/uuid"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type insertIter struct {
	schema                sql.Schema
	inserter              sql.RowInserter
	replacer              sql.RowReplacer
	updater               sql.RowUpdater
	rowSource             sql.RowIter
	lastInsertIdUpdated   bool
	lastInsertUuidUpdated bool
	hasAutoAutoIncValue   bool
	ctx                   *sql.Context
	insertExprs           []sql.Expression
	insertTuples          [][]sql.Expression
	insertTupleIndex      int
	updateExprs           []sql.Expression
	checks                sql.CheckConstraints
	tableNode             sql.Node
	closed                bool
	ignore                bool
}

func getInsertExpressions(values sql.Node) []sql.Expression {
	var exprs []sql.Expression
	transform.Inspect(values, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			exprs = node.Projections
			return false
		}
		return true
	})
	return exprs
}

func (i *insertIter) Next(ctx *sql.Context) (returnRow sql.Row, returnErr error) {
	row, err := i.rowSource.Next(ctx)
	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	// Prune the row down to the size of the schema. It can be larger in the case of running with an outer scope, in which
	// case the additional scope variables are prepended to the row.
	if len(row) > len(i.schema) {
		row = row[len(row)-len(i.schema):]
	}

	err = i.validateNullability(ctx, i.schema, row)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	err = i.evaluateChecks(ctx, row)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	origRow := make(sql.Row, len(row))
	copy(origRow, row)

	// Do any necessary type conversions to the target schema
	for idx, col := range i.schema {
		if row[idx] != nil {
			converted, inRange, cErr := col.Type.Convert(row[idx])
			if cErr == nil && !inRange {
				cErr = sql.ErrValueOutOfRange.New(row[idx], col.Type)
			}
			if cErr != nil {
				// Ignore individual column errors when INSERT IGNORE, UPDATE IGNORE, etc. is specified.
				// For JSON column types, always throw an error. MySQL throws the following error even when
				// IGNORE is specified:
				// ERROR 3140 (22032): Invalid JSON text: "Invalid value." at position 0 in value for column
				// 'table.column'.
				if i.ignore && col.Type.Type() != query.Type_JSON {
					if _, ok := col.Type.(sql.NumberType); ok {
						if converted == nil {
							converted = i.schema[idx].Type.Zero()
						}
						row[idx] = converted
						// Add a warning instead
						ctx.Session.Warn(&sql.Warning{
							Level:   "Note",
							Code:    sql.CastSQLError(cErr).Num,
							Message: cErr.Error(),
						})
					} else {
						row = convertDataAndWarn(ctx, i.schema, row, idx, cErr)
					}
					continue
				} else {
					// Fill in error with information
					if types.ErrLengthBeyondLimit.Is(cErr) {
						cErr = types.ErrLengthBeyondLimit.New(row[idx], col.Name)
					} else if sql.ErrNotMatchingSRID.Is(cErr) {
						cErr = sql.ErrNotMatchingSRIDWithColName.New(col.Name, cErr)
					}
					return nil, sql.NewWrappedInsertError(origRow, cErr)
				}
			}
			row[idx] = converted
		}
	}

	if i.replacer != nil {
		toReturn := make(sql.Row, len(row)*2)
		for i := 0; i < len(row); i++ {
			toReturn[i+len(row)] = row[i]
		}
		// May have multiple duplicate pk & unique errors due to multiple indexes
		//TODO: how does this interact with triggers?
		for {
			if err := i.replacer.Insert(ctx, row); err != nil {
				if !sql.ErrPrimaryKeyViolation.Is(err) && !sql.ErrUniqueKeyViolation.Is(err) {
					i.rowSource.Close(ctx)
					i.rowSource = nil
					return nil, sql.NewWrappedInsertError(row, err)
				}

				ue := err.(*errors.Error).Cause().(sql.UniqueKeyError)
				if err = i.replacer.Delete(ctx, ue.Existing); err != nil {
					i.rowSource.Close(ctx)
					i.rowSource = nil
					return nil, sql.NewWrappedInsertError(row, err)
				}
				// the row had to be deleted, write the values into the toReturn row
				copy(toReturn, ue.Existing)
			} else {
				break
			}
		}
		return toReturn, nil
	} else {
		if err := i.inserter.Insert(ctx, row); err != nil {
			if (!sql.ErrPrimaryKeyViolation.Is(err) && !sql.ErrUniqueKeyViolation.Is(err) && !sql.ErrDuplicateEntry.Is(err)) || len(i.updateExprs) == 0 {
				return nil, i.ignoreOrClose(ctx, row, err)
			}

			ue := err.(*errors.Error).Cause().(sql.UniqueKeyError)
			return i.handleOnDuplicateKeyUpdate(ctx, ue.Existing, row)
		}
	}

	i.updateLastInsertId(ctx, row)
	i.updateLastInsertUuid(ctx, row)
	i.insertTupleIndex++

	return row, nil
}

func (i *insertIter) handleOnDuplicateKeyUpdate(ctx *sql.Context, oldRow, newRow sql.Row) (returnRow sql.Row, returnErr error) {
	var err error
	updateAcc := append(oldRow, newRow...)
	var evalRow sql.Row
	for _, updateExpr := range i.updateExprs {
		// this SET <val> indexes into LHS, but the <expr> can
		// reference the new row on RHS
		val, err := updateExpr.Eval(i.ctx, updateAcc)
		if err != nil {
			if i.ignore {
				idx, ok := getFieldIndexFromUpdateExpr(updateExpr)
				if !ok {
					return nil, err
				}

				val = convertDataAndWarn(ctx, i.schema, newRow, idx, err)
			} else {
				return nil, err
			}
		}

		updateAcc = val.(sql.Row)
	}
	// project LHS only
	evalRow = updateAcc[:len(oldRow)]

	// Should revaluate the check conditions.
	err = i.evaluateChecks(ctx, evalRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	err = i.updater.Update(ctx, oldRow, evalRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	// In the case that we attempted an update, return a concatenated [old,new] row just like update.
	return oldRow.Append(evalRow), nil
}

func getFieldIndexFromUpdateExpr(updateExpr sql.Expression) (int, bool) {
	setField, ok := updateExpr.(*expression.SetField)
	if !ok {
		return 0, false
	}

	getField, ok := setField.LeftChild.(*expression.GetField)
	if !ok {
		return 0, false
	}

	return getField.Index(), true
}

// resolveValues resolves all VALUES functions.
func (i *insertIter) resolveValues(ctx *sql.Context, insertRow sql.Row) error {
	for _, updateExpr := range i.updateExprs {
		var err error
		sql.Inspect(updateExpr, func(expr sql.Expression) bool {
			valuesExpr, ok := expr.(*function.Values)
			if !ok {
				return true
			}
			getField, ok := valuesExpr.Child.(*expression.GetField)
			if !ok {
				err = fmt.Errorf("VALUES functions may only contain column names")
				return false
			}
			valuesExpr.Value = insertRow[getField.Index()]
			return false
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *insertIter) Close(ctx *sql.Context) error {
	if !i.closed {
		i.closed = true
		var rsErr, iErr, rErr, uErr error
		if i.rowSource != nil {
			rsErr = i.rowSource.Close(ctx)
		}
		if i.inserter != nil {
			iErr = i.inserter.Close(ctx)
		}
		if i.replacer != nil {
			rErr = i.replacer.Close(ctx)
		}
		if i.updater != nil {
			uErr = i.updater.Close(ctx)
		}
		if rsErr != nil {
			return rsErr
		}
		if iErr != nil {
			return iErr
		}
		if rErr != nil {
			return rErr
		}
		if uErr != nil {
			return uErr
		}
	}
	return nil
}

func (i *insertIter) updateLastInsertId(ctx *sql.Context, row sql.Row) {
	if i.lastInsertIdUpdated {
		return
	}

	if i.hasAutoAutoIncValue {
		autoIncVal := i.getAutoIncVal(row)
		ctx.SetLastQueryInfo(sql.LastInsertId, autoIncVal)
		i.lastInsertIdUpdated = true
	}
}

func (i *insertIter) updateLastInsertUuid(ctx *sql.Context, row sql.Row) {
	// If we've already captured the first inserted UUID in this statement, don't capture any more
	if i.lastInsertUuidUpdated {
		return
	}

	if uuidColumnIdx := i.findUuidPrimaryKey(); uuidColumnIdx != -1 {
		uuidVal := i.getUuidVal(uuidColumnIdx, row)
		ctx.SetLastQueryInfoString(sql.LastInsertUuid, uuidVal)
		i.lastInsertUuidUpdated = true
	}
}

func (i *insertIter) getAutoIncVal(row sql.Row) int64 {
	var autoIncVal int64
	for i, expr := range i.insertExprs {
		if _, ok := expr.(*expression.AutoIncrement); ok {
			autoIncVal = toInt64(row[i])
			break
		}
	}
	return autoIncVal
}

func (i *insertIter) findUuidPrimaryKey() int {
	// TODO: this could be computed once up front, instead of for each row (although... we would skip out early
	//       anyway after we find the first insert UUID)

	// TODO: Should we consider matching columns as long as they are PART of the PK?
	//       Or restrict it to being the ONLY column in the PK?
	for columnIdx, col := range i.schema {
		if col.PrimaryKey == false {
			continue
		}

		switch col.Type.Type() {
		case sqltypes.Char, sqltypes.VarChar:
			stringType := col.Type.(sql.StringType)
			if stringType.MaxCharacterLength() != 36 {
				continue
			}
			if _, ok := col.Default.Expr.(*function.UUIDFunc); ok {
				// Do we need to make sure UUIDFunc doesn't have a child argument in it?
				return columnIdx
			}
		case sqltypes.Binary, sqltypes.VarBinary:
			stringType := col.Type.(sql.StringType)
			if stringType.MaxByteLength() != 16 {
				continue
			}
			if uuidToBinFunc, ok := col.Default.Expr.(*function.UUIDToBin); ok {
				if _, ok := uuidToBinFunc.Children()[0].(*function.UUIDFunc); ok {
					// Do we need to make sure UUIDFunc doesn't have a child argument in it?
					return columnIdx
				}
			}
		default:
			// no-op
		}
	}

	return -1
}

func (i *insertIter) getUuidVal(idx int, row sql.Row) string {
	// There can only be one column that has a UUID in it that we will store as the last insert UUID
	// We need to precompute whether the schema has one of these, and if so, what column index it is
	// Then in this function, we can just grab that value from the row.
	//
	// However... something that's different from auto_increment is that we can use "DEFAULT" or we can
	// explicitly call "UUID()" and in either case, we should capture that as the insert UUID.
	//
	// However... like auto_increment, if they provide a value that does NOT use auto_increment, then
	// we should NOT capture that.

	// Grab the expression that generated the value for the UUID key column
	expr := i.insertTuples[i.insertTupleIndex][idx]
	if wrapper, ok := expr.(*expression.Wrapper); ok {
		expr = wrapper.Unwrap()
	}

	// If the Tuple Expression has a *sql.ColumnDefaultValue in it, then return the row value
	foundAThing := false
	transform.InspectExpr(expr, func(expr sql.Expression) bool {
		if _, ok := expr.(*sql.ColumnDefaultValue); ok {
			foundAThing = true
		}
		return foundAThing
	})

	// If the expression is a function.UUIDFunc (directly, not transitively), then return the row value
	if _, ok := expr.(*function.UUIDFunc); ok {
		// TODO: This should probably also check that the column is a varchar(36)? i.e. prevent explicitly trying to
		//       insert a UUID() into a varbinary(16)
		foundAThing = true
	}
	if uuidToBin, ok := expr.(*function.UUIDToBin); ok {
		if _, ok := uuidToBin.Children()[0].(*function.UUIDFunc); ok {
			// TODO: Same comment here... this should probably assert the column is varbinary(16)
			foundAThing = true
		}
	}

	if foundAThing {
		// TODO: for UUID_to_bin... we need to run bin_to_uuid to convert it back to a UUID string
		isBinary := false
		if i.schema[idx].Type.Type() == sqltypes.Binary || i.schema[idx].Type.Type() == sqltypes.VarBinary {
			isBinary = true
		}

		// TODO: Do we need to support the swap flag in uuid_to_bin? YES!
		//       can use the function swapUUIDBytes maybe? if we make it public? (or do we need unswap?)
		if isBinary {
			bytes := row[idx].([]byte)
			parsed, err := uuid.FromBytes(bytes)
			if err != nil {
				// TODO: Fix to return an error
				//return nil, sql.ErrUuidUnableToParse.New(bytes, err.Error())
				panic(sql.ErrUuidUnableToParse.New(bytes, err.Error()).Error())
			}
			return parsed.String()
		} else {
			return row[idx].(string)
		}
	}

	return ""
}

func (i *insertIter) ignoreOrClose(ctx *sql.Context, row sql.Row, err error) error {
	if !i.ignore {
		return sql.NewWrappedInsertError(row, err)
	}

	return warnOnIgnorableError(ctx, row, err)
}

// convertDataAndWarn modifies a row with data conversion issues in INSERT/UPDATE IGNORE calls
// Per MySQL docs "Rows set to values that would cause data conversion errors are set to the closest valid values instead"
// cc. https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sql-mode-strict
func convertDataAndWarn(ctx *sql.Context, tableSchema sql.Schema, row sql.Row, columnIdx int, err error) sql.Row {
	if types.ErrLengthBeyondLimit.Is(err) {
		maxLength := tableSchema[columnIdx].Type.(sql.StringType).MaxCharacterLength()
		row[columnIdx] = row[columnIdx].(string)[:maxLength] // truncate string
	} else {
		row[columnIdx] = tableSchema[columnIdx].Type.Zero()
	}

	sqlerr := sql.CastSQLError(err)

	// Add a warning instead
	if ctx != nil && ctx.Session != nil {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Note",
			Code:    sqlerr.Num,
			Message: err.Error(),
		})
	}

	return row
}

func warnOnIgnorableError(ctx *sql.Context, row sql.Row, err error) error {
	// Check that this error is a part of the list of Ignorable Errors and create the relevant warning
	for _, ie := range plan.IgnorableErrors {
		if ie.Is(err) {
			sqlerr := sql.CastSQLError(err)

			// Add a warning instead
			if ctx != nil && ctx.Session != nil {
				ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    sqlerr.Num,
					Message: err.Error(),
				})
			}

			// In this case the default value gets updated so return nil
			if sql.ErrInsertIntoNonNullableDefaultNullColumn.Is(err) {
				return nil
			}

			// Return the InsertIgnore err to ensure our accumulator doesn't count this row.
			return sql.NewIgnorableError(row)
		}
	}

	return err
}

func (i *insertIter) evaluateChecks(ctx *sql.Context, row sql.Row) error {
	for _, check := range i.checks {
		if !check.Enforced {
			continue
		}

		res, err := sql.EvaluateCondition(ctx, check.Expr, row)

		if err != nil {
			return err
		}

		if sql.IsFalse(res) {
			return sql.ErrCheckConstraintViolated.New(check.Name)
		}
	}

	return nil
}

func (i *insertIter) validateNullability(ctx *sql.Context, dstSchema sql.Schema, row sql.Row) error {
	for count, col := range dstSchema {
		if !col.Nullable && row[count] == nil {
			// In the case of an IGNORE we set the nil value to a default and add a warning
			if i.ignore {
				row[count] = col.Type.Zero()
				_ = warnOnIgnorableError(ctx, row, sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)) // will always return nil
			} else {
				return sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)
			}
		}
	}
	return nil
}

func toInt64(x interface{}) int64 {
	switch x := x.(type) {
	case int:
		return int64(x)
	case uint:
		return int64(x)
	case int8:
		return int64(x)
	case uint8:
		return int64(x)
	case int16:
		return int64(x)
	case uint16:
		return int64(x)
	case int32:
		return int64(x)
	case uint32:
		return int64(x)
	case int64:
		return x
	case uint64:
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		panic(fmt.Sprintf("Expected a numeric auto increment value, but got %T", x))
	}
}
