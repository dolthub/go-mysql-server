// Copyright 2020-2021 Dolthub, Inc.
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

package plan

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ErrInsertIntoNotSupported is thrown when a table doesn't support inserts
var ErrInsertIntoNotSupported = errors.NewKind("table doesn't support INSERT INTO")
var ErrReplaceIntoNotSupported = errors.NewKind("table doesn't support REPLACE INTO")
var ErrOnDuplicateKeyUpdateNotSupported = errors.NewKind("table doesn't support ON DUPLICATE KEY UPDATE")
var ErrAutoIncrementNotSupported = errors.NewKind("table doesn't support AUTO_INCREMENT")
var ErrInsertIntoMismatchValueCount = errors.NewKind("number of values does not match number of columns provided")
var ErrInsertIntoUnsupportedValues = errors.NewKind("%T is unsupported for inserts")
var ErrInsertIntoDuplicateColumn = errors.NewKind("duplicate column name %v")
var ErrInsertIntoNonexistentColumn = errors.NewKind("invalid column name %v")
var ErrInsertIntoIncompatibleTypes = errors.NewKind("cannot convert type %s to %s")

// cc: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sql-mode-strict
// The INSERT IGNORE syntax applies to these ignorable errors
// ER_BAD_NULL_ERROR - yes
// ER_DUP_ENTRY - yes
// ER_DUP_ENTRY_WITH_KEY_NAME - Yes
// ER_DUP_KEY - kinda
// ER_NO_PARTITION_FOR_GIVEN_VALUE - yes
// ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT - No
// ER_NO_REFERENCED_ROW_2 - Yes
// ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET - No
// ER_ROW_IS_REFERENCED_2 - Yes
// ER_SUBQUERY_NO_1_ROW - yes
// ER_VIEW_CHECK_FAILED - No
var IgnorableErrors = []*errors.Kind{sql.ErrInsertIntoNonNullableProvidedNull,
	sql.ErrPrimaryKeyViolation,
	sql.ErrPartitionNotFound,
	sql.ErrExpectedSingleRow,
	sql.ErrForeignKeyChildViolation,
	sql.ErrForeignKeyParentViolation,
	sql.ErrDuplicateEntry,
	sql.ErrUniqueKeyViolation,
	sql.ErrCheckConstraintViolated,
}

// InsertInto is the top level node for INSERT INTO statements. It has a source for rows and a destination to insert
// them into.
type InsertInto struct {
	db          sql.Database
	Destination sql.Node
	Source      sql.Node
	ColumnNames []string
	IsReplace   bool
	OnDupExprs  []sql.Expression
	Checks      sql.CheckConstraints
	Ignore      bool
}

var _ sql.Databaser = (*InsertInto)(nil)
var _ sql.Node = (*InsertInto)(nil)
var _ sql.Expressioner = (*InsertInto)(nil)

// NewInsertInto creates an InsertInto node.
func NewInsertInto(db sql.Database, dst, src sql.Node, isReplace bool, cols []string, onDupExprs []sql.Expression, ignore bool) *InsertInto {
	return &InsertInto{
		db:          db,
		Destination: dst,
		Source:      src,
		ColumnNames: cols,
		IsReplace:   isReplace,
		OnDupExprs:  onDupExprs,
		Ignore:      ignore,
	}
}

// Schema implements the sql.Node interface.
// Insert nodes return rows that are inserted. Replaces return a concatenation of the deleted row and the inserted row.
// If no row was deleted, the value of those columns is nil.
func (ii *InsertInto) Schema() sql.Schema {
	if ii.IsReplace {
		return append(ii.Destination.Schema(), ii.Destination.Schema()...)
	}
	return ii.Destination.Schema()
}

func (ii *InsertInto) Children() []sql.Node {
	// The source node is analyzed completely independently, so we don't include it in children
	return []sql.Node{ii.Destination}
}

func (ii *InsertInto) Database() sql.Database {
	return ii.db
}

func (ii *InsertInto) WithDatabase(database sql.Database) (sql.Node, error) {
	nc := *ii
	nc.db = database
	return &nc, nil
}

// InsertDestination is a wrapper for a table to be used with InsertInto.Destination that allows the schema to be
// overridden. This is useful when the table in question has late-resolving column defaults.
type InsertDestination struct {
	UnaryNode
	Sch sql.Schema
}

var _ sql.Expressioner = (*InsertDestination)(nil)

func NewInsertDestination(schema sql.Schema, node sql.Node) *InsertDestination {
	return &InsertDestination{
		UnaryNode: UnaryNode{Child: node},
		Sch:       schema,
	}
}

func (id *InsertDestination) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(id.Sch)
}

func (id InsertDestination) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(id.Sch) {
		return nil, sql.ErrInvalidChildrenNumber.New(id, len(exprs), len(id.Sch))
	}

	id.Sch = transform.SchemaWithDefaults(id.Sch, exprs)
	return &id, nil
}

func (id *InsertDestination) String() string {
	return id.UnaryNode.Child.String()
}

func (id *InsertDestination) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("InsertDestination")
	var children []string
	for _, col := range id.Sch {
		children = append(children, sql.DebugString(col.Default))
	}
	children = append(children, sql.DebugString(id.Child))

	pr.WriteChildren(children...)

	return pr.String()
}

func (id *InsertDestination) Schema() sql.Schema {
	return id.Sch
}

func (id *InsertDestination) Resolved() bool {
	if !id.UnaryNode.Resolved() {
		return false
	}

	for _, col := range id.Sch {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

func (id *InsertDestination) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return id.UnaryNode.Child.RowIter(ctx, row)
}

func (id InsertDestination) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(id, len(children), 1)
	}

	id.UnaryNode.Child = children[0]
	return &id, nil
}

// CheckPrivileges implements the interface sql.Node.
func (id *InsertDestination) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return id.Child.CheckPrivileges(ctx, opChecker)
}

type insertIter struct {
	schema              sql.Schema
	inserter            sql.RowInserter
	replacer            sql.RowReplacer
	updater             sql.RowUpdater
	rowSource           sql.RowIter
	lastInsertIdUpdated bool
	ctx                 *sql.Context
	insertExprs         []sql.Expression
	updateExprs         []sql.Expression
	checks              sql.CheckConstraints
	tableNode           sql.Node
	closed              bool
	ignore              bool
}

func GetInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *Exchange:
		return GetInsertable(node.Child)
	case sql.InsertableTable:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	case sql.TableWrapper:
		return getInsertableTable(node.Underlying())
	case *InsertDestination:
		return GetInsertable(node.Child)
	case *prependNode:
		return GetInsertable(node.Child)
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.InsertableTable, error) {
	switch t := t.(type) {
	case sql.InsertableTable:
		return t, nil
	case sql.TableWrapper:
		return getInsertableTable(t.Underlying())
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func newInsertIter(
	ctx *sql.Context,
	dest sql.Node,
	values sql.Node,
	isReplace bool,
	onDupUpdateExpr []sql.Expression,
	checks sql.CheckConstraints,
	row sql.Row,
	ignore bool,
) (sql.RowIter, error) {
	// This schema may vary from the table itself, particularly in terms of column defaults
	dstSchema := dest.Schema()

	insertable, err := GetInsertable(dest)
	if err != nil {
		return nil, err
	}

	var inserter sql.RowInserter

	var replacer sql.RowReplacer
	var updater sql.RowUpdater
	// These type casts have already been asserted in the analyzer
	if isReplace {
		replacer = insertable.(sql.ReplaceableTable).Replacer(ctx)
	} else {
		inserter = insertable.Inserter(ctx)
		if len(onDupUpdateExpr) > 0 {
			updater = insertable.(sql.UpdatableTable).Updater(ctx)
		}
	}

	rowIter, err := values.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	insertExpressions := getInsertExpressions(values)
	insertIter := &insertIter{
		schema:      dstSchema,
		tableNode:   dest,
		inserter:    inserter,
		replacer:    replacer,
		updater:     updater,
		rowSource:   rowIter,
		updateExprs: onDupUpdateExpr,
		insertExprs: insertExpressions,
		checks:      checks,
		ctx:         ctx,
		ignore:      ignore,
	}

	var ed sql.TableEditor
	if replacer != nil {
		ed = replacer
	} else {
		ed = inserter
	}

	if ignore {
		return NewCheckpointingTableEditorIter(ed, insertIter), nil
	} else {
		return NewTableEditorIter(ed, insertIter), nil
	}
}

func getInsertExpressions(values sql.Node) []sql.Expression {
	var exprs []sql.Expression
	transform.Inspect(values, func(node sql.Node) bool {
		switch node := node.(type) {
		case *Project:
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
			converted, cErr := col.Type.Convert(row[idx]) // allows for better error handling
			if cErr != nil {
				// Ignore individual column errors when INSERT IGNORE, UPDATE IGNORE, etc. is specified.
				// For JSON column types, always throw an error. MySQL throws the following error even when
				// IGNORE is specified:
				// ERROR 3140 (22032): Invalid JSON text: "Invalid value." at position 0 in value for column
				// 'table.column'.
				if i.ignore && col.Type.Type() != query.Type_JSON {
					row = convertDataAndWarn(ctx, i.schema, row, idx, cErr)
					continue
				} else {
					// Fill in error with information
					if sql.ErrLengthBeyondLimit.Is(cErr) {
						cErr = sql.ErrLengthBeyondLimit.New(row[idx], col.Name)
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
				for i := 0; i < len(ue.Existing); i++ {
					toReturn[i] = ue.Existing[i]
				}
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
			return i.handleOnDuplicateKeyUpdate(ctx, row, ue.Existing)
		}
	}

	i.updateLastInsertId(ctx, row)

	return row, nil
}

func (i *insertIter) handleOnDuplicateKeyUpdate(ctx *sql.Context, row, rowToUpdate sql.Row) (returnRow sql.Row, returnErr error) {
	err := i.resolveValues(ctx, row)
	if err != nil {
		return nil, err
	}

	newRow := rowToUpdate
	for _, updateExpr := range i.updateExprs {
		val, err := updateExpr.Eval(i.ctx, newRow)
		if err != nil {
			if i.ignore {
				idx, ok := getFieldIndexFromUpdateExpr(updateExpr)
				if !ok {
					return nil, err
				}

				val = convertDataAndWarn(ctx, i.schema, row, idx, err)
			} else {
				return nil, err
			}
		}

		newRow = val.(sql.Row)
	}

	// Should revaluate the check conditions.
	err = i.evaluateChecks(ctx, newRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	err = i.updater.Update(ctx, rowToUpdate, newRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	// In the case that we attempted an update, return a concatenated [old,new] row just like update.
	return rowToUpdate.Append(newRow), nil
}

func getFieldIndexFromUpdateExpr(updateExpr sql.Expression) (int, bool) {
	setField, ok := updateExpr.(*expression.SetField)
	if !ok {
		return 0, false
	}

	getField, ok := setField.Left.(*expression.GetField)
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

	var autoIncVal int64
	var found bool
	for i, expr := range i.insertExprs {
		if _, ok := expr.(*expression.AutoIncrement); ok {
			autoIncVal = toInt64(row[i])
			found = true
			break
		}
	}

	if found {
		ctx.SetLastQueryInfo(sql.LastInsertId, autoIncVal)
		i.lastInsertIdUpdated = true
	}
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
	if sql.ErrLengthBeyondLimit.Is(err) {
		maxLength := tableSchema[columnIdx].Type.(sql.StringType).MaxCharacterLength()
		row[columnIdx] = row[columnIdx].(string)[:maxLength] // truncate string
	} else {
		row[columnIdx] = tableSchema[columnIdx].Type.Zero()
	}

	sqlerr := sql.CastSQLError(err)

	// Add a warning instead
	ctx.Session.Warn(&sql.Warning{
		Level:   "Note",
		Code:    sqlerr.Num,
		Message: err.Error(),
	})

	return row
}

func warnOnIgnorableError(ctx *sql.Context, row sql.Row, err error) error {
	// Check that this error is a part of the list of Ignorable Errors and create the relevant warning
	for _, ie := range IgnorableErrors {
		if ie.Is(err) {
			sqlerr := sql.CastSQLError(err)

			// Add a warning instead
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    sqlerr.Num,
				Message: err.Error(),
			})

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
		if x > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		panic(fmt.Sprintf("Expected a numeric auto increment value, but got %T", x))
	}
}

// RowIter implements the Node interface.
func (ii *InsertInto) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return newInsertIter(ctx, ii.Destination, ii.Source, ii.IsReplace, ii.OnDupExprs, ii.Checks, row, ii.Ignore)
}

// WithChildren implements the Node interface.
func (ii *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ii, len(children), 1)
	}

	np := *ii
	np.Destination = children[0]
	return &np, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ii *InsertInto) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if ii.IsReplace {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(ii.db.Name(), getTableName(ii.Destination), "", sql.PrivilegeType_Insert, sql.PrivilegeType_Delete))
	} else {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(ii.db.Name(), getTableName(ii.Destination), "", sql.PrivilegeType_Insert))
	}
}

// WithSource sets the source node for this insert, which is analyzed separately
func (ii *InsertInto) WithSource(src sql.Node) sql.Node {
	np := *ii
	np.Source = src
	return &np
}

func (ii InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if ii.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(ii.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(ii.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(ii.Destination.String(), ii.Source.String())
	return pr.String()
}

func (ii InsertInto) DebugString() string {
	pr := sql.NewTreePrinter()
	if ii.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(ii.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(ii.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(sql.DebugString(ii.Destination), sql.DebugString(ii.Source))
	return pr.String()
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

func (ii *InsertInto) Expressions() []sql.Expression {
	return append(ii.OnDupExprs, ii.Checks.ToExpressions()...)
}

func (ii InsertInto) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(ii.OnDupExprs)+len(ii.Checks) {
		return nil, sql.ErrInvalidChildrenNumber.New(ii, len(newExprs), len(ii.OnDupExprs)+len(ii.Checks))
	}

	ii.OnDupExprs = newExprs[:len(ii.OnDupExprs)]

	var err error
	ii.Checks, err = ii.Checks.FromExpressions(newExprs[len(ii.OnDupExprs):])
	if err != nil {
		return nil, err
	}

	return &ii, nil
}

// Resolved implements the Resolvable interface.
func (ii *InsertInto) Resolved() bool {
	if !ii.Destination.Resolved() || !ii.Source.Resolved() {
		return false
	}
	for _, updateExpr := range ii.OnDupExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	for _, checkExpr := range ii.Checks {
		if !checkExpr.Expr.Resolved() {
			return false
		}
	}
	return true
}
