// Copyright 2023-2024 Dolthub, Inc.
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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/vitess/go/mysql"
)

const TriggerSavePointPrefix = "__go_mysql_server_trigger_savepoint__"

type triggerRollbackIter struct {
	child         sql.RowIter
	savePointName string
}

func AddTriggerRollbackIter(ctx *sql.Context, qFlags *sql.QueryFlags, iter sql.RowIter) sql.RowIter {
	if !qFlags.IsSet(sql.QFlagTrigger) {
		return iter
	}

	transSess, isTransSess := ctx.Session.(sql.TransactionSession)
	if !isTransSess {
		return iter
	}

	ctx.GetLogger().Tracef("TriggerRollback creating savepoint: %s", TriggerSavePointPrefix)
	if err := transSess.CreateSavepoint(ctx, ctx.GetTransaction(), TriggerSavePointPrefix); err != nil {
		ctx.GetLogger().WithError(err).Errorf("CreateSavepoint failed")
	}

	return &triggerRollbackIter{
		child:         iter,
		savePointName: TriggerSavePointPrefix,
	}
}

func (t *triggerRollbackIter) Next(ctx *sql.Context) (row sql.Row, returnErr error) {
	childRow, err := t.child.Next(ctx)

	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil, fmt.Errorf("expected a sql.TransactionSession, but got %T", ctx.Session)
	}

	// Rollback if error occurred
	if err != nil && err != io.EOF {
		if err := ts.RollbackToSavepoint(ctx, ctx.GetTransaction(), t.savePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling RollbackToSavePoint during triggerRollbackIter.Next()")
		}
		if err := ts.ReleaseSavepoint(ctx, ctx.GetTransaction(), t.savePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling ReleaseSavepoint during triggerRollbackIter.Next()")
		} else {
			t.savePointName = ""
		}
	}

	return childRow, err
}

func (t *triggerRollbackIter) Close(ctx *sql.Context) error {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return fmt.Errorf("expected a sql.TransactionSession, but got %T", ctx.Session)
	}

	if len(t.savePointName) != 0 {
		if err := ts.ReleaseSavepoint(ctx, ctx.GetTransaction(), t.savePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling ReleaseSavepoint during triggerRollbackIter.Close()")
		}
		t.savePointName = ""
	}
	return t.child.Close(ctx)
}

// triggerBlockIter is the sql.RowIter for TRIGGER BEGIN/END blocks, which operate differently than normal blocks.
type triggerBlockIter struct {
	statements []sql.Node
	row        sql.Row
	once       *sync.Once
	b          *BaseBuilder
}

var _ sql.RowIter = (*triggerBlockIter)(nil)

// Next implements the sql.RowIter interface.
func (i *triggerBlockIter) Next(ctx *sql.Context) (sql.Row, error) {
	run := false
	i.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	row := i.row
	for _, s := range i.statements {
		subIter, err := i.b.buildNodeExec(ctx, s, row)
		if err != nil {
			return nil, err
		}

		for {
			newRow, err := subIter.Next(ctx)
			if err == io.EOF {
				err := subIter.Close(ctx)
				if err != nil {
					return nil, err
				}
				break
			} else if err != nil {
				_ = subIter.Close(ctx)
				return nil, err
			}

			// We only return the result of a trigger block statement in certain cases, specifically when we are setting the
			// value of new.field, so that the wrapping iterator can use it for the insert / update. Otherwise, this iterator
			// always returns its input row.
			if shouldUseTriggerStatementForReturnRow(s) {
				row = newRow.Subslice(newRow.Len()/2, newRow.Len())
			}
		}
	}

	return row, nil
}

// shouldUseTriggerStatementForReturnRow returns whether the statement has Set node that contains GetField expression,
// which means whether there is column value update. The Set node can be inside other nodes, so need to inspect all nodes
// of the given node.
func shouldUseTriggerStatementForReturnRow(stmt sql.Node) bool {
	hasSetField := false
	transform.Inspect(stmt, func(n sql.Node) bool {
		switch logic := n.(type) {
		case *plan.Set:
			for _, expr := range logic.Exprs {
				sql.Inspect(expr.(*expression.SetField).LeftChild, func(e sql.Expression) bool {
					if _, ok := e.(*expression.GetField); ok {
						hasSetField = true
						return false
					}
					return true
				})
			}
		}
		return true
	})
	return hasSetField
}

// Close implements the sql.RowIter interface.
func (i *triggerBlockIter) Close(*sql.Context) error {
	return nil
}

type triggerIter struct {
	child          sql.RowIter
	executionLogic sql.Node
	triggerTime    plan.TriggerTime
	triggerEvent   plan.TriggerEvent
	ctx            *sql.Context
	b              *BaseBuilder
}

// prependRowInPlanForTriggerExecution returns a transformation function that prepends the row given to any row source in a query
// plan. Any source of rows, as well as any node that alters the schema of its children, will be wrapped so that its
// result rows are prepended with the row given.
func prependRowInPlanForTriggerExecution(row sql.Row) func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
	return func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.Project:
			// Only prepend rows for projects that aren't the input to inserts and other triggers
			switch c.Parent.(type) {
			case *plan.InsertInto, *plan.Into, *plan.TriggerExecutor:
				return n, transform.SameTree, nil
			default:
				return plan.NewPrependNode(n, row), transform.NewTree, nil
			}
		case *plan.ResolvedTable, *plan.IndexedTableAccess:
			return plan.NewPrependNode(n, row), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	}
}

func prependRowForTriggerExecutionSelector(ctx transform.Context) bool {
	switch p := ctx.Parent.(type) {
	case *plan.TriggerExecutor:
		// don't nest prepends
		return !(p.Right() == ctx.Node)
	default:
		return true
	}
}

func (t *triggerIter) Next(ctx *sql.Context) (row sql.Row, returnErr error) {
	childRow, err := t.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap the execution logic with the current child row before executing it.
	logic, _, err := transform.NodeWithCtx(t.executionLogic, prependRowForTriggerExecutionSelector, prependRowInPlanForTriggerExecution(childRow))
	if err != nil {
		return nil, err
	}

	// We don't do anything interesting with this subcontext yet, but it's a good idea to cancel it independently of the
	// parent context if something goes wrong in trigger execution.
	ctx, cancelFunc := t.ctx.NewSubContext()
	defer cancelFunc()

	logicIter, err := t.b.buildNodeExec(ctx, logic, childRow)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := logicIter.Close(t.ctx)
		if returnErr == nil {
			returnErr = err
		}
	}()

	var logicRow sql.Row = sql.UntypedSqlRow{}
	for {
		row, err := logicIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		logicRow = row
	}

	// For some logic statements, we want to return the result of the logic operation as our row, e.g. a Set that alters
	// the fields of the new row
	if ok, returnRow := shouldUseLogicResult(logic, logicRow); ok {
		return returnRow, nil
	}

	return childRow, nil
}

func shouldUseLogicResult(logic sql.Node, row sql.Row) (bool, sql.Row) {
	switch logic := logic.(type) {
	// TODO: are there other statement types that we should use here?
	case *plan.Set:
		hasSetField := false
		for _, expr := range logic.Exprs {
			sql.Inspect(expr.(*expression.SetField).LeftChild, func(e sql.Expression) bool {
				if _, ok := e.(*expression.GetField); ok {
					hasSetField = true
					return false
				}
				return true
			})
		}
		return hasSetField, row.Subslice(row.Len()/2, row.Len())
	case *plan.TriggerBeginEndBlock:
		hasSetField := false
		transform.Inspect(logic, func(n sql.Node) bool {
			set, ok := n.(*plan.Set)
			if !ok {
				return true
			}
			for _, expr := range set.Exprs {
				sql.Inspect(expr.(*expression.SetField).LeftChild, func(e sql.Expression) bool {
					if _, ok := e.(*expression.GetField); ok {
						hasSetField = true
						return false
					}
					return true
				})
			}
			return !hasSetField
		})
		return hasSetField, row
	default:
		return false, nil
	}
}

func (t *triggerIter) Close(ctx *sql.Context) error {
	return t.child.Close(ctx)
}

type accumulatorRowHandler interface {
	handleRowUpdate(row sql.Row) error
	okResult() types.OkResult
}

// TODO: Extend this to UPDATE IGNORE JOIN
type updateIgnoreAccumulatorRowHandler interface {
	accumulatorRowHandler
	handleRowUpdateWithIgnore(row sql.Row, ignore bool) error
}

type insertRowHandler struct {
	rowsAffected              int
	lastInsertId              uint64
	updatedAutoIncrementValue bool
	lastInsertIdGetter        func(row sql.Row) int64
}

func (i *insertRowHandler) handleRowUpdate(row sql.Row) error {
	i.rowsAffected++
	return nil
}

func (i *insertRowHandler) okResult() types.OkResult {
	return types.OkResult{
		RowsAffected: uint64(i.rowsAffected),
	}
}

type replaceRowHandler struct {
	rowsAffected int
}

func (r *replaceRowHandler) handleRowUpdate(row sql.Row) error {
	r.rowsAffected++

	// If a row was deleted as well as inserted, increment the counter again. A row was deleted if at least one column in
	// the first half of the row is non-null.
	for i := 0; i < row.Len()/2; i++ {
		if row.GetValue(i) != nil {
			r.rowsAffected++
			break
		}
	}

	return nil
}

func (r *replaceRowHandler) okResult() types.OkResult {
	return types.NewOkResult(r.rowsAffected)
}

type onDuplicateUpdateHandler struct {
	rowsAffected              int
	schema                    sql.Schema
	clientFoundRowsCapability bool
}

func (o *onDuplicateUpdateHandler) handleRowUpdate(row sql.Row) error {
	// See https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html for row count semantics
	// If a row was inserted, increment by 1
	if row.Len() == len(o.schema) {
		o.rowsAffected++
		return nil
	}

	// Otherwise (a row was updated), increment by 2 if the row changed, 0 if not
	oldRow := row.Subslice(0, row.Len()/2)
	newRow := row.Subslice(row.Len()/2, row.Len())
	if equals, err := oldRow.Equals(newRow, o.schema); err == nil {
		if equals {
			// Ig the CLIENT_FOUND_ROWS capabilities flag is set, increment by 1 if a row stays the same.
			if o.clientFoundRowsCapability {
				o.rowsAffected++
			}
		} else {
			o.rowsAffected += 2
		}
	} else {
		o.rowsAffected++
	}

	return nil
}

func (o *onDuplicateUpdateHandler) okResult() types.OkResult {
	return types.NewOkResult(o.rowsAffected)
}

type updateRowHandler struct {
	rowsMatched               int
	rowsAffected              int
	schema                    sql.Schema
	clientFoundRowsCapability bool
}

func (u *updateRowHandler) handleRowUpdate(row sql.Row) error {
	u.rowsMatched++
	oldRow := row.Subslice(0, row.Len()/2)
	newRow := row.Subslice(row.Len()/2, row.Len())
	if equals, err := oldRow.Equals(newRow, u.schema); err == nil {
		if !equals {
			u.rowsAffected++
		}
	} else {
		return err
	}
	return nil
}

func (u *updateRowHandler) handleRowUpdateWithIgnore(row sql.Row, ignore bool) error {
	if !ignore {
		return u.handleRowUpdate(row)
	}

	u.rowsMatched++
	return nil
}

func (u *updateRowHandler) okResult() types.OkResult {
	affected := u.rowsAffected
	if u.clientFoundRowsCapability {
		affected = u.rowsMatched
	}
	return types.OkResult{
		RowsAffected: uint64(affected),
		Info: plan.UpdateInfo{
			Matched:  u.rowsMatched,
			Updated:  u.rowsAffected,
			Warnings: 0,
		},
	}
}

func (u *updateRowHandler) RowsMatched() int64 {
	return int64(u.rowsMatched)
}

// updateJoinRowHandler handles row update count for all UPDATEs that use a JOIN.
type updateJoinRowHandler struct {
	rowsMatched  int
	rowsAffected int
	joinSchema   sql.Schema
	tableMap     map[string]sql.Schema // Needs to only be the tables that can be updated.
	updaterMap   map[string]sql.RowUpdater
}

// handleRowMatched is called when an update join's source returns a row
func (u *updateJoinRowHandler) handleRowMatched() {
	u.rowsMatched += 1
}

func (u *updateJoinRowHandler) handleRowUpdate(row sql.Row) error {
	oldJoinRow := row.Subslice(0, row.Len()/2)
	newJoinRow := row.Subslice(row.Len()/2, row.Len())

	tableToOldRow := plan.SplitRowIntoTableRowMap(oldJoinRow, u.joinSchema)
	tableToNewRow := plan.SplitRowIntoTableRowMap(newJoinRow, u.joinSchema)

	for tableName, _ := range u.updaterMap {
		tableOldRow := tableToOldRow[tableName]
		tableNewRow := tableToNewRow[tableName]
		if equals, err := tableOldRow.Equals(tableNewRow, u.tableMap[tableName]); err == nil {
			if !equals {
				u.rowsAffected++
			}
		} else {
			return err
		}
	}
	return nil
}

func (u *updateJoinRowHandler) okResult() types.OkResult {
	return types.OkResult{
		RowsAffected: uint64(u.rowsAffected),
		Info: plan.UpdateInfo{
			Matched:  u.rowsMatched,
			Updated:  u.rowsAffected,
			Warnings: 0,
		},
	}
}

func (u *updateJoinRowHandler) RowsMatched() int64 {
	return int64(u.rowsMatched)
}

type deleteRowHandler struct {
	rowsAffected int
}

func (u *deleteRowHandler) handleRowUpdate(row sql.Row) error {
	u.rowsAffected++
	return nil
}

func (u *deleteRowHandler) okResult() types.OkResult {
	return types.NewOkResult(u.rowsAffected)
}

type accumulatorIter struct {
	iter             sql.RowIter
	once             sync.Once
	updateRowHandler accumulatorRowHandler
}

func getRowHandler(clientFoundRowsToggled bool, iter sql.RowIter) accumulatorRowHandler {
	switch i := iter.(type) {
	case *plan.TableEditorIter:
		return getRowHandler(clientFoundRowsToggled, i.InnerIter())
	case *plan.CheckpointingTableEditorIter:
		return getRowHandler(clientFoundRowsToggled, i.InnerIter())
	case *ProjectIter:
		return getRowHandler(clientFoundRowsToggled, i.childIter)
	case *triggerIter:
		return getRowHandler(clientFoundRowsToggled, i.child)
	case *blockIter:
		return getRowHandler(clientFoundRowsToggled, i.repIter)
	case *updateIter:
		// it's possible that there's an updateJoinIter that's not the immediate child of updateIter
		rowHandler := getRowHandler(clientFoundRowsToggled, i.childIter)
		if rowHandler != nil {
			return rowHandler
		}
		sch := i.schema
		// special case for foreign keys; plan.ForeignKeyHandler.Schema() returns original schema
		if fkHandler, isFk := i.updater.(*plan.ForeignKeyHandler); isFk {
			sch = fkHandler.Sch
		}
		return &updateRowHandler{schema: sch, clientFoundRowsCapability: clientFoundRowsToggled}
	case *updateJoinIter:
		rowHandler := &updateJoinRowHandler{
			joinSchema: i.joinSchema,
			tableMap:   plan.RecreateTableSchemaFromJoinSchema(i.joinSchema),
			updaterMap: i.updaters,
		}
		i.accumulator = rowHandler
		return rowHandler
	case *insertIter:
		if i.replacer != nil {
			return &replaceRowHandler{}
		}
		if i.updater != nil {
			return &onDuplicateUpdateHandler{schema: i.schema, clientFoundRowsCapability: clientFoundRowsToggled}
		}
		return &insertRowHandler{}
	case *deleteIter:
		return &deleteRowHandler{}
	default:
		return nil
	}
}

func AddAccumulatorIter(ctx *sql.Context, iter sql.RowIter) (sql.RowIter, sql.Schema) {
	switch i := iter.(type) {
	case sql.MutableRowIter:
		childIter := i.GetChildIter()
		childIter, sch := AddAccumulatorIter(ctx, childIter)
		return i.WithChildIter(childIter), sch
	default:
		clientFoundRowsToggled := (ctx.Client().Capabilities & mysql.CapabilityClientFoundRows) > 0
		rowHandler := getRowHandler(clientFoundRowsToggled, iter)
		if rowHandler == nil {
			return iter, nil
		}
		return &accumulatorIter{
			iter:             iter,
			updateRowHandler: rowHandler,
		}, types.OkResultSchema
	}
}

func (a *accumulatorIter) Next(ctx *sql.Context) (r sql.Row, err error) {
	run := false
	a.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	// We close our child iterator before returning any results. In
	// particular, the LOAD DATA source iterator needs to be closed before
	// results are returned.
	defer func() {
		cerr := a.iter.Close(ctx)
		if err == nil {
			err = cerr
		}
	}()

	for {
		row, err := a.iter.Next(ctx)
		igErr, isIg := err.(sql.IgnorableError)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if err == io.EOF {
			// TODO: The information flow here is pretty gnarly. We
			// set some session variables based on the result, and
			// we actually use a session variable to set
			// InsertID. This should be improved.

			// UPDATE statements also set FoundRows to the number of rows that
			// matched the WHERE clause, same as a SELECT.
			if ma, ok := a.updateRowHandler.(matchingAccumulator); ok {
				ctx.SetLastQueryInfoInt(sql.FoundRows, ma.RowsMatched())
			}

			res := a.updateRowHandler.okResult() // TODO: Should add warnings here

			// For some update accumulators, we don't accurately track the last insert ID in the handler and need to set
			// it manually in the result by getting it from the session. This doesn't work correctly in all cases and needs
			// to be fixed. See comment in buildRowUpdateAccumulator in rowexec/dml.go
			switch a.updateRowHandler.(type) {
			case *onDuplicateUpdateHandler, *replaceRowHandler, *insertRowHandler:
				lastInsertId := ctx.Session.GetLastQueryInfoInt(sql.LastInsertId)
				res.InsertID = uint64(lastInsertId)
			}

			// By definition, ROW_COUNT() is equal to RowsAffected.
			ctx.SetLastQueryInfoInt(sql.RowCount, int64(res.RowsAffected))

			return sql.NewRow(res), nil
		} else if isIg {
			if ui, ok := a.updateRowHandler.(updateIgnoreAccumulatorRowHandler); ok {
				err = ui.handleRowUpdateWithIgnore(igErr.OffendingRow, true)
				if err != nil {
					return nil, err
				}
			}
		} else if err != nil {
			return nil, err
		} else {
			err = a.updateRowHandler.handleRowUpdate(row)
			if err != nil {
				return nil, err
			}
		}
	}
}

func (a *accumulatorIter) Close(ctx *sql.Context) error {
	return nil
}

type matchingAccumulator interface {
	RowsMatched() int64
}

type updateSourceIter struct {
	childIter   sql.RowIter
	updateExprs []sql.Expression
	tableSchema sql.Schema
	ignore      bool
}

func (u *updateSourceIter) Next(ctx *sql.Context) (sql.Row, error) {
	oldRow, err := u.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	newRow, err := applyUpdateExpressionsWithIgnore(ctx, u.updateExprs, u.tableSchema, oldRow, u.ignore)
	if err != nil {
		return nil, err
	}

	// Reduce the row to the length of the schema. The length can differ when some update values come from an outer
	// scope, which will be the first N values in the row.
	// TODO: handle this in the analyzer instead?
	expectedSchemaLen := len(u.tableSchema)
	if expectedSchemaLen < oldRow.Len() {
		oldRow = oldRow.Subslice(oldRow.Len()-expectedSchemaLen, oldRow.Len())
		newRow = newRow.Subslice(newRow.Len()-expectedSchemaLen, newRow.Len())
	}

	return oldRow.Append(newRow), nil
}

func (u *updateSourceIter) Close(ctx *sql.Context) error {
	return u.childIter.Close(ctx)
}
