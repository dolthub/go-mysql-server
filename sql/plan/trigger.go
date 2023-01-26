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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type TriggerEvent string

const (
	InsertTrigger TriggerEvent = "insert"
	UpdateTrigger              = "update"
	DeleteTrigger              = "delete"
)

type TriggerTime string

const (
	BeforeTrigger TriggerTime = "before"
	AfterTrigger              = "after"
)

// TriggerExecutor is node that wraps, or is wrapped by, an INSERT, UPDATE, or DELETE node to execute defined trigger
// logic either before or after that operation. When a table has multiple triggers defined, TriggerExecutor nodes can
// wrap each other as well.
type TriggerExecutor struct {
	BinaryNode        // Left = wrapped node, Right = trigger execution logic
	TriggerEvent      TriggerEvent
	TriggerTime       TriggerTime
	TriggerDefinition sql.TriggerDefinition
}

func NewTriggerExecutor(child, triggerLogic sql.Node, triggerEvent TriggerEvent, triggerTime TriggerTime, triggerDefinition sql.TriggerDefinition) *TriggerExecutor {
	return &TriggerExecutor{
		BinaryNode: BinaryNode{
			left:  child,
			right: triggerLogic,
		},
		TriggerEvent:      triggerEvent,
		TriggerTime:       triggerTime,
		TriggerDefinition: triggerDefinition,
	}
}

func (t *TriggerExecutor) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Trigger(%s)", t.TriggerDefinition.CreateStatement)
	_ = pr.WriteChildren(t.left.String())
	return pr.String()
}

func (t *TriggerExecutor) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Trigger(%s)", t.TriggerDefinition.CreateStatement)
	_ = pr.WriteChildren(sql.DebugString(t.left), sql.DebugString(t.right))
	return pr.String()
}

func (t *TriggerExecutor) Schema() sql.Schema {
	return t.left.Schema()
}

func (t *TriggerExecutor) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 2)
	}

	return NewTriggerExecutor(children[0], children[1], t.TriggerEvent, t.TriggerTime, t.TriggerDefinition), nil
}

// CheckPrivileges implements the interface sql.Node.
func (t *TriggerExecutor) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// TODO: Figure out exactly how triggers work, not exactly clear whether trigger creator AND user needs the privileges
	return t.left.CheckPrivileges(ctx, opChecker) && opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(getDatabaseName(t.right), getTableName(t.right), "", sql.PrivilegeType_Trigger))
}

type triggerIter struct {
	child          sql.RowIter
	executionLogic sql.Node
	triggerTime    TriggerTime
	triggerEvent   TriggerEvent
	ctx            *sql.Context
}

// prependRowInPlanForTriggerExecution returns a transformation function that prepends the row given to any row source in a query
// plan. Any source of rows, as well as any node that alters the schema of its children, will be wrapped so that its
// result rows are prepended with the row given.
func prependRowInPlanForTriggerExecution(row sql.Row) func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
	return func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *Project:
			// Only prepend rows for projects that aren't the input to inserts and other triggers
			switch c.Parent.(type) {
			case *InsertInto, *TriggerExecutor:
				return n, transform.SameTree, nil
			default:
				return &prependNode{
					UnaryNode: UnaryNode{Child: n},
					row:       row,
				}, transform.NewTree, nil
			}
		case *ResolvedTable, *IndexedTableAccess:
			return &prependNode{
				UnaryNode: UnaryNode{Child: n},
				row:       row,
			}, transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	}
}

func (t *triggerIter) Next(ctx *sql.Context) (row sql.Row, returnErr error) {
	childRow, err := t.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap the execution logic with the current child row before executing it.
	logic, _, err := transform.NodeWithCtx(t.executionLogic, nil, prependRowInPlanForTriggerExecution(childRow))
	if err != nil {
		return nil, err
	}

	// We don't do anything interesting with this subcontext yet, but it's a good idea to cancel it independently of the
	// parent context if something goes wrong in trigger execution.
	ctx, cancelFunc := t.ctx.NewSubContext()
	defer cancelFunc()

	logicIter, err := logic.RowIter(ctx, childRow)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := logicIter.Close(t.ctx)
		if returnErr == nil {
			returnErr = err
		}
	}()

	var logicRow sql.Row
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
	case *Set:
		hasSetField := false
		for _, expr := range logic.Exprs {
			sql.Inspect(expr.(*expression.SetField).Left, func(e sql.Expression) bool {
				if _, ok := e.(*expression.GetField); ok {
					hasSetField = true
					return false
				}
				return true
			})
		}
		return hasSetField, row[len(row)/2:]
	case *TriggerBeginEndBlock:
		hasSetField := false
		transform.Inspect(logic, func(n sql.Node) bool {
			set, ok := n.(*Set)
			if !ok {
				return true
			}
			for _, expr := range set.Exprs {
				sql.Inspect(expr.(*expression.SetField).Left, func(e sql.Expression) bool {
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

func (t *TriggerExecutor) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := t.left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &triggerIter{
		child:          childIter,
		triggerTime:    t.TriggerTime,
		triggerEvent:   t.TriggerEvent,
		executionLogic: t.right,
		ctx:            ctx,
	}, nil
}

const SavePointName = "__go_mysql_server_starting_savepoint__"

// TriggerRollback is a node that wraps the entire tree iff it contains a trigger, creates a savepoint, and performs a
// rollback if something went wrong during execution
type TriggerRollback struct {
	UnaryNode
}

func NewTriggerRollback(child sql.Node) *TriggerRollback {
	return &TriggerRollback{
		UnaryNode: UnaryNode{Child: child},
	}
}

func (t *TriggerRollback) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}

	return NewTriggerRollback(children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (t *TriggerRollback) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return t.Child.CheckPrivileges(ctx, opChecker)
}

func (t *TriggerRollback) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := t.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	ctx.GetLogger().Tracef("TriggerRollback creating savepoint: %s", SavePointName)

	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil, fmt.Errorf("expected a sql.TransactionSession, but got %T", ctx.Session)
	}

	if err := ts.CreateSavepoint(ctx, ctx.GetTransaction(), SavePointName); err != nil {
		ctx.GetLogger().WithError(err).Errorf("CreateSavepoint failed")
	}

	return &triggerRollbackIter{
		child:        childIter,
		hasSavepoint: true,
	}, nil
}

func (t *TriggerRollback) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TriggerRollback()")
	_ = pr.WriteChildren(t.Child.String())
	return pr.String()
}

func (t *TriggerRollback) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TriggerRollback")
	_ = pr.WriteChildren(sql.DebugString(t.Child))
	return pr.String()
}

type triggerRollbackIter struct {
	child        sql.RowIter
	hasSavepoint bool
}

func (t *triggerRollbackIter) Next(ctx *sql.Context) (row sql.Row, returnErr error) {
	childRow, err := t.child.Next(ctx)

	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil, fmt.Errorf("expected a sql.TransactionSession, but got %T", ctx.Session)
	}

	// Rollback if error occurred
	if err != nil && err != io.EOF {
		if err := ts.RollbackToSavepoint(ctx, ctx.GetTransaction(), SavePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling RollbackToSavePoint during triggerRollbackIter.Next()")
		}
		if err := ts.ReleaseSavepoint(ctx, ctx.GetTransaction(), SavePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling ReleaseSavepoint during triggerRollbackIter.Next()")
		} else {
			t.hasSavepoint = false
		}
	}

	return childRow, err
}

func (t *triggerRollbackIter) Close(ctx *sql.Context) error {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return fmt.Errorf("expected a sql.TransactionSession, but got %T", ctx.Session)
	}

	if t.hasSavepoint {
		if err := ts.ReleaseSavepoint(ctx, ctx.GetTransaction(), SavePointName); err != nil {
			ctx.GetLogger().WithError(err).Errorf("Unexpected error when calling ReleaseSavepoint during triggerRollbackIter.Close()")
		}
		t.hasSavepoint = false
	}
	return t.child.Close(ctx)
}

type NoopTriggerRollback struct {
	UnaryNode
}

func NewNoopTriggerRollback(child sql.Node) *NoopTriggerRollback {
	return &NoopTriggerRollback{
		UnaryNode: UnaryNode{Child: child},
	}
}

func (t *NoopTriggerRollback) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}

	return NewNoopTriggerRollback(children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (t *NoopTriggerRollback) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return t.Child.CheckPrivileges(ctx, opChecker)
}

func (t *NoopTriggerRollback) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return t.Child.RowIter(ctx, row)
}

func (t *NoopTriggerRollback) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TriggerRollback()")
	_ = pr.WriteChildren(t.Child.String())
	return pr.String()
}

func (t *NoopTriggerRollback) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TriggerRollback")
	_ = pr.WriteChildren(sql.DebugString(t.Child))
	return pr.String()
}
