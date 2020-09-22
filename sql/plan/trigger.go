// Copyright 2020 Liquidata, Inc.
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
	"io"

	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	"github.com/liquidata-inc/go-mysql-server/sql"
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
			Left:  child,
			Right: triggerLogic,
		},
		TriggerEvent:      triggerEvent,
		TriggerTime:       triggerTime,
		TriggerDefinition: triggerDefinition,
	}
}

func (t *TriggerExecutor) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TRIGGER(%s)", t.TriggerDefinition.CreateStatement)
	_ = pr.WriteChildren(t.Left.String())
	return pr.String()
}

func (t *TriggerExecutor) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TRIGGER(%s)", t.TriggerDefinition.CreateStatement)
	_ = pr.WriteChildren(sql.DebugString(t.Left), sql.DebugString(t.Right))
	return pr.String()
}

func (t *TriggerExecutor) Schema() sql.Schema {
	return t.Left.Schema()
}

func (t *TriggerExecutor) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 2)
	}

	return NewTriggerExecutor(children[0], children[1], t.TriggerEvent, t.TriggerTime, t.TriggerDefinition), nil
}

type triggerIter struct {
	child          sql.RowIter
	executionLogic sql.Node
	triggerTime    TriggerTime
	triggerEvent   TriggerEvent
	ctx            *sql.Context
}

func (t *triggerIter) Next() (row sql.Row, returnErr error) {
	childRow, err := t.child.Next()
	if err != nil {
		return nil, err
	}

	// Wrap the execution logic with the current child row before executing it
	// For update triggers that happen before the update, we need to double the input row to get old and new inputs.
	// Update triggers that happen after update don't have this issue, since they wrap an Update node that already has
	// the right schema.
	// TODO: this won't work in all cases for executing multiple triggers
	if t.triggerEvent == UpdateTrigger && t.triggerTime == BeforeTrigger {
		childRow = childRow.Append(childRow)
	}

	logic, err := TransformUp(t.executionLogic, prependRowInPlan(childRow))
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
		err := logicIter.Close()
		if returnErr == nil {
			returnErr = err
		}
	}()

	var logicRow sql.Row
	for {
		row, err := logicIter.Next()
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
	if qp, ok := logic.(*QueryProcess); ok {
		logic = qp.Child
	}

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
	default:
		return false, nil
	}
}

func (t *triggerIter) Close() error {
	return t.child.Close()
}

func (t *TriggerExecutor) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := t.Left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &triggerIter{
		child:          childIter,
		triggerTime:    t.TriggerTime,
		triggerEvent:   t.TriggerEvent,
		executionLogic: t.Right,
		ctx:            ctx,
	}, nil
}
