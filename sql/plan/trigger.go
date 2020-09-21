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

// TriggerExecutor is node that wraps, or is wrapped by, an INSERT, UPDATE, or DELETE node to execute defined trigger
// logic either before or after that operation. When a table has multiple triggers defined, TriggerExecutor nodes can
// wrap each other as well.
type TriggerExecutor struct {
	BinaryNode        // Left = wrapped node, Right = trigger execution logic
	TriggerEvent      TriggerEvent
	TriggerDefinition sql.TriggerDefinition
}

func NewTriggerExecutor(child, triggerLogic sql.Node, triggerEvent TriggerEvent, triggerDefinition sql.TriggerDefinition) *TriggerExecutor {
	return &TriggerExecutor{
		BinaryNode: BinaryNode{
			Left:  child,
			Right: triggerLogic,
		},
		TriggerEvent:      triggerEvent,
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

	return NewTriggerExecutor(children[0], children[1], t.TriggerEvent, t.TriggerDefinition), nil
}

type triggerIter struct {
	child          sql.RowIter
	executionLogic sql.Node
	ctx            *sql.Context
}

func (t *triggerIter) Next() (row sql.Row, returnErr error) {
	childRow, err := t.child.Next()
	if err != nil {
		return nil, err
	}

	// Wrap the execution logic with the current child row before executing it
	// TODO: for update, this needs to get the old row and then the new row both appended
	logic, err := TransformUp(t.executionLogic, prependRowInPlan(childRow))
	if err != nil {
		return nil, err
	}

	// We can't use the same context for trigger logic execution, because it will cause the entire outer context to get
	// canceled once the iterator finishes. Instead, we use a new empty context for each loop iteration.
	subCtx := sql.NewEmptyContext()
	logicIter, err := logic.RowIter(subCtx, childRow)
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
		executionLogic: t.Right,
		ctx:            ctx,
	}, nil
}
