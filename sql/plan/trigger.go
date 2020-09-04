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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"io"
)

type TriggerEvent string

const (
	InsertTrigger TriggerEvent = "insert"
	UpdateTrigger = "update"
	DeleteTrigger = "delete"
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
	child sql.RowIter
	executionLogic sql.Node
	ctx *sql.Context
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

	logicIter, err := logic.RowIter(t.ctx, childRow)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := logicIter.Close()
		if returnErr != nil {
			returnErr = err
		}
	}()

	for {
		_, err := logicIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return childRow, nil
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