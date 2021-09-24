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
	"io"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
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
func prependRowInPlanForTriggerExecution(row sql.Row) func(c TransformContext) (sql.Node, error) {
	return func(c TransformContext) (sql.Node, error) {
		switch n := c.Node.(type) {
		case *Project:
			// Only prepend rows for projects that aren't the input to inserts and other triggers
			switch c.Parent.(type) {
			case *InsertInto, *TriggerExecutor:
				return n, nil
			default:
				return &prependNode{
					UnaryNode: UnaryNode{Child: n},
					row:       row,
				}, nil
			}
		case *ResolvedTable, *IndexedTableAccess:
			return &prependNode{
				UnaryNode: UnaryNode{Child: n},
				row:       row,
			}, nil
		default:
			return n, nil
		}
	}
}

func (t *triggerIter) Next() (row sql.Row, returnErr error) {
	childRow, err := t.child.Next()
	if err != nil {
		return nil, err
	}

	// Wrap the execution logic with the current child row before executing it.
	logic, err := TransformUpCtx(t.executionLogic, nil, prependRowInPlanForTriggerExecution(childRow))
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
	case *TriggerBeginEndBlock:
		hasSetField := false
		Inspect(logic, func(n sql.Node) bool {
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
