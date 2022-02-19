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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TriggerBeginEndBlock represents a BEGIN/END block specific to TRIGGER execution, which has special considerations
// regarding logic execution through the RowIter function.
type TriggerBeginEndBlock struct {
	*BeginEndBlock
}

var _ sql.Node = (*TriggerBeginEndBlock)(nil)
var _ sql.DebugStringer = (*TriggerBeginEndBlock)(nil)

// NewTriggerBeginEndBlock creates a new *TriggerBeginEndBlock node.
func NewTriggerBeginEndBlock(block *BeginEndBlock) *TriggerBeginEndBlock {
	return &TriggerBeginEndBlock{
		BeginEndBlock: block,
	}
}

// WithChildren implements the sql.Node interface.
func (b *TriggerBeginEndBlock) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewTriggerBeginEndBlock(NewBeginEndBlock(NewBlock(children))), nil
}

// CheckPrivileges implements the interface sql.Node.
func (b *TriggerBeginEndBlock) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return b.Block.CheckPrivileges(ctx, opChecker)
}

// RowIter implements the sql.Node interface.
func (b *TriggerBeginEndBlock) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &triggerBlockIter{
		statements: b.statements,
		row:        row,
		once:       &sync.Once{},
	}, nil
}

// triggerBlockIter is the sql.RowIter for TRIGGER BEGIN/END blocks, which operate differently than normal blocks.
type triggerBlockIter struct {
	statements []sql.Node
	row        sql.Row
	once       *sync.Once
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
		subIter, err := s.RowIter(ctx, row)
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
				row = newRow[len(newRow)/2:]
			}
		}
	}

	return row, nil
}

func shouldUseTriggerStatementForReturnRow(stmt sql.Node) bool {
	switch logic := stmt.(type) {
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
		return hasSetField
	default:
		return false
	}
}

// Close implements the sql.RowIter interface.
func (i *triggerBlockIter) Close(*sql.Context) error {
	return nil
}
