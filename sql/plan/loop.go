// Copyright 2022 Dolthub, Inc.
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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Loop represents the LOOP statement, which loops over a set of statements.
type Loop struct {
	Label          string
	Condition      sql.Expression // We continue looping until the condition returns false
	OnceBeforeEval bool           // Whether to run through the statements first before evaluating the condition
	*Block
}

var _ sql.Node = (*Loop)(nil)
var _ sql.DebugStringer = (*Loop)(nil)
var _ sql.Expressioner = (*Loop)(nil)
var _ RepresentsLabeledBlock = (*Loop)(nil)

// NewLoop returns a new *Loop node.
func NewLoop(label string, block *Block) *Loop {
	return &Loop{
		Label:          label,
		Condition:      expression.NewLiteral(true, types.Boolean),
		OnceBeforeEval: true,
		Block:          block,
	}
}

// String implements the interface sql.Node.
func (l *Loop) String() string {
	label := ""
	if len(l.Label) > 0 {
		label = l.Label + ": "
	}
	p := sql.NewTreePrinter()
	_ = p.WriteNode(label + "LOOP")
	var children []string
	for _, s := range l.statements {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// DebugString implements the interface sql.DebugStringer.
func (l *Loop) DebugString() string {
	label := ""
	if len(l.Label) > 0 {
		label = l.Label + ": "
	}
	p := sql.NewTreePrinter()
	_ = p.WriteNode(label + ": LOOP")
	var children []string
	for _, s := range l.statements {
		children = append(children, sql.DebugString(s))
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// Resolved implements the interface sql.Node.
func (l *Loop) Resolved() bool {
	return l.Condition.Resolved() && l.Block.Resolved()
}

// WithChildren implements the interface sql.Node.
func (l *Loop) WithChildren(children ...sql.Node) (sql.Node, error) {
	return &Loop{
		Label:          l.Label,
		Condition:      l.Condition,
		OnceBeforeEval: l.OnceBeforeEval,
		Block:          NewBlock(children),
	}, nil
}

// Expressions implements the interface sql.Node.
func (l *Loop) Expressions() []sql.Expression {
	return []sql.Expression{l.Condition}
}

// WithExpressions implements the interface sql.Node.
func (l *Loop) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(exprs), 1)
	}

	return &Loop{
		Label:          l.Label,
		Condition:      exprs[0],
		OnceBeforeEval: l.OnceBeforeEval,
		Block:          l.Block,
	}, nil
}

// CheckPrivileges implements the interface sql.Node.
func (l *Loop) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return l.Block.CheckPrivileges(ctx, opChecker)
}

// RowIter implements the interface sql.Node.
func (l *Loop) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var blockIter sql.RowIter
	// Currently, acquiring the RowIter will actually run through the loop once, so we abuse this by grabbing the iter
	// only if we're supposed to run through the iter once before evaluating the condition
	if l.OnceBeforeEval {
		var err error
		blockIter, err = loopAcquireRowIter(ctx, row, l.Label, l.Block, true)
		if err != nil {
			return nil, err
		}
	}
	iter := &loopIter{
		block:         l.Block,
		label:         strings.ToLower(l.Label),
		condition:     l.Condition,
		once:          sync.Once{},
		blockIter:     blockIter,
		row:           row,
		loopIteration: 0,
	}
	return iter, nil
}

// GetBlockLabel implements the interface RepresentsLabeledBlock.
func (l *Loop) GetBlockLabel(ctx *sql.Context) string {
	return l.Label
}

// RepresentsLoop implements the interface RepresentsLabeledBlock.
func (l *Loop) RepresentsLoop() bool {
	return true
}

// loopIter is the sql.RowIter of *Loop.
type loopIter struct {
	block         *Block
	label         string
	condition     sql.Expression
	once          sync.Once
	blockIter     sql.RowIter
	row           sql.Row
	loopIteration uint64
}

var _ sql.RowIter = (*loopIter)(nil)

// Next implements the interface sql.RowIter.
func (l *loopIter) Next(ctx *sql.Context) (sql.Row, error) {
	// It's technically valid to make an infinite loop, but we don't want to actually allow that
	const maxIterationCount = 10_000_000_000
	l.loopIteration++
	for ; l.loopIteration < maxIterationCount; l.loopIteration++ {
		// If the condition is false, then we stop evaluation
		condition, err := l.condition.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		conditionBool, err := types.ConvertToBool(condition)
		if err != nil {
			return nil, err
		}
		if !conditionBool {
			return nil, io.EOF
		}

		if l.blockIter == nil {
			var err error
			l.blockIter, err = loopAcquireRowIter(ctx, nil, l.label, l.block, false)
			if err != nil {
				return nil, err
			}
		}

		nextRow, err := l.blockIter.Next(ctx)
		if err != nil {
			restart := false
			if err == io.EOF {
				restart = true
			} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == l.label {
				if controlFlow.IsExit {
					return nil, io.EOF
				} else {
					restart = true
				}
			}

			if restart {
				err = l.blockIter.Close(ctx)
				if err != nil {
					return nil, err
				}
				l.blockIter = nil
				continue
			}
			return nil, err
		}
		return nextRow, nil
	}
	if l.loopIteration >= maxIterationCount {
		return nil, fmt.Errorf("infinite LOOP detected")
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (l *loopIter) Close(ctx *sql.Context) error {
	if l.blockIter != nil {
		return l.blockIter.Close(ctx)
	}
	return nil
}

// loopError is an error used to control a loop's flow.
type loopError struct {
	Label  string
	IsExit bool
}

var _ error = loopError{}

// Error implements the interface error. As long as the analysis step is implemented correctly, this should never be seen.
func (l loopError) Error() string {
	option := "exited"
	if !l.IsExit {
		option = "continued"
	}
	return fmt.Sprintf("should have %s the loop `%s` but it was somehow not found in the call stack", option, l.Label)
}

// loopAcquireRowIter is a helper function for LOOP that conditionally acquires a new sql.RowIter. If a loop exit is
// encountered, `exitIter` determines whether to return an empty iterator or an io.EOF error.
func loopAcquireRowIter(ctx *sql.Context, row sql.Row, label string, block *Block, exitIter bool) (sql.RowIter, error) {
	blockIter, err := block.RowIter(ctx, row)
	if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(label) {
		if controlFlow.IsExit {
			if exitIter {
				return sql.RowsToRowIter(), nil
			} else {
				return nil, io.EOF
			}
		} else {
			err = io.EOF
		}
	}
	if err == io.EOF {
		blockIter = sql.RowsToRowIter()
		err = nil
	}
	return blockIter, err
}
