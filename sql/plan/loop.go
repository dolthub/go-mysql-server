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

	"github.com/dolthub/go-mysql-server/sql"
)

// Loop represents the LOOP statement, which loops over a set of statements.
type Loop struct {
	Label string
	*Block
}

var _ sql.Node = (*Loop)(nil)
var _ sql.DebugStringer = (*Loop)(nil)

// NewLoop returns a new *Loop node.
func NewLoop(label string, block *Block) *Loop {
	return &Loop{
		Label: label,
		Block: block,
	}
}

// String implements the interface sql.Node.
func (l *Loop) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode(l.Label + ": LOOP")
	var children []string
	for _, s := range l.statements {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// DebugString implements the interface sql.DebugStringer.
func (l *Loop) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode(l.Label + ": LOOP")
	var children []string
	for _, s := range l.statements {
		children = append(children, sql.DebugString(s))
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// WithChildren implements the interface sql.Node.
func (l *Loop) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewLoop(l.Label, NewBlock(children)), nil
}

// CheckPrivileges implements the interface sql.Node.
func (l *Loop) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return l.Block.CheckPrivileges(ctx, opChecker)
}

// RowIter implements the interface sql.Node.
func (l *Loop) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	blockIter, err := l.Block.RowIter(ctx, row)
	if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(l.Label) {
		if controlFlow.IsExit {
			return sql.RowsToRowIter(), nil
		} else {
			err = io.EOF
		}
	}
	if err == io.EOF {
		blockIter = sql.RowsToRowIter()
	} else if err != nil {
		return nil, err
	}
	return &loopIter{
		block:         l.Block,
		label:         strings.ToLower(l.Label),
		blockIter:     blockIter,
		row:           row,
		loopIteration: 0,
	}, nil
}

// loopIter is the sql.RowIter of *Loop.
type loopIter struct {
	block         *Block
	label         string
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
				l.blockIter, err = l.block.RowIter(ctx, l.row)
				if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == l.label {
					if controlFlow.IsExit {
						return nil, io.EOF
					} else {
						err = io.EOF
					}
				}
				if err == io.EOF {
					l.blockIter = sql.RowsToRowIter()
				} else if err != nil {
					return nil, err
				}
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
