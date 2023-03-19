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
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
)

// BeginEndBlock represents a BEGIN/END block.
type BeginEndBlock struct {
	*Block
	Label string
	pRef  *expression.ProcedureReference
}

// NewBeginEndBlock creates a new *BeginEndBlock node.
func NewBeginEndBlock(label string, block *Block) *BeginEndBlock {
	return &BeginEndBlock{
		Block: block,
		Label: label,
	}
}

var _ sql.Node = (*BeginEndBlock)(nil)
var _ sql.DebugStringer = (*BeginEndBlock)(nil)
var _ expression.ProcedureReferencable = (*BeginEndBlock)(nil)
var _ RepresentsLabeledBlock = (*BeginEndBlock)(nil)
var _ RepresentsScope = (*BeginEndBlock)(nil)

// String implements the interface sql.Node.
func (b *BeginEndBlock) String() string {
	label := ""
	if len(b.Label) > 0 {
		label = b.Label + ": "
	}
	p := sql.NewTreePrinter()
	_ = p.WriteNode(label + "BEGIN .. END")
	var children []string
	for _, s := range b.statements {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// DebugString implements the interface sql.DebugStringer.
func (b *BeginEndBlock) DebugString() string {
	label := ""
	if len(b.Label) > 0 {
		label = b.Label + ": "
	}
	p := sql.NewTreePrinter()
	_ = p.WriteNode(label + "BEGIN .. END")
	var children []string
	for _, s := range b.statements {
		children = append(children, sql.DebugString(s))
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// WithChildren implements the interface sql.Node.
func (b *BeginEndBlock) WithChildren(children ...sql.Node) (sql.Node, error) {
	newBeginEndBlock := *b
	newBlock := *b.Block
	newBlock.statements = children
	newBeginEndBlock.Block = &newBlock
	return &newBeginEndBlock, nil
}

// CheckPrivileges implements the interface sql.Node.
func (b *BeginEndBlock) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return b.Block.CheckPrivileges(ctx, opChecker)
}

// WithParamReference implements the interface expression.ProcedureReferencable.
func (b *BeginEndBlock) WithParamReference(pRef *expression.ProcedureReference) sql.Node {
	nb := *b
	nb.pRef = pRef
	return &nb
}

// implementsRepresentsScope implements the interface RepresentsScope.
func (b *BeginEndBlock) implementsRepresentsScope() {}

// GetBlockLabel implements the interface RepresentsLabeledBlock.
func (b *BeginEndBlock) GetBlockLabel(ctx *sql.Context) string {
	return b.Label
}

// RepresentsLoop implements the interface RepresentsLabeledBlock.
func (b *BeginEndBlock) RepresentsLoop() bool {
	return false
}

// RowIter implements the interface sql.Node.
func (b *BeginEndBlock) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	b.pRef.PushScope()
	rowIter, err := b.Block.RowIter(ctx, row)
	if err != nil {
		if exitErr, ok := err.(expression.ProcedureBlockExitError); ok && b.pRef.CurrentHeight() == int(exitErr) {
			err = nil
		} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(b.Label) {
			if controlFlow.IsExit {
				err = nil
			} else {
				err = fmt.Errorf("encountered ITERATE on BEGIN...END, which should should have been caught by the analyzer")
			}
		}
		if nErr := b.pRef.PopScope(ctx); err == nil && nErr != nil {
			err = nErr
		}
		return sql.RowsToRowIter(), err
	}
	return &beginEndIter{
		BeginEndBlock: b,
		rowIter:       rowIter,
	}, nil
}

// beginEndIter is the sql.RowIter of *BeginEndBlock.
type beginEndIter struct {
	*BeginEndBlock
	rowIter sql.RowIter
}

var _ sql.RowIter = (*beginEndIter)(nil)

// Next implements the interface sql.RowIter.
func (b *beginEndIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := b.rowIter.Next(ctx)
	if err != nil {
		if exitErr, ok := err.(expression.ProcedureBlockExitError); ok && b.pRef.CurrentHeight() == int(exitErr) {
			err = io.EOF
		} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(b.Label) {
			if controlFlow.IsExit {
				err = nil
			} else {
				err = fmt.Errorf("encountered ITERATE on BEGIN...END, which should should have been caught by the analyzer")
			}
		}
		if nErr := b.pRef.PopScope(ctx); nErr != nil && err == io.EOF {
			err = nErr
		}
		return nil, err
	}
	return row, nil
}

// Close implements the interface sql.RowIter.
func (b *beginEndIter) Close(ctx *sql.Context) error {
	return b.rowIter.Close(ctx)
}
