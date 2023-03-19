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

	"github.com/gabereiser/go-mysql-server/sql"
)

// Block represents a collection of statements that should be executed in sequence.
type Block struct {
	statements []sql.Node
	rowIterSch sql.Schema // This is set during RowIter, as the schema is unknown until iterating over the statements.
}

// RepresentsBlock is an interface that defines whether a node contains a Block node, or contains multiple child
// statements similar to a block node. As a rule of thumb, if a parent node depends upon a child node, either explicitly
// or implicitly, then it does not represent a Block.
type RepresentsBlock interface {
	sql.Node
	implementsRepresentsBlock()
}

// RepresentsLabeledBlock is an interface that defines whether a node represents a Block node, while also carrying a
// label that may be referenced by statements within the block (such as LEAVE, ITERATE, etc.). Some statements that use
// labels only look for labels on statements that loop (such as LOOP and REPEAT), so there's an additional function
// to check whether this also represents a loop.
type RepresentsLabeledBlock interface {
	RepresentsBlock
	GetBlockLabel(ctx *sql.Context) string
	RepresentsLoop() bool
}

// RepresentsScope is an interface that defines whether a node represents a new scope. Scopes define boundaries that
// are used for variable resolution and control flow modification (via condition handling, etc.).
type RepresentsScope interface {
	RepresentsBlock
	implementsRepresentsScope()
}

var _ sql.Node = (*Block)(nil)
var _ sql.DebugStringer = (*Block)(nil)
var _ RepresentsBlock = (*Block)(nil)

// NewBlock creates a new *Block node.
func NewBlock(statements []sql.Node) *Block {
	return &Block{statements: statements}
}

// Resolved implements the sql.Node interface.
func (b *Block) Resolved() bool {
	for _, s := range b.statements {
		if !s.Resolved() {
			return false
		}
	}
	return true
}

// String implements the sql.Node interface.
func (b *Block) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("BLOCK")
	var children []string
	for _, s := range b.statements {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// DebugString implements the sql.DebugStringer interface.
func (b *Block) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("BLOCK")
	var children []string
	for _, s := range b.statements {
		children = append(children, sql.DebugString(s))
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// Schema implements the sql.Node interface.
func (b *Block) Schema() sql.Schema {
	return b.rowIterSch
}

// Children implements the sql.Node interface.
func (b *Block) Children() []sql.Node {
	return b.statements
}

// WithChildren implements the sql.Node interface.
func (b *Block) WithChildren(children ...sql.Node) (sql.Node, error) {
	nb := *b
	nb.statements = children
	return &nb, nil
}

// CheckPrivileges implements the interface sql.Node.
func (b *Block) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	for _, statement := range b.statements {
		if !statement.CheckPrivileges(ctx, opChecker) {
			return false
		}
	}
	return true
}

// RowIter implements the sql.Node interface.
func (b *Block) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var returnRows []sql.Row
	var returnNode sql.Node
	var returnSch sql.Schema

	selectSeen := false
	for _, s := range b.statements {
		err := func() error {
			rowCache, disposeFunc := ctx.Memory.NewRowsCache()
			defer disposeFunc()

			var isSelect bool
			subIter, err := s.RowIter(ctx, row)
			if err != nil {
				return err
			}
			subIterNode := s
			subIterSch := s.Schema()
			if blockSubIter, ok := subIter.(BlockRowIter); ok {
				subIterNode = blockSubIter.RepresentingNode()
				subIterSch = blockSubIter.Schema()
			}
			if isSelect = nodeRepresentsSelect(subIterNode); isSelect {
				selectSeen = true
				returnNode = subIterNode
				returnSch = subIterSch
			} else if !selectSeen {
				returnNode = subIterNode
				returnSch = subIterSch
			}

			for {
				newRow, err := subIter.Next(ctx)
				if err == io.EOF {
					err := subIter.Close(ctx)
					if err != nil {
						return err
					}
					if isSelect || !selectSeen {
						returnRows = rowCache.Get()
					}
					break
				} else if err != nil {
					return err
				} else if isSelect || !selectSeen {
					err = rowCache.Add(newRow)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	b.rowIterSch = returnSch
	return &blockIter{
		internalIter: sql.RowsToRowIter(returnRows...),
		repNode:      returnNode,
		sch:          returnSch,
	}, nil
}

// implementsRepresentsBlock implements the RepresentsBlock interface.
func (b *Block) implementsRepresentsBlock() {}

// blockIter is a sql.RowIter that iterates over the given rows.
type blockIter struct {
	internalIter sql.RowIter
	repNode      sql.Node
	sch          sql.Schema
}

var _ BlockRowIter = (*blockIter)(nil)

// Next implements the sql.RowIter interface.
func (i *blockIter) Next(ctx *sql.Context) (sql.Row, error) {
	return i.internalIter.Next(ctx)
}

// Close implements the sql.RowIter interface.
func (i *blockIter) Close(ctx *sql.Context) error {
	return i.internalIter.Close(ctx)
}

// RepresentingNode implements the sql.BlockRowIter interface.
func (i *blockIter) RepresentingNode() sql.Node {
	return i.repNode
}

// Schema implements the sql.BlockRowIter interface.
func (i *blockIter) Schema() sql.Schema {
	return i.sch
}
