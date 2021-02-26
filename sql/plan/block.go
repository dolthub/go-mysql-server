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
)

// Block represents a collection of statements that should be executed in sequence.
type Block struct {
	statements []sql.Node
}

var _ sql.Node = (*Block)(nil)
var _ sql.DebugStringer = (*Block)(nil)

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
	// TODO: some of these actually do return a result (like for stored procedures)
	return nil
}

// Children implements the sql.Node interface.
func (b *Block) Children() []sql.Node {
	return b.statements
}

// WithChildren implements the sql.Node interface.
func (b *Block) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewBlock(children), nil
}

// RowIter implements the sql.Node interface.
func (b *Block) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &blockIter{
		statements: b.statements,
		row:        row,
		ctx:        ctx,
		once:       &sync.Once{},
	}, nil
}

// blockIter is a sql.RowIter that iterates over the given rows.
type blockIter struct {
	statements []sql.Node
	ctx        *sql.Context
	row        sql.Row
	once       *sync.Once
}

var _ sql.RowIter = (*blockIter)(nil)

// Next implements the sql.RowIter interface.
func (i *blockIter) Next() (sql.Row, error) {
	//TODO: copied from BeginEndBlock, need to change this to behave properly
	run := false
	i.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	if len(i.statements) == 0 {
		return nil, io.EOF
	}
	row := i.row
	for _, s := range i.statements {
		subIter, err := s.RowIter(i.ctx, row)
		if err != nil {
			return nil, err
		}

		for {
			newRow, err := subIter.Next()
			if err == io.EOF {
				err := subIter.Close(i.ctx)
				if err != nil {
					return nil, err
				}
				break
			} else if err != nil {
				return nil, err
			}
			row = newRow[len(newRow)/2:]
		}
	}

	return row, nil
}

// Close implements the sql.RowIter interface.
func (i *blockIter) Close(*sql.Context) error {
	return nil
}
