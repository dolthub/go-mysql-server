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
	"github.com/dolthub/go-mysql-server/sql"
)

// BeginEndBlock represents a BEGIN/END block.
type BeginEndBlock struct {
	*Block
}

// NewBeginEndBlock creates a new *BeginEndBlock node.
func NewBeginEndBlock(block *Block) *BeginEndBlock {
	return &BeginEndBlock{
		Block: block,
	}
}

var _ sql.Node = (*BeginEndBlock)(nil)
var _ sql.DebugStringer = (*BeginEndBlock)(nil)

// String implements the interface sql.Node.
func (b *BeginEndBlock) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("BEGIN .. END")
	var children []string
	for _, s := range b.statements {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)
	return p.String()
}

// DebugString implements the interface sql.DebugStringer.
func (b *BeginEndBlock) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("BEGIN .. END")
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

// WithHandlerIds returns a new *BeginEndBlock with the given handler IDs set.
func (b *BeginEndBlock) WithHandlerIds(ids []int) *BeginEndBlock {
	newBeginEndBlock := *b
	newBlock := *b.Block
	newBlock.validExitHandlerIds = ids
	newBeginEndBlock.Block = &newBlock
	return &newBeginEndBlock
}
