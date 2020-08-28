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
	"sync"
)

type BeginEndBlock struct {
	statements []sql.Node
}

func NewBeginEndBlock(statements []sql.Node) *BeginEndBlock {
	return &BeginEndBlock{statements: statements}
}

func (b *BeginEndBlock) Resolved() bool {
	for _, s := range b.statements{
		if !s.Resolved() {
			return false
		}
	}
	return true
}

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

func (b *BeginEndBlock) Schema() sql.Schema {
	// TODO: some of these actually do return a result (like for stored procedures)
	return nil
}

func (b *BeginEndBlock) Children() []sql.Node {
	return b.statements
}

type blockIter struct {
	statements []sql.Node
	ctx *sql.Context
	row sql.Row
	once *sync.Once
}

func (i *blockIter) Next() (sql.Row, error) {
	run := false
	i.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	for _, s := range i.statements {
		subIter, err := s.RowIter(i.ctx, i.row)
		if err != nil {
			return nil, err
		}

		for {
			_, err := subIter.Next()
			if err == io.EOF {
				err := subIter.Close()
				if err != nil {
					return nil, err
				}
				break
			} else if err != nil {
				return nil, err
			}
		}
	}

	return nil, io.EOF
}

func (i *blockIter) Close() error {
	return nil
}

func (b *BeginEndBlock) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &blockIter{
		statements: b.statements,
		row:        row,
		once:       &sync.Once{},
	}, nil
}

func (b *BeginEndBlock) WithChildren(node ...sql.Node) (sql.Node, error) {
	return NewBeginEndBlock(node), nil
}