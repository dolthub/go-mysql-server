// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
)

// Concat is a node that returns everything in Left and then everything in
// Right, but it excludes any results in Right that already appeared in Left.
// Similar to Distinct(Union(...)) but allows Left to return return the same
// row more than once.
type Concat struct {
	BinaryNode
}

var _ sql.Node = (*Concat)(nil)

// NewConcat creates a new Concat node with the given children.
func NewConcat(left, right sql.Node) *Concat {
	return &Concat{
		BinaryNode: BinaryNode{left: left, right: right},
	}
}

func (c *Concat) Schema() sql.Schema {
	ls := c.left.Schema()
	rs := c.right.Schema()
	ret := make([]*sql.Column, len(ls))
	for i := range ls {
		c := *ls[i]
		if i < len(rs) {
			c.Nullable = ls[i].Nullable || rs[i].Nullable
		}
		ret[i] = &c
	}
	return ret
}

// RowIter implements the Node interface.
func (c *Concat) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Concat")
	li, err := c.left.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	i := newConcatIter(
		ctx,
		li,
		func() (sql.RowIter, error) {
			return c.right.RowIter(ctx, row)
		},
	)
	return sql.NewSpanIter(span, i), nil
}

// WithChildren implements the Node interface.
func (c *Concat) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 2)
	}
	return NewConcat(children[0], children[1]), nil
}

func (c Concat) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Concat")
	_ = pr.WriteChildren(c.left.String(), c.right.String())
	return pr.String()
}

func (c Concat) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Concat")
	_ = pr.WriteChildren(sql.DebugString(c.left), sql.DebugString(c.right))
	return pr.String()
}

type concatIter struct {
	ctx      *sql.Context
	cur      sql.RowIter
	inLeft   sql.KeyValueCache
	dispose  sql.DisposeFunc
	nextIter func() (sql.RowIter, error)
}

func newConcatIter(ctx *sql.Context, cur sql.RowIter, nextIter func() (sql.RowIter, error)) *concatIter {
	seen, dispose := ctx.Memory.NewHistoryCache()
	return &concatIter{
		ctx,
		cur,
		seen,
		dispose,
		nextIter,
	}
}

var _ sql.Disposable = (*concatIter)(nil)
var _ sql.RowIter = (*concatIter)(nil)

func (ci *concatIter) Next() (sql.Row, error) {
	for {
		res, err := ci.cur.Next()
		if err == io.EOF {
			if ci.nextIter == nil {
				return nil, io.EOF
			}
			err = ci.cur.Close(ci.ctx)
			if err != nil {
				return nil, err
			}
			ci.cur, err = ci.nextIter()
			ci.nextIter = nil
			if err != nil {
				return nil, err
			}
			res, err = ci.cur.Next()
		}
		if err != nil {
			return nil, err
		}
		hash, err := sql.HashOf(res)
		if err != nil {
			return nil, err
		}
		if ci.nextIter != nil {
			// On Left
			if err := ci.inLeft.Put(hash, struct{}{}); err != nil {
				return nil, err
			}
		} else {
			// On Right
			if _, err := ci.inLeft.Get(hash); err == nil {
				continue
			}
		}
		return res, err
	}
}

func (ci *concatIter) Dispose() {
	ci.dispose()
}

func (ci *concatIter) Close(ctx *sql.Context) error {
	ci.Dispose()
	if ci.cur != nil {
		return ci.cur.Close(ctx)
	} else {
		return nil
	}
}
