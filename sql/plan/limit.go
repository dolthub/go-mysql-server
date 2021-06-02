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

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/dolthub/go-mysql-server/sql"
)

// Limit is a node that only allows up to N rows to be retrieved.
type Limit struct {
	UnaryNode
	Limit         sql.Expression
	CalcFoundRows bool
}

// NewLimit creates a new Limit node with the given size.
func NewLimit(size sql.Expression, child sql.Node) *Limit {
	return &Limit{
		UnaryNode: UnaryNode{Child: child},
		Limit:     size,
	}
}

// Expressions implements sql.Expressioner
func (l *Limit) Expressions() []sql.Expression {
	return []sql.Expression{l.Limit}
}

// WithExpressions implements sql.Expressioner
func (l Limit) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(exprs), 1)
	}
	nl := &l
	nl.Limit = exprs[0]
	return nl, nil
}

// Resolved implements the Resolvable interface.
func (l *Limit) Resolved() bool {
	return l.UnaryNode.Child.Resolved() && l.Limit.Resolved()
}

// RowIter implements the Node interface.
func (l *Limit) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Limit", opentracing.Tag{Key: "limit", Value: l.Limit})

	limit, err := getInt64Value(ctx, l.Limit)
	if err != nil {
		return nil, err
	}

	childIter, err := l.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &limitIter{
		l:         l,
		limit:     limit,
		childIter: childIter,
	}), nil
}

// getInt64Value returns the int64 literal value in the expression given, or an error with the errStr given if it
// cannot.
func getInt64Value(ctx *sql.Context, expr sql.Expression) (int64, error) {
	i, err := expr.Eval(ctx, nil)
	if err != nil {
		return 0, err
	}

	switch i := i.(type) {
	case int:
		return int64(i), nil
	case int8:
		return int64(i), nil
	case int16:
		return int64(i), nil
	case int32:
		return int64(i), nil
	case int64:
		return i, nil
	case uint:
		return int64(i), nil
	case uint8:
		return int64(i), nil
	case uint16:
		return int64(i), nil
	case uint32:
		return int64(i), nil
	case uint64:
		return int64(i), nil
	default:
		// analyzer should catch this already
		panic(fmt.Sprintf("Unsupported type for limit %T", i))
	}
}

// WithChildren implements the Node interface.
func (l *Limit) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	nl := *l
	nl.Child = children[0]
	return &nl, nil
}

func (l Limit) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Limit(%s)", l.Limit)
	_ = pr.WriteChildren(l.Child.String())
	return pr.String()
}

func (l Limit) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Limit(%s)", l.Limit)
	_ = pr.WriteChildren(sql.DebugString(l.Child))
	return pr.String()
}

type limitIter struct {
	l          *Limit
	currentPos int64
	childIter  sql.RowIter
	limit      int64
}

func (li *limitIter) Next() (sql.Row, error) {
	if li.currentPos >= li.limit {
		// If we were asked to calc all found rows, then when we are past the limit we iterate over the rest of the
		// result set to count it
		if li.l.CalcFoundRows {
			for {
				_, err := li.childIter.Next()
				if err != nil {
					return nil, err
				}
				li.currentPos++
			}
		}

		return nil, io.EOF
	}

	childRow, err := li.childIter.Next()
	li.currentPos++
	if err != nil {
		return nil, err
	}

	return childRow, nil
}

func (li *limitIter) Close(ctx *sql.Context) error {
	err := li.childIter.Close(ctx)
	if err != nil {
		return err
	}

	if li.l.CalcFoundRows {
		ctx.SetLastQueryInfo(sql.FoundRows, li.currentPos)
	}
	return nil
}
