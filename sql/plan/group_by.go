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

	"github.com/cespare/xxhash"
	opentracing "github.com/opentracing/opentracing-go"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
)

// ErrGroupBy is returned when the aggregation is not supported.
var ErrGroupBy = errors.NewKind("group by aggregation '%v' not supported")

// GroupBy groups the rows by some expressions.
type GroupBy struct {
	UnaryNode
	SelectedExprs []sql.Expression
	GroupByExprs  []sql.Expression
}

// NewGroupBy creates a new GroupBy node. Like Project, GroupBy is a top-level node, and contains all the fields that
// will appear in the output of the query. Some of these fields may be aggregate functions, some may be columns or
// other expressions. Unlike a project, the GroupBy also has a list of group-by expressions, which usually also appear
// in the list of selected expressions.
func NewGroupBy(selectedExprs, groupByExprs []sql.Expression, child sql.Node) *GroupBy {
	return &GroupBy{
		UnaryNode:     UnaryNode{Child: child},
		SelectedExprs: selectedExprs,
		GroupByExprs:  groupByExprs,
	}
}

// Resolved implements the Resolvable interface.
func (g *GroupBy) Resolved() bool {
	return g.UnaryNode.Child.Resolved() &&
		expression.ExpressionsResolved(g.SelectedExprs...) &&
		expression.ExpressionsResolved(g.GroupByExprs...)
}

// Schema implements the Node interface.
func (g *GroupBy) Schema() sql.Schema {
	var s = make(sql.Schema, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}

		s[i] = &sql.Column{
			Name:     name,
			Type:     e.Type(),
			Nullable: e.IsNullable(),
			Source:   table,
		}
	}

	return s
}

// RowIter implements the Node interface.
func (g *GroupBy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.GroupBy", opentracing.Tags{
		"groupings":  len(g.GroupByExprs),
		"aggregates": len(g.SelectedExprs),
	})

	i, err := g.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	var iter sql.RowIter
	if len(g.GroupByExprs) == 0 {
		iter = newGroupByIter(ctx, g.SelectedExprs, i)
	} else {
		iter = newGroupByGroupingIter(ctx, g.SelectedExprs, g.GroupByExprs, i)
	}

	return sql.NewSpanIter(span, iter), nil
}

// WithChildren implements the Node interface.
func (g *GroupBy) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 1)
	}

	return NewGroupBy(g.SelectedExprs, g.GroupByExprs, children[0]), nil
}

// WithExpressions implements the Node interface.
func (g *GroupBy) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	expected := len(g.SelectedExprs) + len(g.GroupByExprs)
	if len(exprs) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(exprs), expected)
	}

	agg := make([]sql.Expression, len(g.SelectedExprs))
	copy(agg, exprs[:len(g.SelectedExprs)])

	grouping := make([]sql.Expression, len(g.GroupByExprs))
	copy(grouping, exprs[len(g.SelectedExprs):])

	return NewGroupBy(agg, grouping, g.Child), nil
}

func (g *GroupBy) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GroupBy")

	var selectedExprs = make([]string, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		selectedExprs[i] = e.String()
	}

	var grouping = make([]string, len(g.GroupByExprs))
	for i, g := range g.GroupByExprs {
		grouping[i] = g.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("SelectedExprs(%s)", strings.Join(selectedExprs, ", ")),
		fmt.Sprintf("Grouping(%s)", strings.Join(grouping, ", ")),
		g.Child.String(),
	)
	return pr.String()
}

func (g *GroupBy) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GroupBy")

	var selectedExprs = make([]string, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		selectedExprs[i] = sql.DebugString(e)
	}

	var grouping = make([]string, len(g.GroupByExprs))
	for i, g := range g.GroupByExprs {
		grouping[i] = sql.DebugString(g)
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("SelectedExprs(%s)", strings.Join(selectedExprs, ", ")),
		fmt.Sprintf("Grouping(%s)", strings.Join(grouping, ", ")),
		sql.DebugString(g.Child),
	)
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (g *GroupBy) Expressions() []sql.Expression {
	var exprs []sql.Expression
	exprs = append(exprs, g.SelectedExprs...)
	exprs = append(exprs, g.GroupByExprs...)
	return exprs
}

type groupByIter struct {
	selectedExprs []sql.Expression
	child         sql.RowIter
	ctx           *sql.Context
	buf           []sql.AggregationBuffer
	done          bool
}

func newGroupByIter(ctx *sql.Context, selectedExprs []sql.Expression, child sql.RowIter) *groupByIter {
	return &groupByIter{
		selectedExprs: selectedExprs,
		child:         child,
		ctx:           ctx,
		buf:           make([]sql.AggregationBuffer, len(selectedExprs)),
	}
}

func (i *groupByIter) Next() (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	i.done = true

	var err error
	for j, a := range i.selectedExprs {
		i.buf[j], err = newAggregationBuffer(a)
		if err != nil {
			return nil, err
		}
	}

	for {
		row, err := i.child.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := updateBuffers(i.ctx, i.buf, row); err != nil {
			return nil, err
		}
	}

	return evalBuffers(i.ctx, i.buf)
}

func (i *groupByIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.buf = nil
	return i.child.Close(ctx)
}

func (i *groupByIter) Dispose() {
	for _, b := range i.buf {
		b.Dispose()
	}
}

type groupByGroupingIter struct {
	selectedExprs []sql.Expression
	groupByExprs  []sql.Expression
	aggregations  sql.KeyValueCache
	keys          []uint64
	pos           int
	child         sql.RowIter
	ctx           *sql.Context
	dispose       sql.DisposeFunc
}

func newGroupByGroupingIter(
	ctx *sql.Context,
	selectedExprs, groupByExprs []sql.Expression,
	child sql.RowIter,
) *groupByGroupingIter {
	return &groupByGroupingIter{
		selectedExprs: selectedExprs,
		groupByExprs:  groupByExprs,
		child:         child,
		ctx:           ctx,
	}
}

func (i *groupByGroupingIter) Next() (sql.Row, error) {
	if i.aggregations == nil {
		i.aggregations, i.dispose = i.ctx.Memory.NewHistoryCache()
		if err := i.compute(); err != nil {
			return nil, err
		}
	}

	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	buffers, err := i.get(i.keys[i.pos])
	if err != nil {
		return nil, err
	}
	i.pos++
	return evalBuffers(i.ctx, buffers)
}

func (i *groupByGroupingIter) compute() error {
	for {
		row, err := i.child.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, err := groupingKey(i.ctx, i.groupByExprs, row)
		if err != nil {
			return err
		}

		b, err := i.get(key)
		if sql.ErrKeyNotFound.Is(err) {
			b = make([]sql.AggregationBuffer, len(i.selectedExprs))
			for j, a := range i.selectedExprs {
				b[j], err = newAggregationBuffer(a)
				if err != nil {
					return err
				}
			}

			if err := i.aggregations.Put(key, b); err != nil {
				return err
			}

			i.keys = append(i.keys, key)
		} else if err != nil {
			return err
		}

		err = updateBuffers(i.ctx, b, row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *groupByGroupingIter) get(key uint64) ([]sql.AggregationBuffer, error) {
	v, err := i.aggregations.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.([]sql.AggregationBuffer), err
}

func (i *groupByGroupingIter) put(key uint64, val []sql.AggregationBuffer) error {
	return i.aggregations.Put(key, val)
}

func (i *groupByGroupingIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.aggregations = nil
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}

	return i.child.Close(ctx)
}

func (i *groupByGroupingIter) Dispose() {
	for _, k := range i.keys {
		bs, _ := i.get(k)
		if bs != nil {
			for _, b := range bs {
				b.Dispose()
			}
		}
	}
}

func groupingKey(
	ctx *sql.Context,
	exprs []sql.Expression,
	row sql.Row,
) (uint64, error) {
	hash := xxhash.New()
	for _, expr := range exprs {
		v, err := expr.Eval(ctx, row)
		if err != nil {
			return 0, err
		}
		_, err = hash.Write(([]byte)(fmt.Sprintf("%#v,", v)))
		if err != nil {
			return 0, err
		}
	}

	return hash.Sum64(), nil
}

func newAggregationBuffer(expr sql.Expression) (sql.AggregationBuffer, error) {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.NewBuffer()
	default:
		// The semantics for a non-aggregation in a group by node is Last.
		return aggregation.NewLast(expr).NewBuffer()
	}
}

func updateBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
	row sql.Row,
) error {
	for _, b := range buffers {
		if err := b.Update(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func evalBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
) (sql.Row, error) {
	var row = make(sql.Row, len(buffers))

	var err error
	for i, b := range buffers {
		row[i], err = b.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}
