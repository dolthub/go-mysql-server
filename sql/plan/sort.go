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
	"container/heap"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Sort is the sort node.
type Sort struct {
	UnaryNode
	SortFields sql.SortFields
}

// NewSort creates a new Sort node.
func NewSort(sortFields []sql.SortField, child sql.Node) *Sort {
	return &Sort{
		UnaryNode:  UnaryNode{child},
		SortFields: sortFields,
	}
}

var _ sql.Expressioner = (*Sort)(nil)
var _ sql.Node = (*Sort)(nil)
var _ sql.Node2 = (*Sort)(nil)

// Resolved implements the Resolvable interface.
func (s *Sort) Resolved() bool {
	for _, f := range s.SortFields {
		if !f.Column.Resolved() {
			return false
		}
	}
	return s.Child.Resolved()
}

// RowIter implements the Node interface.
func (s *Sort) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := s.UnaryNode.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(ctx, s, i)), nil
}

func (s *Sort) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := s.UnaryNode.Child.(sql.Node2).RowIter2(ctx, f)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(ctx, s, i)).(sql.RowIter2), nil
}

func (s *Sort) String() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(s.SortFields))
	for i, f := range s.SortFields {
		fields[i] = fmt.Sprintf("%s %s", f.Column, f.Order)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(fields, ", "))
	_ = pr.WriteChildren(s.Child.String())
	return pr.String()
}

func (s *Sort) DebugString() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(s.SortFields))
	for i, f := range s.SortFields {
		fields[i] = sql.DebugString(f)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(fields, ", "))
	_ = pr.WriteChildren(sql.DebugString(s.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (s *Sort) Expressions() []sql.Expression {
	// TODO: use shared method
	var exprs = make([]sql.Expression, len(s.SortFields))
	for i, f := range s.SortFields {
		exprs[i] = f.Column
	}
	return exprs
}

// WithChildren implements the Node interface.
func (s *Sort) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	return NewSort(s.SortFields, children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (s *Sort) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return s.Child.CheckPrivileges(ctx, opChecker)
}

// WithExpressions implements the Expressioner interface.
func (s *Sort) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.SortFields) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.SortFields))
	}

	fields := s.SortFields.FromExpressions(exprs...)
	return NewSort(fields, s.Child), nil
}

type sortIter struct {
	s           *Sort
	childIter   sql.RowIter
	childIter2  sql.RowIter2
	sortedRows  []sql.Row
	sortedRows2 []sql.Row2
	idx         int
}

var _ sql.RowIter = (*sortIter)(nil)
var _ sql.RowIter2 = (*sortIter)(nil)

func newSortIter(ctx *sql.Context, s *Sort, child sql.RowIter) *sortIter {
	childIter2, _ := child.(sql.RowIter2)
	return &sortIter{
		s:          s,
		childIter:  child,
		childIter2: childIter2,
		idx:        -1,
	}
}

func (i *sortIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeSortedRows(ctx)
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}

	if i.idx >= len(i.sortedRows) {
		return nil, io.EOF
	}
	row := i.sortedRows[i.idx]
	i.idx++
	return row, nil
}

func (i *sortIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	if i.idx == -1 {
		err := i.computeSortedRows2(ctx)
		if err != nil {
			return err
		}
		i.idx = 0
	}

	if i.idx >= len(i.sortedRows2) {
		return io.EOF
	}

	row := i.sortedRows2[i.idx]
	i.idx++
	frame.Append(row...)

	return nil
}

func (i *sortIter) Close(ctx *sql.Context) error {
	i.sortedRows = nil
	return i.childIter.Close(ctx)
}

func (i *sortIter) computeSortedRows(ctx *sql.Context) error {
	cache, dispose := ctx.Memory.NewRowsCache()
	defer dispose()

	for {
		row, err := i.childIter.Next(ctx)

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cache.Add(row); err != nil {
			return err
		}
	}

	rows := cache.Get()
	sorter := &expression.Sorter{
		SortFields: i.s.SortFields,
		Rows:       rows,
		LastError:  nil,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return sorter.LastError
	}
	i.sortedRows = rows
	return nil
}

func (i *sortIter) computeSortedRows2(ctx *sql.Context) error {
	cache, dispose := ctx.Memory.NewRows2Cache()
	defer dispose()

	f := sql.NewRowFrame()
	defer f.Recycle()

	for {
		f.Clear()
		err := i.childIter2.Next2(ctx, f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cache.Add2(f.Row2Copy()); err != nil {
			return err
		}
	}

	rows := cache.Get2()
	sorter := &expression.Sorter2{
		SortFields: i.s.SortFields,
		Rows:       rows,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return sorter.LastError
	}
	i.sortedRows2 = rows
	return nil
}

// TopN was a sort node that has a limit. It doesn't need to buffer everything,
// but can calculate the top n on the fly.
type TopN struct {
	UnaryNode
	Limit         sql.Expression
	Fields        sql.SortFields
	CalcFoundRows bool
}

// NewTopN creates a new TopN node.
func NewTopN(fields sql.SortFields, limit sql.Expression, child sql.Node) *TopN {
	return &TopN{
		UnaryNode: UnaryNode{child},
		Limit:     limit,
		Fields:    fields,
	}
}

var _ sql.Expressioner = (*TopN)(nil)

// Resolved implements the Resolvable interface.
func (n *TopN) Resolved() bool {
	for _, f := range n.Fields {
		if !f.Column.Resolved() {
			return false
		}
	}
	return n.Child.Resolved()
}

func (n TopN) WithCalcFoundRows(v bool) *TopN {
	n.CalcFoundRows = v
	return &n
}

// RowIter implements the Node interface.
func (n *TopN) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.TopN")
	i, err := n.UnaryNode.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	limit, err := getInt64Value(ctx, n.Limit)
	if err != nil {
		return nil, err
	}
	return sql.NewSpanIter(span, newTopRowsIter(n, limit, i)), nil
}

func (n *TopN) String() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(n.Fields))
	for i, f := range n.Fields {
		fields[i] = fmt.Sprintf("%s %s", f.Column, f.Order)
	}
	_ = pr.WriteNode("TopN(Limit: [%s]; %s)", n.Limit.String(), strings.Join(fields, ", "))
	_ = pr.WriteChildren(n.Child.String())
	return pr.String()
}

func (n *TopN) DebugString() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(n.Fields))
	for i, f := range n.Fields {
		fields[i] = sql.DebugString(f)
	}
	_ = pr.WriteNode("TopN(Limit: [%s]; %s)", sql.DebugString(n.Limit), strings.Join(fields, ", "))
	_ = pr.WriteChildren(sql.DebugString(n.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (n *TopN) Expressions() []sql.Expression {
	exprs := []sql.Expression{n.Limit}
	exprs = append(exprs, n.Fields.ToExpressions()...)
	return exprs
}

// WithChildren implements the Node interface.
func (n *TopN) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	topn := NewTopN(n.Fields, n.Limit, children[0])
	topn.CalcFoundRows = n.CalcFoundRows
	return topn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *TopN) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return n.Child.CheckPrivileges(ctx, opChecker)
}

// WithExpressions implements the Expressioner interface.
func (n *TopN) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(n.Fields)+1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(exprs), len(n.Fields)+1)
	}

	var limit = exprs[0]
	var fields = n.Fields.FromExpressions(exprs[1:]...)

	topn := NewTopN(fields, limit, n.Child)
	topn.CalcFoundRows = n.CalcFoundRows
	return topn, nil
}

type topRowsIter struct {
	n            *TopN
	childIter    sql.RowIter
	limit        int64
	topRows      []sql.Row
	numFoundRows int64
	idx          int
}

func newTopRowsIter(n *TopN, limit int64, child sql.RowIter) *topRowsIter {
	return &topRowsIter{
		n:         n,
		limit:     limit,
		childIter: child,
		idx:       -1,
	}
}

func (i *topRowsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeTopRows(ctx)
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}

	if i.idx >= len(i.topRows) {
		return nil, io.EOF
	}
	row := i.topRows[i.idx]
	i.idx++
	return row, nil
}

func (i *topRowsIter) Close(ctx *sql.Context) error {
	i.topRows = nil

	if i.n.CalcFoundRows {
		ctx.SetLastQueryInfo(sql.FoundRows, i.numFoundRows)
	}

	return i.childIter.Close(ctx)
}

func (i *topRowsIter) computeTopRows(ctx *sql.Context) error {
	topRowsHeap := &expression.TopRowsHeap{
		expression.Sorter{
			SortFields: i.n.Fields,
			Rows:       []sql.Row{},
			LastError:  nil,
			Ctx:        ctx,
		},
	}
	for {
		row, err := i.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		i.numFoundRows++

		heap.Push(topRowsHeap, row)
		if int64(topRowsHeap.Len()) > i.limit {
			heap.Pop(topRowsHeap)
		}
		if topRowsHeap.LastError != nil {
			return topRowsHeap.LastError
		}
	}

	var err error
	i.topRows, err = topRowsHeap.Rows()
	return err
}
