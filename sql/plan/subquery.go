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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Subquery is as an expression whose value is derived by executing a subquery. It must be executed for every row in
// the outer result set. It's in the plan package instead of the expression package because it functions more like a
// plan Node than an expression.
type Subquery struct {
	// The subquery to execute for each row in the outer result set
	Query sql.Node
	// The original verbatim select statement for this subquery
	QueryString string
	// Whether it's safe to cache result values for this subquery
	canCacheResults bool
	// Whether results have been cached
	resultsCached bool
	// Cached results, if any
	cache []interface{}
	// Cached hash results, if any
	hashCache sql.KeyValueCache
	// Dispose function for the cache, if any. This would appear to violate the rule that nodes must be comparable by
	// reflect.DeepEquals, but it's safe in practice because the function is always nil until execution.
	disposeFunc sql.DisposeFunc
	// Mutex to guard the caches
	cacheMu sync.Mutex
}

// NewSubquery returns a new subquery expression.
func NewSubquery(node sql.Node, queryString string) *Subquery {
	return &Subquery{Query: node, QueryString: queryString}
}

var _ sql.NonDeterministicExpression = (*Subquery)(nil)

type StripRowNode struct {
	UnaryNode
	numCols int
}

type stripRowIter struct {
	sql.RowIter
	numCols int
}

func (sri *stripRowIter) Next() (sql.Row, error) {
	r, err := sri.RowIter.Next()
	if err != nil {
		return nil, err
	}
	return r[sri.numCols:], nil
}

func NewStripRowNode(child sql.Node, numCols int) sql.Node {
	return &StripRowNode{UnaryNode: UnaryNode{child}, numCols: numCols}
}

func (srn *StripRowNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := srn.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &stripRowIter{
		childIter,
		srn.numCols,
	}, nil
}

func (srn *StripRowNode) String() string {
	return srn.Child.String()
}

func (srn *StripRowNode) DebugString() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("StripRowNode(%d)", srn.numCols)
	_ = tp.WriteChildren(sql.DebugString(srn.Child))
	return tp.String()
}

func (srn *StripRowNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(srn, len(children), 1)
	}
	return &StripRowNode{
		UnaryNode: UnaryNode{Child: children[0]},
		numCols:   srn.numCols,
	}, nil
}

// prependNode wraps its child by prepending column values onto any result rows
type prependNode struct {
	UnaryNode
	row sql.Row
}

type prependRowIter struct {
	row       sql.Row
	childIter sql.RowIter
}

func (p *prependRowIter) Next() (sql.Row, error) {
	next, err := p.childIter.Next()
	if err != nil {
		return next, err
	}
	return p.row.Append(next), nil
}

func (p *prependRowIter) Close(ctx *sql.Context) error {
	return p.childIter.Close(ctx)
}

func (p *prependNode) String() string {
	return p.Child.String()
}

func (p *prependNode) DebugString() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("Prepend(%s)", sql.FormatRow(p.row))
	_ = tp.WriteChildren(sql.DebugString(p.Child))
	return tp.String()
}

func (p *prependNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := p.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &prependRowIter{
		row:       p.row,
		childIter: childIter,
	}, nil
}

func (p *prependNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return &prependNode{
		UnaryNode: UnaryNode{Child: children[0]},
		row:       p.row,
	}, nil
}

// Eval implements the Expression interface.
func (s *Subquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	s.cacheMu.Lock()
	cached := s.resultsCached
	s.cacheMu.Unlock()

	if cached {
		if len(s.cache) == 0 {
			return nil, nil
		}
		return s.cache[0], nil
	}

	rows, err := s.evalMultiple(ctx, row)
	if err != nil {
		return nil, err
	}

	if len(rows) > 1 {
		return nil, sql.ErrExpectedSingleRow.New()
	}

	if s.canCacheResults {
		s.cacheMu.Lock()
		if !s.resultsCached {
			s.cache, s.resultsCached = rows, true
		}
		s.cacheMu.Unlock()
	}

	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// prependRowInPlan returns a transformation function that prepends the row given to any row source in a query
// plan. Any source of rows, as well as any node that alters the schema of its children, will be wrapped so that its
// result rows are prepended with the row given.
func prependRowInPlan(row sql.Row) func(n sql.Node) (sql.Node, error) {
	return func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *Project, *GroupBy, *Having, *SubqueryAlias, *Window, sql.Table, *ValueDerivedTable:
			return &prependNode{
				UnaryNode: UnaryNode{Child: n},
				row:       row,
			}, nil
		default:
			return n, nil
		}
	}
}

// EvalMultiple returns all rows returned by a subquery.
func (s *Subquery) EvalMultiple(ctx *sql.Context, row sql.Row) ([]interface{}, error) {
	s.cacheMu.Lock()
	cached := s.resultsCached
	s.cacheMu.Unlock()
	if cached {
		return s.cache, nil
	}

	result, err := s.evalMultiple(ctx, row)
	if err != nil {
		return nil, err
	}

	if s.canCacheResults {
		s.cacheMu.Lock()
		if s.resultsCached == false {
			s.cache, s.resultsCached = result, true
		}
		s.cacheMu.Unlock()
	}

	return result, nil
}

func (s *Subquery) evalMultiple(ctx *sql.Context, row sql.Row) ([]interface{}, error) {
	// Any source of rows, as well as any node that alters the schema of its children, needs to be wrapped so that its
	// result rows are prepended with the scope row.
	q, err := TransformUp(s.Query, prependRowInPlan(row))
	if err != nil {
		return nil, err
	}

	iter, err := q.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	// Reduce the result row to the size of the expected schema. This means chopping off the first len(row) columns.
	col := len(row)
	var result []interface{}
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		result = append(result, row[col])
	}

	if err := iter.Close(ctx); err != nil {
		return nil, err
	}

	return result, nil
}

// HashMultiple returns all rows returned by a subquery, backed by a sql.KeyValueCache. Keys are constructed using the
// 64-bit hash of the values stored.
func (s *Subquery) HashMultiple(ctx *sql.Context, row sql.Row) (sql.KeyValueCache, error) {
	s.cacheMu.Lock()
	cached := s.resultsCached && s.hashCache != nil
	s.cacheMu.Unlock()
	if cached {
		return s.hashCache, nil
	}

	result, err := s.evalMultiple(ctx, row)
	if err != nil {
		return nil, err
	}

	if s.canCacheResults {
		s.cacheMu.Lock()
		defer s.cacheMu.Unlock()
		if !s.resultsCached || s.hashCache == nil {
			hashCache, disposeFn := ctx.Memory.NewHistoryCache()
			err = putAllRows(hashCache, result)
			if err != nil {
				return nil, err
			}
			s.cache, s.hashCache, s.disposeFunc, s.resultsCached = result, hashCache, disposeFn, true
		}
		return s.hashCache, nil
	}

	cache := sql.NewMapCache()
	return cache, putAllRows(cache, result)
}

func putAllRows(cache sql.KeyValueCache, vals []interface{}) error {
	for _, val := range vals {
		rowKey, err := sql.HashOf(sql.NewRow(val))
		if err != nil {
			return err
		}
		err = cache.Put(rowKey, val)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsNullable implements the Expression interface.
func (s *Subquery) IsNullable() bool {
	return s.Query.Schema()[0].Nullable
}

func (s *Subquery) String() string {
	return fmt.Sprintf("(%s)", s.Query)
}

func (s *Subquery) DebugString() string {
	return fmt.Sprintf("(%s), cacheable = %t", sql.DebugString(s.Query), s.canCacheResults)
}

// Resolved implements the Expression interface.
func (s *Subquery) Resolved() bool {
	return s.Query.Resolved()
}

// Type implements the Expression interface.
func (s *Subquery) Type() sql.Type {
	// TODO: handle row results (more than one column)
	return s.Query.Schema()[0].Type
}

// WithChildren implements the Expression interface.
func (s *Subquery) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}
	return s, nil
}

// Children implements the Expression interface.
func (s *Subquery) Children() []sql.Expression {
	return nil
}

// WithQuery returns the subquery with the query node changed.
func (s *Subquery) WithQuery(node sql.Node) *Subquery {
	ns := *s
	ns.Query = node
	return &ns
}

func (s *Subquery) IsNonDeterministic() bool {
	return !s.canCacheResults
}

// WithCachedResults returns the subquery with CanCacheResults set to true.
func (s *Subquery) WithCachedResults() *Subquery {
	ns := *s
	ns.canCacheResults = true
	return &ns
}

// Dispose implements sql.Disposable
func (s *Subquery) Dispose() {
	if s.disposeFunc != nil {
		s.disposeFunc()
		s.disposeFunc = nil
	}
}
