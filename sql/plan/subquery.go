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
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

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
var _ sql.ExpressionWithNodes = (*Subquery)(nil)

type StripRowNode struct {
	UnaryNode
	numCols int
}

type stripRowIter struct {
	sql.RowIter
	numCols int
}

func (sri *stripRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	r, err := sri.RowIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	return r[sri.numCols:], nil
}

func (sri *stripRowIter) Close(ctx *sql.Context) error {
	return sri.RowIter.Close(ctx)
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
	return sql.DebugString(srn.Child)
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

// CheckPrivileges implements the interface sql.Node.
func (srn *StripRowNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return srn.Child.CheckPrivileges(ctx, opChecker)
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

func (p *prependRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	next, err := p.childIter.Next(ctx)
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

// CheckPrivileges implements the interface sql.Node.
func (p *prependNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return p.Child.CheckPrivileges(ctx, opChecker)
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
func prependRowInPlan(row sql.Row) func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case sql.Table, sql.Projector, *ValueDerivedTable:
			return &prependNode{
				UnaryNode: UnaryNode{Child: n},
				row:       row,
			}, transform.NewTree, nil
		case *Union:
			newUnion := *n
			newRight, _, err := transform.Node(n.Right(), prependRowInPlan(row))
			if err != nil {
				return n, transform.SameTree, err
			}
			newLeft, _, err := transform.Node(n.Left(), prependRowInPlan(row))
			if err != nil {
				return n, transform.SameTree, err
			}
			newUnion.left = newLeft
			newUnion.right = newRight
			return &newUnion, transform.NewTree, nil
		case *RecursiveCte:
			newRecursiveCte := *n
			newUnion, _, err := transform.Node(n.union, prependRowInPlan(row))
			newRecursiveCte.union = newUnion.(*Union)
			return &newRecursiveCte, transform.NewTree, err
		case *SubqueryAlias:
			// For SubqueryAliases (i.e. DerivedTables), since they may have visibility to outer scopes, we need to
			// transform their inner nodes to prepend the outer scope row data. Ideally, we would only do this when
			// the subquery alias references those outer fields. That will also require updating subquery expression
			// scope handling to also make the same optimization.
			if n.OuterScopeVisibility {
				newSubqueryAlias := *n
				newChildNode, _, err := transform.Node(n.Child, prependRowInPlan(row))
				newSubqueryAlias.Child = newChildNode
				return &newSubqueryAlias, transform.NewTree, err
			} else {
				return &prependNode{
					UnaryNode: UnaryNode{Child: n},
					row:       row,
				}, transform.NewTree, nil
			}
		}

		return n, transform.SameTree, nil
	}
}

func NewMax1Row(n sql.Node, name string) *Max1Row {
	return &Max1Row{Child: n, name: name, mu: &sync.Mutex{}}
}

// Max1Row throws a runtime error if its child (usually subquery) tries
// to return more than one row.
type Max1Row struct {
	Child       sql.Node
	name        string
	result      sql.Row
	mu          *sync.Mutex
	emptyResult bool
}

var _ sql.Node = (*Max1Row)(nil)

func (m *Max1Row) Name() string {
	return m.name
}

func (m *Max1Row) Resolved() bool {
	return m.Child.Resolved()
}

func (m *Max1Row) Schema() sql.Schema {
	return m.Child.Schema()
}

func (m *Max1Row) Children() []sql.Node {
	return []sql.Node{m.Child}
}

func (m *Max1Row) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Max1Row")
	children := []string{m.Child.String()}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (m *Max1Row) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Max1Row")
	children := []string{sql.DebugString(m.Child)}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (m *Max1Row) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.hasResults() {
		err := m.populateResults(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case m.emptyResult:
		return emptyIter, nil
	case m.result != nil:
		return sql.RowsToRowIter(m.result), nil
	default:
		return nil, fmt.Errorf("Max1Row failed to load results")
	}
}

// populateResults loads and stores the state of its child iter:
// 1) no rows returned, 2) 1 row returned, or 3) more than 1 row
// returned
func (m *Max1Row) populateResults(ctx *sql.Context, row sql.Row) error {
	i, err := m.Child.RowIter(ctx, row)
	if err != nil {
		return err
	}
	r1, err := i.Next(ctx)
	if errors.Is(err, io.EOF) {
		m.emptyResult = true
		return nil
	} else if err != nil {
		return err
	}

	_, err = i.Next(ctx)
	if err == nil {
		return sql.ErrExpectedSingleRow.New()
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	m.result = r1
	return nil
}

// hasResults returns true after a successful call to populateResults()
func (m *Max1Row) hasResults() bool {
	return m.result != nil || m.emptyResult
}

func (m *Max1Row) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	ret := *m

	ret.Child = children[0]

	return &ret, nil
}

func (m *Max1Row) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return m.Child.CheckPrivileges(ctx, opChecker)
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
	q, _, err := transform.Node(s.Query, prependRowInPlan(row))
	if err != nil {
		return nil, err
	}

	iter, err := q.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	returnsTuple := len(s.Query.Schema()) > 1

	// Reduce the result row to the size of the expected schema. This means chopping off the first len(row) columns.
	col := len(row)
	var result []interface{}
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if returnsTuple {
			result = append(result, append([]interface{}{}, row[col:]...))
		} else {
			result = append(result, row[col])
		}
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

// HasResultRow returns whether the subquery has a result set > 0.
func (s *Subquery) HasResultRow(ctx *sql.Context, row sql.Row) (bool, error) {
	// First check if the query was cached.
	s.cacheMu.Lock()
	cached := s.resultsCached
	s.cacheMu.Unlock()

	if cached {
		return len(s.cache) > 0, nil
	}

	// Any source of rows, as well as any node that alters the schema of its children, needs to be wrapped so that its
	// result rows are prepended with the scope row.
	q, _, err := transform.Node(s.Query, prependRowInPlan(row))
	if err != nil {
		return false, err
	}

	iter, err := q.RowIter(ctx, row)
	if err != nil {
		return false, err
	}

	// Call the iterator once and see if it has a row. If io.EOF is received return false.
	_, err = iter.Next(ctx)
	if err == io.EOF {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	err = iter.Close(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
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
	return true
}

func (s *Subquery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Subquery")
	children := []string{fmt.Sprintf("cacheable: %t", s.canCacheResults), s.Query.String()}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (s *Subquery) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Subquery")
	children := []string{fmt.Sprintf("cacheable: %t", s.canCacheResults), sql.DebugString(s.Query)}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// Resolved implements the Expression interface.
func (s *Subquery) Resolved() bool {
	return s.Query.Resolved()
}

// Type implements the Expression interface.
func (s *Subquery) Type() sql.Type {
	qs := s.Query.Schema()
	if len(qs) == 1 {
		return s.Query.Schema()[0].Type
	}
	ts := make([]sql.Type, len(qs))
	for i, c := range qs {
		ts[i] = c.Type
	}
	return types.CreateTuple(ts...)
}

// WithChildren implements the Expression interface.
func (s *Subquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}
	return s, nil
}

// Children implements the Expression interface.
func (s *Subquery) Children() []sql.Expression {
	return nil
}

// NodeChildren implements the sql.ExpressionWithNodes interface.
func (s *Subquery) NodeChildren() []sql.Node {
	return []sql.Node{s.Query}
}

// WithNodeChildren implements the sql.ExpressionWithNodes interface.
func (s *Subquery) WithNodeChildren(children ...sql.Node) (sql.ExpressionWithNodes, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return s.WithQuery(children[0]), nil
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

func (s *Subquery) CanCacheResults() bool {
	return s.canCacheResults
}

// Dispose implements sql.Disposable
func (s *Subquery) Dispose() {
	if s.disposeFunc != nil {
		s.disposeFunc()
		s.disposeFunc = nil
	}
	disposeNode(s.Query)
}
