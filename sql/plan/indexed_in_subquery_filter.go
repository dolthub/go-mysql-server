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
	"fmt"
	"io"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
)

// NewIndexedInSubqueryFilter returns an IndexedInSubqueryFilter
// sql.Node. The Node implements the semantics of `Filter(field IN
// (SELECT ...), Child)`, but runs the subquery `SELECT` first, and
// the makes repeated calls to `child.RowIter()` to get the matching
// Child rows. Typically `child` should be an `IndexedTableAccess`,
// and for this to be the right tradeoff, the results from `subquery`
// should be much smaller than the unfiltered results from `child`.
// `padding` is the number of `null` columns which will be appended to
// an incoming `Row` in the `RowIter` call because calling `Eval` on
// the `Subquery`, since the `Subquery` was originally expecting to
// expect within the scope of something like `child`. `child` itself
// should expect `RowIter()` calls with a single column `Row`, which
// will be the results from the `Subquery`. `filterField` is a
// `GetField` expression which will extract the field from `child`
// results that should be matched against the `subquery` results; this
// condition is still checked here because `child` is allowed to
// return non-matching rows. `equals` true means this node will call
// `subquery.Eval` and expect a single result, whereas `equals` false
// means this node will call `subquery.EvalMultiple` and expect 0 or
// more results.
func NewIndexedInSubqueryFilter(subquery *Subquery, child sql.Node, padding int, filterField *expression.GetField, equals bool) sql.Node {
	return &IndexedInSubqueryFilter{subquery, child, padding, filterField, equals}
}

type IndexedInSubqueryFilter struct {
	subquery *Subquery
	child    sql.Node
	padding  int
	getField *expression.GetField
	equals   bool
}

var _ sql.Node = (*IndexedInSubqueryFilter)(nil)
var _ sql.Disposable = (*IndexedInSubqueryFilter)(nil)
var _ sql.Expressioner = (*IndexedInSubqueryFilter)(nil)

func (i *IndexedInSubqueryFilter) Resolved() bool {
	return i.subquery.Resolved() && i.child.Resolved()
}

// Dispose implements sql.Disposable. Without this, the subquery member will not be properly disposed, since it's
// not a child node.
func (i *IndexedInSubqueryFilter) Dispose() {
	i.subquery.Dispose()
}

func (i *IndexedInSubqueryFilter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexedInSubqueryFilter")
	children := []string{fmt.Sprintf("field: %s", i.getField), fmt.Sprintf("subquery: %s", i.subquery), i.child.String()}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (i *IndexedInSubqueryFilter) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexedInSubqueryFilter")
	children := []string{
		fmt.Sprintf("field: %s", sql.DebugString(i.getField)),
		fmt.Sprintf("subquery: %s", sql.DebugString(i.subquery)),
		i.child.String(),
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (i *IndexedInSubqueryFilter) Schema() sql.Schema {
	return i.child.Schema()
}

func (i *IndexedInSubqueryFilter) Children() []sql.Node {
	return []sql.Node{i.child}
}

func (i *IndexedInSubqueryFilter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIndexedInSubqueryFilter(i.subquery, children[0], i.padding, i.getField, i.equals), nil
}

// CheckPrivileges implements the interface sql.Node.
func (i *IndexedInSubqueryFilter) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return i.subquery.Query.CheckPrivileges(ctx, opChecker) && i.child.CheckPrivileges(ctx, opChecker)
}

// Expressions implements the interface sql.Expressioner.
func (i *IndexedInSubqueryFilter) Expressions() []sql.Expression {
	return []sql.Expression{i.subquery}
}

// WithExpressions implements the interface sql.Expressioner.
func (i *IndexedInSubqueryFilter) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidExpressionNumber.New(i, len(exprs), 1)
	}
	subquery, ok := exprs[0].(*Subquery)
	if !ok {
		return nil, sql.ErrInvalidChildType.New(i, subquery, i.subquery)
	}
	return NewIndexedInSubqueryFilter(subquery, i.child, i.padding, i.getField, i.equals), nil
}

func (i *IndexedInSubqueryFilter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	padded := make(sql.Row, len(row)+i.padding)
	copy(padded[:], row[:])
	var res []interface{}
	var err error
	if i.equals {
		resi, err := i.subquery.Eval(ctx, padded)
		if err != nil {
			return &deferredErrIter{err}, nil
		}
		res = append(res, resi)
	} else {
		res, err = i.subquery.EvalMultiple(ctx, padded)
		if err != nil {
			return &deferredErrIter{err}, nil
		}
	}
	tupLits := make([]sql.Expression, len(res))
	for j := range res {
		tupLits[j] = expression.NewLiteral(res[j], i.subquery.Type())
	}
	expr := expression.NewInTuple(i.getField, expression.NewTuple(tupLits...))
	return NewFilterIter(expr, &indexedInSubqueryIter{res, i.child, nil, 0}), nil
}

type deferredErrIter struct {
	err error
}

func (i *deferredErrIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.err != nil {
		err := i.err
		i.err = nil
		return nil, err
	}
	return nil, io.EOF
}

func (i *deferredErrIter) Close(ctx *sql.Context) error {
	return nil
}

type indexedInSubqueryIter struct {
	rows  []interface{}
	child sql.Node
	cur   sql.RowIter
	i     int
}

func (iter *indexedInSubqueryIter) Next(ctx *sql.Context) (sql.Row, error) {
	var ret sql.Row
	err := io.EOF
	for err == io.EOF {
		if iter.cur == nil {
			if iter.i >= len(iter.rows) {
				return nil, io.EOF
			}
			childIter, err := iter.child.RowIter(ctx, sql.NewRow(iter.rows[iter.i]))
			if err != nil {
				return nil, err
			}
			iter.i += 1
			iter.cur = childIter
		}
		ret, err = iter.cur.Next(ctx)
		if err == io.EOF {
			cerr := iter.cur.Close(ctx)
			iter.cur = nil
			if cerr != nil {
				return nil, cerr
			}
		}
	}
	return ret, err
}

func (iter *indexedInSubqueryIter) Close(ctx *sql.Context) error {
	if iter.cur != nil {
		err := iter.cur.Close(ctx)
		iter.cur = nil
		return err
	}
	return nil
}
