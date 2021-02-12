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
	"github.com/dolthub/go-mysql-server/sql/expression"
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
	Subquery *Subquery
	Child    sql.Node
	Padding  int
	GetField *expression.GetField
	Equals   bool
}

func (i *IndexedInSubqueryFilter) Resolved() bool {
	return i.Subquery.Resolved() && i.Child.Resolved()
}

func (i *IndexedInSubqueryFilter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexedInSubqueryFilter(%s IN (%s))", i.GetField, i.Subquery)
	_ = pr.WriteChildren(i.Child.String())
	return pr.String()
}

func (i *IndexedInSubqueryFilter) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexedInSubqueryFilter(%s IN (%s))", sql.DebugString(i.GetField), sql.DebugString(i.Subquery))
	_ = pr.WriteChildren(sql.DebugString(i.Child))
	return pr.String()
}

func (i *IndexedInSubqueryFilter) Schema() sql.Schema {
	return i.Child.Schema()
}

func (i *IndexedInSubqueryFilter) Children() []sql.Node {
	return []sql.Node{i.Child}
}

func (i *IndexedInSubqueryFilter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIndexedInSubqueryFilter(i.Subquery, children[0], i.Padding, i.GetField, i.Equals), nil
}

func (i *IndexedInSubqueryFilter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	padded := make(sql.Row, len(row)+i.Padding)
	copy(padded[:], row[:])
	var res []interface{}
	var err error
	if i.Equals {
		resi, err := i.Subquery.Eval(ctx, padded)
		if err != nil {
			return nil, err
		}
		res = append(res, resi)
	} else {
		res, err = i.Subquery.EvalMultiple(ctx, padded)
		if err != nil {
			return nil, err
		}
	}
	tupLits := make([]sql.Expression, len(res))
	for j := range res {
		tupLits[j] = expression.NewLiteral(res[j], i.Subquery.Type())
	}
	expr := expression.NewInTuple(i.GetField, expression.NewTuple(tupLits...))
	return NewFilterIter(ctx, expr, &indexedInSubqueryIter{ctx, res, i.Child, nil, 0}), nil
}

type indexedInSubqueryIter struct {
	Ctx   *sql.Context
	Rows  []interface{}
	Child sql.Node
	Cur   sql.RowIter
	I     int
}

func (i *indexedInSubqueryIter) Next() (sql.Row, error) {
	var ret sql.Row
	err := io.EOF
	for err == io.EOF {
		if i.Cur == nil {
			if i.I >= len(i.Rows) {
				return nil, io.EOF
			}
			iter, err := i.Child.RowIter(i.Ctx, sql.NewRow(i.Rows[i.I]))
			if err != nil {
				return nil, err
			}
			i.I += 1
			i.Cur = iter
		}
		ret, err = i.Cur.Next()
		if err == io.EOF {
			cerr := i.Cur.Close()
			i.Cur = nil
			if cerr != nil {
				return nil, cerr
			}
		}
	}
	return ret, err
}

func (i *indexedInSubqueryIter) Close() error {
	if i.Cur != nil {
		err := i.Cur.Close()
		i.Cur = nil
		return err
	}
	return nil
}
