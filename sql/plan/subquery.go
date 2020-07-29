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
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

var errExpectedSingleRow = errors.NewKind("the subquery returned more than 1 row")

// Subquery is as an expression whose value is derived by executing a subquery. It must be executed for every row in
// the outer result set. It's in the plan package instead of the expression package because it functions more like a
// plan Node than an expression.
type Subquery struct {
	// The subquery to execute for each row in the outer result set
	Query sql.Node
	// The original verbatim select statement for this subquery
	QueryString string
	// The number of columns of outer scope schema expected before inner-query result row columns
	ScopeLen int
}

// NewSubquery returns a new subquery expression.
func NewSubquery(node sql.Node, queryString string) *Subquery {
	return &Subquery{Query: node, QueryString: queryString}
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

func (p *prependRowIter) Close() error {
	return p.childIter.Close()
}

func (p *prependNode) String() string {
	return p.Child.String()
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
	// TODO: the row being evaluated here might be shorter than the schema of the wrapping scope node. This is typically a
	//  problem for Project nodes, where there isn't a 1:1 correspondence between child row results and projections (more
	//  projections than unique columns in the underlying result set). This creates a problem for the evaluation of
	//  subquery rows -- all the indexes will be off, since they expect to be given a row the same length as the schema of
	//  their scope node. In this case, we fix the indexes by filling in zero values for the missing elements. This should
	//  probably be dealt with by adjustments to field indexes in the analyzer instead.
	scopeRow := row
	// if len(scopeRow) < s.ScopeLen {
	// 	scopeRow = make(sql.Row, s.ScopeLen)
	// 	copy(scopeRow, row)
	// }

	// Any source of rows, as well as any node that alters the schema of its children, needs to be wrapped so that its
	// result rows are prepended with the scope row.
	q, err := TransformUp(s.Query, prependScopeRowInPlan(scopeRow))

	if err != nil {
		return nil, err
	}

	iter, err := q.RowIter(ctx, scopeRow)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	if len(rows) > 1 {
		return nil, errExpectedSingleRow.New()
	}

	// TODO: fix this. This should always be true, but isn't, because we don't consistently pass the scope row in all
	//  parts of the engine.
	col := 0
	if len(scopeRow) < len(rows[0]) {
		col = len(scopeRow)
	}

	return rows[0][col], nil
}

// prependScopeRowInPlan returns a transformation function that prepends the row given to any row source in a query
// plan. Any source of rows, as well as any node that alters the schema of its children, needs to be wrapped so that its
// result rows are prepended with the scope row.
func prependScopeRowInPlan(scopeRow sql.Row) func(n sql.Node) (sql.Node, error) {
	return func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *Project, sql.Table:
			return &prependNode{
				UnaryNode: UnaryNode{Child: n},
				row:       scopeRow,
			}, nil
		default:
			return n, nil
		}
	}
}

// EvalMultiple returns all rows returned by a subquery.
func (s *Subquery) EvalMultiple(ctx *sql.Context, row sql.Row) ([]interface{}, error) {
	q, err := TransformUp(s.Query, prependScopeRowInPlan(row))
	if err != nil {
		return nil, err
	}

	iter, err := q.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	var result = make([]interface{}, len(rows))
	for i, row := range rows {
		result[i] = row[0]
	}

	return result, nil
}

// IsNullable implements the Expression interface.
func (s *Subquery) IsNullable() bool {
	return s.Query.Schema()[0].Nullable
}

func (s *Subquery) String() string {
	return fmt.Sprintf("(%s)", s.Query)
}

func (s *Subquery) DebugString() string {
	return fmt.Sprintf("(%s)", sql.DebugString(s.Query))
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

// WithQuery returns the subquery with the query node changed.
func (s *Subquery) WithQuery(node sql.Node) *Subquery {
	ns := *s
	ns.Query = node
	return &ns
}

// WithScopeLen returns the subquery with the scope length changed.
// func (s *Subquery) WithScopeLen(length int) *Subquery {
// 	ns := *s
// 	ns.ScopeLen = length
// 	return &ns
// }
