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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// NewHashLookup returns a node that performs an indexed hash lookup
// of cached rows for fulfilling RowIter() calls. In particular, this
// node sits directly on top of a `CachedResults` node and has two
// expressions: a projection for hashing the Child row results and
// another projection for hashing the parent row values when
// performing a lookup. When RowIter is called, if cached results are
// available, it fulfills the RowIter call by performing a hash lookup
// on the projected results. If cached results are not available, it
// simply delegates to the child.
func NewHashLookup(n *CachedResults, childProjection sql.Expression, lookupProjection sql.Expression) *HashLookup {
	return &HashLookup{
		UnaryNode: UnaryNode{n},
		Inner:     childProjection,
		Outer:     lookupProjection,
		Mutex:     new(sync.Mutex),
	}
}

type HashLookup struct {
	UnaryNode
	Inner  sql.Expression
	Outer  sql.Expression
	Mutex  *sync.Mutex
	Lookup map[interface{}][]sql.Row
}

var _ sql.Node = (*HashLookup)(nil)
var _ sql.Expressioner = (*HashLookup)(nil)
var _ sql.CollationCoercible = (*HashLookup)(nil)

func (n *HashLookup) Expressions() []sql.Expression {
	return []sql.Expression{n.Inner, n.Outer}
}

func (n *HashLookup) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(exprs), 2)
	}
	ret := *n
	ret.Inner = exprs[0]
	ret.Outer = exprs[1]
	return &ret, nil
}

func (n *HashLookup) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("HashLookup")
	children := make([]string, 3)
	children[0] = fmt.Sprintf("outer: %s", n.Outer)
	children[1] = fmt.Sprintf("inner: %s", n.Inner)
	children[2] = n.Child.String()
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (n *HashLookup) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("HashLookup")
	children := make([]string, 3)
	children[0] = fmt.Sprintf("source: %s", sql.DebugString(n.Outer))
	children[1] = fmt.Sprintf("target: %s", sql.DebugString(n.Inner))
	children[2] = sql.DebugString(n.Child)
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (n *HashLookup) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	if _, ok := children[0].(*CachedResults); !ok {
		return nil, sql.ErrInvalidChildType.New(n, children[0], (*CachedResults)(nil))
	}
	nn := *n
	nn.UnaryNode.Child = children[0]
	return &nn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *HashLookup) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return n.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (n *HashLookup) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, n.Child)
}

// Convert a tuple expression returning []interface{} into something comparable.
// Fast paths a few smaller slices into fixed size arrays, puts everything else
// through string serialization and a hash for now. It is OK to hash lossy here
// as the join condition is still evaluated after the matching rows are returned.
func (n *HashLookup) GetHashKey(ctx *sql.Context, e sql.Expression, row sql.Row) (interface{}, error) {
	key, err := e.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	key, _, err = n.Outer.Type().Convert(key)
	if err != nil {
		return nil, err
	}
	if s, ok := key.([]interface{}); ok {
		switch len(s) {
		case 0:
			return [0]interface{}{}, nil
		case 1:
			return [1]interface{}{s[0]}, nil
		case 2:
			return [2]interface{}{s[0], s[1]}, nil
		case 3:
			return [3]interface{}{s[0], s[1], s[2]}, nil
		case 4:
			return [4]interface{}{s[0], s[1], s[2], s[3]}, nil
		case 5:
			return [5]interface{}{s[0], s[1], s[2], s[3], s[4]}, nil
		default:
			return sql.HashOf(s)
		}
	}
	// byte slices are not hashable
	if k, ok := key.([]byte); ok {
		key = string(k)
	}
	return key, nil
}

func (n *HashLookup) Dispose() {
	cr := n.Child.(*CachedResults)
	cr.Dispose()
}
