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

package transform

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/stretchr/testify/require"
)

// todo(max): more tests
func TestTransformUp(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		inp   sql.Node
		cmp   sql.Node
		visit NodeFunc
		same  TreeIdentity
	}{
		{
			inp:  a(a(a(), a(), a(b())), c()),
			cmp:  b(b(b(), b(), b(c())), c()),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return b(n.children...), NewTree, nil
				case *nodeB:
					return c(n.children...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(a(a(), a(), a(b())), c()),
			cmp:  b(b(b(), b(), b(b())), b()),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA, *nodeB, *nodeC:
					return b(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(a(a(), a(), a(b())), c()),
			cmp:  a(a(a(), a(), a(b())), b()),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return b(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(b(), c(), b(b())), c()),
			cmp:  c(b(b(), c(), b(b())), c()),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(b())),
			cmp:  c(b(b())),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(a())),
			cmp:  c(b(c())),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(b(b(b(b(b(b(b(b()))))))))),
			cmp:  c(b(b(b(b(b(b(b(b(b()))))))))),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(),
			cmp:  c(),
			same: NewTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(a(a(), a(), a(b())), b()),
			cmp:  a(a(a(), a(), a(b())), b()),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return a(n.children...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(a(a(), a(), a(b())), b()),
			cmp:  a(a(a(), a(), a(b())), b()),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return a(n.children...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  c(b(b(), c(), b(b())), c()),
			cmp:  c(b(b(), c(), b(b())), c()),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(b())),
			cmp:  a(b(b())),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(a())),
			cmp:  a(b(a())),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(b(b(b(b(b(b(b(b(b()))))))))),
			cmp:  a(b(b(b(b(b(b(b(b(b()))))))))),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
		{
			inp:  a(),
			cmp:  a(),
			same: SameTree,
			visit: func(node sql.Node) (sql.Node, TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return c(n.Children()...), NewTree, nil
				default:
					return n, SameTree, nil
				}
			},
		},
	}

	for i, tt := range tests {
		var name string
		if tt.same {
			name = fmt.Sprintf("same tree #%d", i)
		} else {
			name = fmt.Sprintf("new tree #%d", i)
		}

		t.Run(name, func(t *testing.T) {
			res, same, err := Node(tt.inp, tt.visit)
			require.NoError(err)
			require.Equal(tt.cmp, res)
			require.Equal(same, tt.same)
		})
	}
}

type nodeA struct {
	testNode
}
type nodeB struct {
	testNode
}
type nodeC struct {
	testNode
}

var _ sql.Node = (*nodeA)(nil)
var _ sql.CollationCoercible = (*nodeA)(nil)

func a(nodes ...sql.Node) *nodeA {
	return &nodeA{testNode{children: nodes}}
}

func b(nodes ...sql.Node) *nodeB {
	return &nodeB{testNode{children: nodes}}
}

func c(nodes ...sql.Node) *nodeC {
	return &nodeC{testNode{children: nodes}}
}

func (n *nodeA) WithChildren(nodes ...sql.Node) (sql.Node, error) {
	nn := *n
	nn.children = nodes
	return &nn, nil
}

func (n *nodeB) WithChildren(nodes ...sql.Node) (sql.Node, error) {
	nn := *n
	nn.children = nodes
	return &nn, nil
}

func (n *nodeC) WithChildren(nodes ...sql.Node) (sql.Node, error) {
	nn := *n
	nn.children = nodes
	return &nn, nil
}

func NewTestNode(nodes ...sql.Node) *testNode {
	return &testNode{
		children: nodes,
	}
}

type testNode struct {
	children []sql.Node
}

var _ sql.Node = (*testNode)(nil)
var _ sql.CollationCoercible = (*testNode)(nil)

func (n *testNode) Resolved() bool {
	return true
}

func (n *testNode) String() string {
	return ""
}

func (n *testNode) Schema() sql.Schema {
	return nil
}

func (n *testNode) IsReadOnly() bool {
	return true
}

func (n *testNode) Children() []sql.Node {
	return n.children
}

func (n *testNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}

func (n *testNode) WithChildren(nodes ...sql.Node) (sql.Node, error) {
	nn := *n
	nn.children = nodes
	return &nn, nil
}

func (n *testNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*testNode) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}
