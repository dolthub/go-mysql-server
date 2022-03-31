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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/stretchr/testify/require"
)

func TestTransformUp(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name  string
		inp   sql.Node
		cmp   sql.Node
		visit sql.TransformNodeFunc
		same  bool
	}{
		{
			name: "modify tree",
			inp:  a(a(a(), a(), a(b())), c()),
			cmp:  b(b(b(), b(), b(c())), c()),
			visit: func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeA:
					return b(n.children...), sql.NewTree, nil
				case *nodeB:
					return c(n.children...), sql.NewTree, nil
				default:
					return n, sql.SameTree, nil
				}
			},
		},
		{
			name: "no modification",
			inp:  a(a(a(), a(), a(b())), b()),
			cmp:  a(a(a(), a(), a(b())), b()),
			same: true,
			visit: func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
				switch n := node.(type) {
				case *nodeC:
					return a(n.children...), sql.NewTree, nil
				default:
					return n, sql.SameTree, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, same, err := Node(tt.inp, tt.visit)
			require.NoError(err)
			require.Equal(tt.cmp, res)
			require.Equal(bool(same), tt.same)
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

func (n *testNode) Resolved() bool {
	return true
}

func (n *testNode) String() string {
	return ""
}

func (n *testNode) Schema() sql.Schema {
	return nil
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
