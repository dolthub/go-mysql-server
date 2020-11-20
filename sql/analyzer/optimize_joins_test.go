// Copyright 2019-2020 Dolthub, Inc.
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

package analyzer

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildJoinTree(t *testing.T) {
	type joinTreeTest struct {
		name string
		tableOrder []string
		joinConds  []sql.Expression
		joinTree *joinSearchNode
	}

	// These tests are a little fragile: for many of these joins, there is more than one correct join tree.
	// For example, a join on tables A, B, C that wants the tables visited in that order has two solutions:
	// join(A, join(B, C, B=C), A=B), OR join(join(A, B, A=B), C, B=C)
	// The test cases obviously choose the one actually produced by the current algorithm, but it's very arbitrary which
	// one is returned.
	testCases := []joinTreeTest{
		{
			name:       "linear join, ABC",
			tableOrder: []string{"A", "B", "C"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt("A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt("B"),
					right:    jt("C"),
				},
			},
		},
		{
			name:       "linear join, ACB", // 👩‍⚖️
			tableOrder: []string{"A", "C", "B"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt("A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt("C"),
					right:    jt("B"),
				},
			},
		},
		{
			name:       "linear join, BAC",
			tableOrder: []string{"B", "A", "C"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt("B"),
					right:    jt("A"),
				},
				right: jt("C"),
			},
		},
		{
			name:       "linear join, BCA",
			tableOrder: []string{"B", "C", "A"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt("B"),
					right:    jt("C"),
				},
				right: jt("A"),
			},
		},
		{
			name:       "linear join, CAB",
			tableOrder: []string{"C", "A", "B"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left:     jt("C"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt("A"),
					right:    jt("B"),
				},
			},
		},
		{
			name:       "linear join, CBA",
			tableOrder: []string{"C", "B", "A"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt("C"),
					right:    jt("B"),
				},
				right: jt("A"),
			},
		},
		{
			name:       "all joined to A, ABC",
			tableOrder: []string{"A", "B", "C"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "C"),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt("A"),
					right:    jt("B"),
				},
				right: jt("C"),
			},
		},
		{
			name:       "all joined to A, CBA",
			tableOrder: []string{"C", "B", "A"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "C"),
				left: jt("C"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt("B"),
					right:    jt("A"),
				},
			},
		},
		{
			name:       "all joined to A, BAC",
			tableOrder: []string{"B", "A", "C"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left: jt("B"),
				right: &joinSearchNode{
					joinCond: jc("A", "C"),
					left:     jt("A"),
					right:    jt("C"),
				},
			},
		},
		{
			name:       "linear join, ABCD",
			tableOrder: []string{"A", "B", "C", "D"},
			joinConds: []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt("A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt("B"),
					right: &joinSearchNode{
						joinCond: jc("C", "D"),
						left:     jt("C"),
						right:    jt("D"),
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			joinTree := buildJoinTree(tt.tableOrder, tt.joinConds)
			pruneParamsAndParent(joinTree)
			if !assert.Equal(t, tt.joinTree, joinTree) {
				fmt.Printf("Expected:\n%s, but got:\n%s", tt.joinTree, joinTree)
			}
		})
	}
}

func pruneParamsAndParent(node *joinSearchNode) {
	if node == nil {
		return
	}
	node.params = nil
	node.parent = nil
	pruneParamsAndParent(node.left)
	pruneParamsAndParent(node.right)
}

// jc == join cond
func jc(leftTable, rightTable string) sql.Expression {
	return eq(gf(0, leftTable, "col"), gf(0, rightTable, "col"))
}

// jt == join table
func jt(name string) *joinSearchNode {
	return &joinSearchNode{
		table:    name,
	}
}