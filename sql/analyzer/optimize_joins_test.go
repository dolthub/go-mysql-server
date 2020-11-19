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

	testCases := []joinTreeTest{
		{
			name: "linear join, ABC",
			tableOrder: []string{"A","B","C"},
			joinConds:  []sql.Expression{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				table:    "",
				joinCond: jc("A", "B"),
				left: &joinSearchNode{
					joinCond: jc("B", "C"),
					left: &joinSearchNode{
						table: "A",
					},
					right: &joinSearchNode{
						table: "B",
					},
				},
				right: &joinSearchNode{
					table: "C",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			joinTree := buildJoinTree(tt.tableOrder, tt.joinConds)
			pruneParamsAndParent(joinTree)
			assert.Equal(t, tt.joinTree, joinTree)
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


func jc(leftTable, rightTable string) sql.Expression {
	return eq(gf(0, leftTable, "col"), gf(0, rightTable, "col"))
}