// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// applyRowFrameToggle returns a new top-level node that either does or does not implement Node2, based on whether all
// the nodes in the tree do. This allows a top-level executor (e.g. server handler) to begin its iteration with a
// RowIter2 or not. Once all node types implements Node2, this step can be replaced with static configuration, based on
// whether an integrator's tables implement Table or Table2.
func applyRowFrameToggle(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	allNode2 := true
	plan.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(sql.Node2); !ok {
			allNode2 = false
			return false
		}
		return true
	})

	if allNode2 {
		return RowIter2Node{n.(sql.Node2)}, nil
	} else {
		return RowIterNode{n}, nil
	}
}

type RowIterNode struct {
	sql.Node
}

var _ sql.Node = RowIterNode{}

type RowIter2Node struct {
	sql.Node2
}

var _ sql.Node2 = RowIter2Node{}