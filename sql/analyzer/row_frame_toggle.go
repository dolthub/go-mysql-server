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

	return TypeSelectorNode{Node: n, isNode2: allNode2}, nil
}

type TypeSelectorNode struct {
	sql.Node
	isNode2 bool
}

var _ sql.Node = TypeSelectorNode{}
var _ sql.NodeTypeSelector = TypeSelectorNode{}

func (r TypeSelectorNode) IsNode2() bool {
	return r.isNode2
}

func (r TypeSelectorNode) DebugString() string {
	tp := sql.NewTreePrinter()
	tp.WriteNode("TypeSelector (node2=%v)", r.isNode2)
	tp.WriteChildren(sql.DebugString(r.Node))
	return tp.String()
}