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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// clearWarnings resets the warning count to 0 and removes any warnings from the current context.
func clearWarnings(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	// This is an analyzer rule because we need to clear the warnings from the previous query before executing
	// the current query, except for the case of `show warnings` which should not clear the warnings.
	children := node.Children()
	if len(children) == 0 {
		return node, transform.SameTree, nil
	}

	switch ch := children[0].(type) {
	case *plan.Offset, *plan.Limit:
		// `show warning limit x offset y` is valid, so we need to recurse
		return clearWarnings(ctx, a, ch, scope, sel, qFlags)
	case plan.ShowWarnings:
		// ShowWarnings should not clear the warnings, but should still reset the warning count.
		ctx.ClearWarningCount()
		return node, transform.SameTree, nil
	default:
		ctx.ClearWarnings()
		return node, transform.SameTree, nil
	}
}
