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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func canApplyLazyWrites(ctx *sql.Context, node sql.Node) bool {
	canApply := true
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) bool {
		switch nn := n.(type) {
		case *plan.InsertInto:
			if !canApplyLazyWrites(ctx, nn.Source) {
				canApply = false
			}
			return false
		case *plan.TriggerBeginEndBlock, *plan.BeginEndBlock, *plan.Block:
			canApply = false
			return false
		default:
			return true
		}
	})
	return canApply
}

// applyLazyWrites makes sql.LazyTableEditors defer flushing writes to disk until necessary.
func applyLazyWrites(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	switch {
	case qFlags.IsSet(sql.QFlagDDL):
		a.ExecBuilder.UseLazyWrites = false
	case qFlags.IsSet(sql.QFlagTrigger):
		a.ExecBuilder.UseLazyWrites = false
	default:
		a.ExecBuilder.UseLazyWrites = true
	}
	return node, transform.SameTree, nil
}
