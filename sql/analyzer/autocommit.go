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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// addAutocommitNode wraps each query with a TransactionCommittingNode.
func addAutocommitNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	// TODO: This is a bit of a hack. Need to figure out better relationship between new transaction node and warnings.
	if hasShowWarningsNode(n) {
		return n, transform.SameTree, nil
	}

	return plan.NewTransactionCommittingNode(n), transform.NewTree, nil
}

func hasShowWarningsNode(n sql.Node) bool {
	var ret bool
	transform.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(plan.ShowWarnings); ok {
			ret = true
			return false
		}

		return true
	})

	return ret
}
