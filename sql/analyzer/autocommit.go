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

// addAutocommit wraps each query with a TransactionCommittingNode.
func addAutocommit(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	// TODO: This is a bit of a hack. Need to figure out better relationship between new transaction node and warnings.
	if FlagIsSet(qFlags, sql.QFlagShowWarnings) {
		return n, transform.SameTree, nil
	}

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}
	return plan.NewTransactionCommittingNode(n), transform.NewTree, nil
}
