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
	"github.com/dolthub/go-mysql-server/sql/analyzer/locks"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func acquireLocks(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// do nothing if autocommit is off
	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return nil, err
	}

	hasAutocommit, err := sql.ConvertToBool(autoCommitSessionVar)
	if err != nil {
		return nil, err
	}

	if hasAutocommit {
		return n, nil
	}

	// Validate that we have a select for update mode

	return assignLockNode(n, a.LockManager)
}

func assignLockNode(n sql.Node, lm locks.LockManager) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		// TODO: Add support for Delete, DDL, FK Tables, etc.
		switch t := n.(type) {
		case *plan.InsertInto:
			lw := plan.NewLockWrapper(t.Source).WithTableName(getTableName(t.Destination)).WithLockManager(lm)
			return t.WithSource(lw), nil
		case *plan.Update:
			lw := plan.NewLockWrapper(t.Child).WithLockManager(lm)
			return t.WithChildren(lw)
		}

		return n, nil
	})
}

func assignLockManager(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("assign_lock_manager")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		switch node := n.(type) {
		case *plan.Commit:
			nc := *node
			nc.LockManager = a.LockManager
			return &nc, nil
		case *plan.Rollback:
			nc := *node
			nc.LockManager = a.LockManager
			return &nc, nil
		case *plan.SelectForUpdate:
			nc := *node
			nc.LockManager = a.LockManager
			return &nc, nil
		default:
			return n, nil
		}
	})
}
