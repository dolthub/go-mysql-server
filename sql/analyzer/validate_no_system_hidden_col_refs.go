// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// validateNoHiddenSystemColumns inspects the query plan and asserts that no references to hidden
// system columns are used, including references to existing hidden system columns, as well as
// attempts to create or rename a column to the reserved hidden system column prefix. For example,
// asserts that no hidden system columns are used in expressions.
// and that no DDL statements are used to modify hidden system columns.
func validateNoHiddenSystemColumns(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *plan.Scope, _ RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var err error
	transform.Inspect(n, func(n sql.Node) bool {
		switch nn := n.(type) {
		case *plan.CreateTable:
			for _, col := range nn.TargetSchema() {
				if sql.IsHiddenSystemColumn(col.Name) {
					err = fmt.Errorf("invalid column name: %s", col.Name)
				}
			}
		case *plan.ModifyColumn:
			if sql.IsHiddenSystemColumn(nn.Column()) {
				err = sql.ErrColumnNotFound.New(nn.Column())
			}
		case *plan.DropColumn:
			if sql.IsHiddenSystemColumn(nn.Column) {
				err = sql.ErrColumnNotFound.New(nn.Column)
			}
		case *plan.RenameColumn:
			if sql.IsHiddenSystemColumn(nn.ColumnName) {
				err = sql.ErrColumnNotFound.New(nn.ColumnName)
			}
			if sql.IsHiddenSystemColumn(nn.NewColumnName) {
				err = fmt.Errorf("invalid column name: %s", nn.NewColumnName)
			}
		}

		switch n.(type) {
		case *plan.Update, *plan.UpdateSource:
		// NOTE: UpdateSource expressions can still have references to hidden system columns, because
		//       those values need to be plugged in for the secondary indexes to be updated correctly.
		//       So we skip validating those here, and rely on checks in planbuilder to prevent users
		//       from directly referencing hidden system columns.
		default:
			transform.InspectExpressions(ctx, n, func(ctx *sql.Context, e sql.Expression) bool {
				if gf, ok := e.(*expression.GetField); ok {
					if sql.IsHiddenSystemColumn(gf.Name()) {
						err = sql.ErrColumnNotFound.New(gf.Name())
					}
				}
				return true
			})
		}

		// continue as long as no error has been set
		return err == nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, err
}
