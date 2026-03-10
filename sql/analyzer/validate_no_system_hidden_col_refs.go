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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// validateNoHiddenSystemColumns inspects the query plan and asserts that no references to hidden
// system columns are used. For example, asserts that no hidden system columns are used in expressions
// and that no DDL statements are used to modify hidden system columns.
func validateNoHiddenSystemColumns(_ *sql.Context, _ *Analyzer, n sql.Node, _ *plan.Scope, _ RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var err error
	// TDOO: We don't want opaque here... because it'll go into subqueries (and union) and then when the analyzer runs on the outer node, it'll find those hidden columsn plugged in and error out
	// hrmm... seems like Inspect is still using WithOpaque... ???
	transform.Inspect(n, func(n sql.Node) bool {
		switch nn := n.(type) {
		case *plan.ModifyColumn:
			if isHiddenSystemColumn(nn.Column()) {
				err = sql.ErrColumnNotFound.New(nn.Column())
			}
		case *plan.DropColumn:
			if isHiddenSystemColumn(nn.Column) {
				err = sql.ErrColumnNotFound.New(nn.Column)
			}
		case *plan.RenameColumn:
			if isHiddenSystemColumn(nn.ColumnName) {
				err = sql.ErrColumnNotFound.New(nn.ColumnName)
			}
		}

		transform.InspectExpressions(n, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if isHiddenSystemColumn(gf.Name()) {
					err = sql.ErrColumnNotFound.New(gf.Name())
				}
			}
			return true
		})

		// continue as long as no error has been set
		return err == nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, err
}

// isHiddenSystemColumn returns true if |name| has the "!hidden!" prefix,
// indicating it is a system hidden column.
func isHiddenSystemColumn(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), "!hidden!")
}
