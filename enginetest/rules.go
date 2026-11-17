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

package enginetest

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// aliasValidatingRule checks analyzer output for self-referencing aliases so
// engine tests reject invalid plans before subsequent rules recurse.
type aliasValidatingRule struct {
	// rule produces the plan whose alias references are validated.
	rule analyzer.Rule
}

// apply runs the underlying rule and rejects aliases referencing their own
// assigned column ID to prevent invalid plans from reaching subsequent rules.
func (r aliasValidatingRule) apply(
	ctx *sql.Context,
	a *analyzer.Analyzer,
	n sql.Node,
	scope *plan.Scope,
	sel analyzer.RuleSelector,
	flags *sql.QueryFlags,
) (sql.Node, transform.TreeIdentity, error) {
	n, same, err := r.rule.Apply(ctx, a, n, scope, sel, flags)
	if err != nil {
		return n, same, err
	}
	transform.InspectExpressions(ctx, n, func(ctx *sql.Context, e sql.Expression) bool {
		alias, ok := e.(*expression.Alias)
		if !ok || alias.Id() == 0 {
			return true
		}
		sql.Inspect(ctx, alias.Child, func(_ *sql.Context, e sql.Expression) bool {
			if field, ok := e.(*expression.GetField); ok && field.Id() == alias.Id() {
				err = fmt.Errorf(
					"rule %s produced alias %q referencing its own column id %d",
					r.rule.Id, alias.Name(), alias.Id(),
				)
			}
			return true
		})
		return true
	})
	return n, same, err
}
