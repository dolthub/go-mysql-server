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
	"slices"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// addRuleChecks installs plan validation on |e|'s analyzer rules.
//
// Each function in |checks| runs on successful rule output. An empty |checks|
// slice leaves |e| unchanged.
func addRuleChecks(e QueryEngine, checks ...func(*sql.Context, sql.Node) error) {
	if len(checks) == 0 {
		return
	}
	for _, batch := range e.EngineAnalyzer().Batches {
		batch.Rules = slices.Clone(batch.Rules)
		for i, rule := range batch.Rules {
			batch.Rules[i].Apply = withRuleChecks(rule, checks...)
		}
	}
}

// withRuleChecks returns a function that validates |rule|'s output.
//
// The function runs |checks| in order after |rule| succeeds and stops at the
// first error. Check errors include |rule|'s ID for diagnostics.
func withRuleChecks(rule analyzer.Rule, checks ...func(*sql.Context, sql.Node) error) analyzer.RuleFunc {
	return func(
		ctx *sql.Context,
		a *analyzer.Analyzer,
		n sql.Node,
		scope *plan.Scope,
		sel analyzer.RuleSelector,
		flags *sql.QueryFlags,
	) (sql.Node, transform.TreeIdentity, error) {
		n, same, err := rule.Apply(ctx, a, n, scope, sel, flags)
		if err != nil {
			return n, same, err
		}
		for _, check := range checks {
			if err := check(ctx, n); err != nil {
				return n, same, fmt.Errorf("after analyzer rule %s: %w", rule.Id, err)
			}
		}
		return n, same, nil
	}
}

// validateNoSelfReferencingAliases rejects alias self-references in |n|.
//
// An alias expression must not reference its own assigned column ID because
// expanding that reference would recurse indefinitely.
func validateNoSelfReferencingAliases(ctx *sql.Context, n sql.Node) error {
	var err error
	transform.InspectExpressions(ctx, n, func(ctx *sql.Context, e sql.Expression) bool {
		alias, ok := e.(*expression.Alias)
		if !ok || alias.Id() == 0 {
			return true
		}
		sql.Inspect(ctx, alias.Child, func(_ *sql.Context, e sql.Expression) bool {
			if field, ok := e.(*expression.GetField); ok && field.Id() == alias.Id() {
				err = fmt.Errorf("alias %q references its own column id %d", alias.Name(), alias.Id())
			}
			return true
		})
		return true
	})
	return err
}
