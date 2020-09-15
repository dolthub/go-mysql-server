// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func resolveSetVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.Set)
	if !ok || n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sf, ok := e.(*expression.SetField)
		if !ok {
			return e, nil
		}

		// For the left side of the SetField expression, we will attempt to resolve the variable being set. The rules to
		// determine whether to treat a left-hand expression as a system var or something else are subtle. These are all
		// the valid ways to assign to a system variable with session scope:
		// SET SESSION sql_mode = 'TRADITIONAL';
		// SET LOCAL sql_mode = 'TRADITIONAL';
		// SET @@SESSION.sql_mode = 'TRADITIONAL';
		// SET @@LOCAL.sql_mode = 'TRADITIONAL';
		// SET @@sql_mode = 'TRADITIONAL';
		// SET sql_mode = 'TRADITIONAL';
		// These are all equivalent, and all distinct from setting a user variable with the same name:
		// set @sql_mode = "abc"
		if uc, ok := sf.Left.(*expression.UnresolvedColumn); ok {
			if isSystemVariable(uc) {

			}
			if uc.Table() == "" {
				// system variable set
			}
		}

		return sf, nil
	})
}

func resolveSetColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.Set)
	if !ok || n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sf, ok := e.(*expression.SetField)
		if !ok {
			return e, nil
		}

		// If this column expression was deferred, it means that it wasn't prefixed with @ and can't be found in any table.
		// So treat it as a naked system variable.
		if uc, ok := sf.Left.(*deferredColumn); ok {
			if isSystemVariable(uc) {

			}
			if uc.Table() == "" {
				// system variable set
			}
		}

		return sf, nil
	})
}

