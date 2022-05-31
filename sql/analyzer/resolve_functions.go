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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveTableFunctions(ctx *sql.Context, a *Analyzer, n sql.Node, _ *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolve_table_functions")
	defer span.Finish()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		utf, ok := n.(*expression.UnresolvedTableFunction)
		if !ok {
			return n, transform.SameTree, nil
		}

		tableFunction, err := a.Catalog.TableFunction(ctx, utf.FunctionName())
		if err != nil {
			return nil, transform.SameTree, err
		}

		database, err := a.Catalog.Database(ctx, ctx.GetCurrentDatabase())
		if err != nil {
			return nil, transform.SameTree, err
		}

		if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
			database = privilegedDatabase.Unwrap()
		}

		newInstance, err := tableFunction.NewInstance(ctx, database, utf.Arguments)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return newInstance, transform.NewTree, nil
	})
}

// resolveFunctions replaces UnresolvedFunction nodes with equivalent functions from the Catalog.
func resolveFunctions(ctx *sql.Context, a *Analyzer, n sql.Node, _ *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolve_functions")
	defer span.Finish()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		return transform.OneNodeExpressions(n, resolveFunctionsInExpr(ctx, a))
	})
}

func resolveFunctionsInExpr(ctx *sql.Context, a *Analyzer) transform.ExprFunc {
	return func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if e.Resolved() {
			return e, transform.SameTree, nil
		}

		uf, ok := e.(*expression.UnresolvedFunction)
		if !ok {
			return e, transform.SameTree, nil
		}

		n := uf.Name()
		f, err := a.Catalog.Function(ctx, n)
		if err != nil {
			return nil, transform.SameTree, err
		}

		rf, err := f.NewInstance(uf.Arguments)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// Because of the way that we instantiate functions, we need to pass in the window from the UnresolvedFunction
		// separately. Otherwise we would need to change function constructors to all consider windows, when most
		// functions don't have a window expression.
		switch a := rf.(type) {
		case sql.WindowAggregation:
			rf, err = a.WithWindow(uf.Window)
			if err != nil {
				return nil, transform.SameTree, err
			}
		case sql.Aggregation:
			rf, err = a.WithWindow(uf.Window)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		a.Log("resolved function %q", n)
		return rf, transform.NewTree, nil
	}
}
