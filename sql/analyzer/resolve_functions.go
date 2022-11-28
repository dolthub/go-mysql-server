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
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveTableFunctions(ctx *sql.Context, a *Analyzer, n sql.Node, _ *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_table_functions")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		utf, ok := n.(*expression.UnresolvedTableFunction)
		if !ok {
			return n, transform.SameTree, nil
		}

		tableFunction, err := a.Catalog.TableFunction(ctx, utf.Name())
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

		var hasBindVarArgs bool
		for _, arg := range utf.Arguments {
			if _, ok := arg.(*expression.BindVar); ok {
				hasBindVarArgs = true
				break
			}
		}

		if hasBindVarArgs {
			return n, transform.SameTree, nil
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
	span, ctx := ctx.Span("resolve_functions")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// Special handling for information_schema.columns: we need to resolve any functions for all column defaults in any
		// table, because by not doing so we would get different textual output than we do in e.g. `show create table` or
		// other places where column defaults are fully resolved by the analyzer. Because column defaults can only contain
		// functions, columns from the same table, and literals, we don't need to resolve them further than this to get an
		// accurate and consistent text representation.
		rt, ok := n.(*plan.ResolvedTable)
		if ok {
			ct, ok := rt.Table.(*information_schema.ColumnsTable)
			if ok {
				cols, err := ct.AllColumns(ctx)
				if err != nil {
					return nil, transform.SameTree, err
				}

				allDefaults, same, err := transform.Exprs(transform.WrappedColumnDefaults(cols), resolveFunctionsInExpr(ctx, a))

				if !same {
					rt.Table, err = ct.WithColumnDefaults(allDefaults)
					if err != nil {
						return nil, transform.SameTree, err
					}
					return rt, transform.NewTree, err
				}

				return rt, transform.SameTree, nil
			}
		}

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
