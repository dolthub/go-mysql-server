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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveViews(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolve_views")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		viewName := urt.Name()
		dbName := urt.Database
		if dbName == "" {
			dbName = ctx.GetCurrentDatabase()
		}

		var view *sql.View

		if dbName != "" {
			db, err := a.Catalog.Database(dbName)
			if err != nil {
				return nil, err
			}

			if vdb, ok := db.(sql.ViewDatabase); ok {
				viewDef, ok, err := vdb.GetView(ctx, viewName)
				if err != nil {
					return nil, err
				}

				if ok {
					query, err := parse.Parse(ctx, viewDef)
					if err != nil {
						return nil, err
					}

					view = plan.NewSubqueryAlias(viewName, viewDef, query).AsView()
				}
			}
		}

		// If we didn't find the view from the database directly, use the in-session registry
		var err error
		if view == nil {
			view, err = ctx.GetViewRegistry().View(dbName, viewName)
			if sql.ErrViewDoesNotExist.Is(err) {
				return n, nil
			} else if err != nil {
				return nil, err
			}
		}

		a.Log("view resolved: %q", viewName)

		query := view.Definition().Children()[0]

		// If this view is being asked for with an AS OF clause, then attempt to apply it to every table in the view.
		if urt.AsOf != nil {
			query, err = applyAsOfToView(query, a, urt.AsOf)
			if err != nil {
				return nil, err
			}
		}

		// If the view name was qualified with a database name, apply that same qualifier to any tables in it
		if urt.Database != "" {
			query, err = applyDatabaseQualifierToView(query, a, urt.Database)
			if err != nil {
				return nil, err
			}
		}

		return view.Definition().WithChildren(query)
	})
}

func applyAsOfToView(n sql.Node, a *Analyzer, asOf sql.Expression) (sql.Node, error) {
	a.Log("applying AS OF clause to view definition")

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		a.Log("applying AS OF clause to view " + urt.Name())
		if urt.AsOf != nil {
			return nil, sql.ErrIncompatibleAsOf.New(
				fmt.Sprintf("cannot combine AS OF clauses %s and %s",
					asOf.String(), urt.AsOf.String()))
		}

		return urt.WithAsOf(asOf)
	})
}

func applyDatabaseQualifierToView(n sql.Node, a *Analyzer, dbName string) (sql.Node, error) {
	a.Log("applying database qualifier to view definition")

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		a.Log("applying database name to view table " + urt.Name())
		if urt.Database == "" {
			return urt.WithDatabase(dbName)
		}

		return n, nil
	})
}
