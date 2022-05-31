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
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveViews(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolve_views")
	defer span.Finish()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		viewName := urt.Name()
		dbName := urt.Database()
		if dbName == "" {
			dbName = ctx.GetCurrentDatabase()
		}

		var view *sql.View

		if dbName != "" {
			db, err := a.Catalog.Database(ctx, dbName)
			if err != nil {
				if sql.ErrDatabaseAccessDeniedForUser.Is(err) || sql.ErrTableAccessDeniedForUser.Is(err) {
					return n, transform.SameTree, nil
				}
				return nil, transform.SameTree, err
			}

			maybeVdb := db
			if privilegedDatabase, ok := maybeVdb.(mysql_db.PrivilegedDatabase); ok {
				maybeVdb = privilegedDatabase.Unwrap()
			}
			if vdb, ok := maybeVdb.(sql.ViewDatabase); ok {
				viewDef, ok, err := vdb.GetView(ctx, viewName)
				if err != nil {
					return nil, transform.SameTree, err
				}

				if ok {
					query, err := parse.Parse(ctx, viewDef)
					if err != nil {
						return nil, transform.SameTree, err
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
				return n, transform.SameTree, nil
			} else if err != nil {
				return nil, transform.SameTree, err
			}
		}

		a.Log("view resolved: %q", viewName)

		query := view.Definition().Children()[0]

		// If this view is being asked for with an AS OF clause, then attempt to apply it to every table in the view.
		if urt.AsOf() != nil {
			query, _, err = applyAsOfToView(query, a, urt.AsOf())
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		// If the view name was qualified with a database name, apply that same qualifier to any tables in it
		if urt.Database() != "" {
			query, _, err = applyDatabaseQualifierToView(query, a, urt.Database())
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		n, err = view.Definition().WithChildren(query)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return n, transform.NewTree, nil
	})
}

func applyAsOfToView(n sql.Node, a *Analyzer, asOf sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	a.Log("applying AS OF clause to view definition")

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		a.Log("applying AS OF clause to view " + urt.Name())
		if urt.AsOf() != nil {
			return nil, transform.SameTree, sql.ErrIncompatibleAsOf.New(
				fmt.Sprintf("cannot combine AS OF clauses %s and %s",
					asOf.String(), urt.AsOf().String()))
		}

		n, err := urt.WithAsOf(asOf)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return n, transform.NewTree, nil
	})
}

func applyDatabaseQualifierToView(n sql.Node, a *Analyzer, dbName string) (sql.Node, transform.TreeIdentity, error) {
	a.Log("applying database qualifier to view definition")

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		urt, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		a.Log("applying database name to view table " + urt.Name())
		if urt.Database() == "" {
			n, err := urt.WithDatabase(dbName)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		}

		return n, transform.SameTree, nil
	})
}
