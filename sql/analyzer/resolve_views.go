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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveViews(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_views")
	defer span.End()

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

			if vdb, vok := db.(sql.ViewDatabase); vok {
				viewDef, vdok, verr := vdb.GetViewDefinition(ctx, viewName)
				if verr != nil {
					return nil, transform.SameTree, verr
				}
				if vdok {
					query, qerr := parse.Parse(ctx, viewDef.TextDefinition)
					if qerr != nil {
						return nil, transform.SameTree, qerr
					}
					view = plan.NewSubqueryAlias(viewName, viewDef.TextDefinition, query).AsView(viewDef.CreateViewStatement)
				}
			}
		}

		// If we didn't find the view from the database directly, use the in-session registry
		var err error
		if view == nil {
			view, ok = ctx.GetViewRegistry().View(dbName, viewName)
			if !ok {
				return n, transform.SameTree, nil
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

// applyAsOfToView transforms the nodes in the view's execution plan to apply the asOf expression to every
// individual table involved in the view.
func applyAsOfToView(n sql.Node, a *Analyzer, asOf sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	a.Log("applying AS OF clause to view definition")

	// Transform any tables in our node tree so that they use the AsOf expression
	newNode, nodeIdentity, err := applyAsOfToViewTables(n, a, asOf)
	if err != nil {
		return n, transform.SameTree, err
	}

	// Subquery expressions won't get updated by the Node transform above, but we still need to update
	// any UnresolvedTable references in them to set the AsOf expression correctly.
	newNode, exprIdentity, err := applyAsOfToViewSubqueries(newNode, a, asOf)
	if err != nil {
		return n, transform.SameTree, err
	}

	identity := transform.SameTree
	if exprIdentity == transform.NewTree || nodeIdentity == transform.NewTree {
		identity = transform.NewTree
	}

	return newNode, identity, nil
}

// applyAsOfToViewSubqueries transforms the specified node by traversing its expressions, finding all the subquery expressions,
// and running applyAsOfToViewTables on each subquery's query node.
func applyAsOfToViewSubqueries(n sql.Node, a *Analyzer, asOf sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithNode(n, func(node sql.Node, expression sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if sq, ok := expression.(*plan.Subquery); ok {
			newNode, identity, err := applyAsOfToViewTables(sq.Query, a, asOf)
			if err != nil {
				return expression, transform.SameTree, err
			}
			if identity == transform.NewTree {
				return sq.WithQuery(newNode), transform.NewTree, nil
			}
		}

		return expression, transform.SameTree, nil
	})
}

// applyAsOfToViewTables transforms the specified node tree by finding all UnresolvedTable nodes
// and setting their AsOf expression to the value specified.
func applyAsOfToViewTables(newNode sql.Node, a *Analyzer, asOf sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithOpaque(newNode, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
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
