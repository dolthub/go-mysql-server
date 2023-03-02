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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func resolveTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_tables")
	defer span.End()

	return transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		ignore := false
		switch p := c.Parent.(type) {
		case *plan.DropTable:
			ignore = p.IfExists()
		}

		switch p := c.Node.(type) {
		case *plan.UnresolvedTable:
			r, err := resolveTable(ctx, p, a)
			if sql.ErrTableNotFound.Is(err) && ignore {
				return p, transform.SameTree, nil
			}
			return r, transform.NewTree, err
		case *plan.InsertInto:
			if with, ok := p.Source.(*plan.With); ok {
				newSrc, same, err := resolveCommonTableExpressions(ctx, a, with, scope, sel)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if !same {
					newSrc, _, err = resolveSubqueries(ctx, a, newSrc, scope, sel)
					if err != nil {
						return nil, transform.SameTree, err
					}
					return p.WithSource(newSrc), transform.NewTree, nil
				}
			}

			newSrc, same, err := resolveTables(ctx, a, p.Source, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return p, transform.SameTree, nil
			}
			return p.WithSource(newSrc), transform.NewTree, nil
		default:
			return p, transform.SameTree, nil
		}
	})
}

func resolveTable(ctx *sql.Context, t sql.UnresolvedTable, a *Analyzer) (sql.Node, error) {
	name := t.Name()
	db := t.Database()
	if db == "" {
		db = ctx.GetCurrentDatabase()
	}

	var asofBindVar bool
	if t.AsOf() != nil {
		asofBindVar = transform.InspectExpr(t.AsOf(), func(expr sql.Expression) bool {
			_, ok := expr.(*expression.BindVar)
			return ok
		})
		if !asofBindVar {
			// This is necessary to use functions in AS OF expressions. Because function resolution happens after table
			// resolution, we resolve any functions in the asOf here in order to evaluate them immediately. A better solution
			// might be to defer evaluating the expression until later in the analysis, but that requires bigger changes.
			asOfExpr, _, err := transform.Expr(t.AsOf(), resolveFunctionsInExpr(ctx, a))
			if err != nil {
				return nil, err
			}

			// special case for AsOf's that use naked identifiers; they are interpreted as UnresolvedColumns
			if col, ok := asOfExpr.(*expression.UnresolvedColumn); ok {
				asOfExpr = expression.NewLiteral(col.String(), types.LongText)
			}

			if !asOfExpr.Resolved() {
				return nil, sql.ErrInvalidAsOfExpression.New(asOfExpr.String())
			}

			asOf, err := asOfExpr.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			rt, database, err := a.Catalog.TableAsOf(ctx, db, name, asOf)
			if err != nil {
				if sql.ErrDatabaseNotFound.Is(err) {
					if db == "" {
						err = sql.ErrNoDatabaseSelected.New()
					}
				}
				return nil, err
			}

			a.Log("table resolved: %q as of %s", rt.Name(), asOf)
			return plan.NewResolvedTable(rt, database, asOf), nil
		}
	}

	rt, database, err := a.Catalog.Table(ctx, db, name)
	if err != nil {
		if sql.ErrDatabaseNotFound.Is(err) {
			if db == "" {
				err = sql.ErrNoDatabaseSelected.New()
			}
		}
		return nil, err
	}

	resolvedTableNode := plan.NewResolvedTable(rt, database, nil)

	a.Log("table resolved: %s", t.Name())
	if asofBindVar {
		return plan.NewDeferredAsOfTable(resolvedTableNode, t.AsOf()), nil
	}
	return resolvedTableNode, nil
}

// setTargetSchemas fills in the target schema for any nodes in the tree that operate on a table node but also want to
// store supplementary schema information. This is useful for lazy resolution of column default values.
func setTargetSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("set_target_schema")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		t, ok := n.(sql.SchemaTarget)
		if !ok {
			return n, transform.SameTree, nil
		}

		// Skip filling in target schema info for CreateTable nodes, since the
		// target schema must be provided by the user and we don't want to pick
		//  up any resolved tables from a subquery node.
		if _, ok := n.(*plan.CreateTable); ok {
			return n, transform.SameTree, nil
		}

		table := getResolvedTable(n)
		if table == nil {
			return n, transform.SameTree, nil
		}

		var err error
		n, err = t.WithTargetSchema(table.Schema())
		if err != nil {
			return nil, transform.SameTree, err
		}

		pkst, ok := n.(sql.PrimaryKeySchemaTarget)
		if !ok {
			return n, transform.NewTree, nil
		}

		pkt, ok := table.Table.(sql.PrimaryKeyTable)
		if !ok {
			return n, transform.NewTree, nil
		}

		n, err = pkst.WithPrimaryKeySchema(pkt.PrimaryKeySchema())
		return n, transform.NewTree, err
	})
}

// reresolveTables is a quick and dirty way to make prepared statement reanalysis
// resolve the most up-to-date table roots while preserving projections folded into
// table scans.
// TODO this is racy, alter statements can change a table's schema in-between
// prepare and execute
func reresolveTables(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var (
			from *plan.ResolvedTable
			to   sql.Node
			db   string
			err  error
		)
		switch n := n.(type) {
		case *plan.ResolvedTable:
			from = n
			if n.Database != nil {
				db = n.Database.Name()
			}
			var asof sql.Expression
			if n.AsOf != nil {
				asof = expression.NewLiteral(n.AsOf, nil)
			}
			if plan.IsDualTable(n) {
				to = n
			} else {
				to, err = resolveTable(ctx, plan.NewUnresolvedTableAsOf(n.Name(), db, asof), a)
				if err != nil {
					return nil, transform.SameTree, err
				}
			}
			new := transferProjections(ctx, from, to.(*plan.ResolvedTable))
			return new, transform.NewTree, nil
		case *plan.IndexedTableAccess:
			from = n.ResolvedTable
			if n.Database() != nil {
				db = n.Database().Name()
			}
			to, err = resolveTable(ctx, plan.NewUnresolvedTable(n.ResolvedTable.Name(), db), a)
			if err != nil {
				return nil, transform.SameTree, err
			}
			new := *n
			new.ResolvedTable = transferProjections(ctx, from, to.(*plan.ResolvedTable))
			return &new, transform.NewTree, nil
		case *plan.DeferredAsOfTable:
			from = n.ResolvedTable
			to, err = resolveTable(ctx, plan.NewDeferredAsOfTable(n.ResolvedTable, n.AsOf()), a)
			if err != nil {
				return nil, transform.SameTree, err
			}
			new := transferProjections(ctx, from, to.(*plan.ResolvedTable))
			return new, transform.NewTree, nil
		default:
		}
		if err != nil {
			return nil, transform.SameTree, err
		}
		return n, transform.SameTree, nil
	})
}

// transferProjections moves projections from one table scan to another
func transferProjections(ctx *sql.Context, from, to *plan.ResolvedTable) *plan.ResolvedTable {
	var fromTable sql.Table
	switch t := from.Table.(type) {
	case sql.TableWrapper:
		fromTable = t.Underlying()
	case sql.Table:
		fromTable = t
	default:
		return to
	}

	var filters []sql.Expression
	if ft, ok := fromTable.(sql.FilteredTable); ok {
		filters = ft.Filters()
	}

	var projections []string
	if pt, ok := fromTable.(sql.ProjectedTable); ok {
		projections = pt.Projections()
	}

	var toTable sql.Table
	switch t := to.Table.(type) {
	case sql.TableWrapper:
		toTable = t.Underlying()
	case sql.Table:
		toTable = t
	default:
		return to
	}

	changed := false

	if _, ok := toTable.(sql.FilteredTable); ok && filters != nil {
		toTable = toTable.(sql.FilteredTable).WithFilters(ctx, filters)
		changed = true
	}

	if _, ok := toTable.(sql.ProjectedTable); ok && projections != nil {
		toTable = toTable.(sql.ProjectedTable).WithProjections(projections)
		changed = true
	}

	if !changed {
		return to
	}

	return plan.NewResolvedTable(toTable, to.Database, to.AsOf)
}

// validateDropTables returns an error if the database is not droppable.
func validateDropTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	dt, ok := n.(*plan.DropTable)
	if !ok {
		return n, transform.SameTree, nil
	}

	// validates that each table in DropTable is ResolvedTable and each database of
	// each table is TableDropper (each table can be of different database later on)
	var resolvedTables []sql.Node
	for _, table := range dt.Tables {
		switch t := table.(type) {
		case *plan.ResolvedTable:
			_, ok = t.Database.(sql.TableDropper)
			if !ok {
				return nil, transform.SameTree, sql.ErrDropTableNotSupported.New(t.Database.Name())
			}
			resolvedTables = append(resolvedTables, table)
		case *plan.UnresolvedTable:
			if !dt.IfExists() {
				return nil, transform.SameTree, sql.ErrUnknownTable.New(t.String())
			}
		case *plan.SubqueryAlias:
			return nil, transform.SameTree, sql.ErrUnknownTable.New(t.Name())
		default:
			// TODO: try to get the name used for the error rather than the node plan string
			return nil, transform.SameTree, sql.ErrUnknownTable.New(table.String())
		}
	}

	newn, _ := n.WithChildren(resolvedTables...)
	return newn, transform.NewTree, nil

}
