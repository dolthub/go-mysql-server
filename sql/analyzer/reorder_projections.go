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
)

// reorderProjection adds intermediate Project nodes to the descendants of existing Project nodes, adding fields to
// the schemas of these intermediate nodes. This is important because the naive parse tree might a descendant of a
// Project refer to a node introduced in that project (typically an alias). For the child to be able to resolve this
// reference, it needs to be pushed lower down in the tree, underneath that child.
// The canonical case here looks something like:
// Project([a, 1 as foo], Sort(foo, table))
// To resolve the reference "foo", the Sort node needs it to be present in a child node. So we push that alias down, in
// a new Project which wraps the original child of the Sort:
// Project([a, foo], Sort(foo, Project([a, 1 as foo], table)))
// This process also converts higher-level projected fields to GetField expressions, since we don't want to evaluate
// the original expression more than once (which could actually produce incorrect results in some cases).
func reorderProjection(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("reorder_projection")
	defer span.End()

	if n.Resolved() {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		project, ok := node.(*plan.Project)
		// When we transform the projection, the children will always be
		// unresolved in the case we want to fix, as the reorder happens just
		// so some columns can be resolved.
		// For that, we need to account for NaturalJoin, whose schema can't be
		// obtained until it's resolved and ignore the projection for the
		// moment until the resolve_natural_joins has finished resolving the
		// node and we can tackle it in the next iteration.
		// Without this check, it would cause a panic, because NaturalJoin's
		// schema method is just a placeholder that should not be called.
		if !ok || hasNaturalJoin(project.Child) {
			return node, transform.SameTree, nil
		}

		// We must find all aliases that may need to be moved inside the projection.
		var projectedAliases = make(map[string]sql.Expression)
		for _, col := range project.Projections {
			alias, ok := col.(*expression.Alias)
			// If there are duplicate alias names defined in the same projection, we need to be careful about
			// which one we select. We have moved closer to MySQL's behavior by keeping only the first seen
			if ok {
				if _, ok := projectedAliases[alias.Name()]; !ok {
					projectedAliases[alias.Name()] = col
				}
			}
		}

		// And add projection nodes where needed in the child tree.
		child, same, err := addIntermediateProjections(project, projectedAliases)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if same {
			return node, transform.SameTree, nil
		}

		// To do the reordering, we need to reason about column types, which means the child needs to be resolved.
		// If it can't be resolved, we can't continue.
		child, _, err = resolveColumns(ctx, a, child, scope, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}

		child, _, err = resolveSubqueries(ctx, a, child, scope, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if !child.Resolved() {
			return node, transform.SameTree, nil
		}

		childSchema := child.Schema()
		// Finally, replace the columns we moved with GetFields since they
		// have already been projected.
		var projections = make([]sql.Expression, len(project.Projections))
		for i, p := range project.Projections {
			if alias, ok := p.(*expression.Alias); ok {
				var found bool
				for idx, col := range childSchema {
					if col.Name == alias.Name() {
						projections[i] = expression.NewGetField(
							idx, col.Type, col.Name, col.Nullable,
						)
						found = true
						break
					}
				}

				if !found {
					projections[i] = p
				}
			} else {
				projections[i] = p
			}
		}

		return plan.NewProject(projections, child), transform.NewTree, nil
	})
}

func addIntermediateProjections(
	project *plan.Project,
	projectedAliases map[string]sql.Expression,
) (sql.Node, transform.TreeIdentity, error) {
	// We only want to apply each projection once, even if it
	// occurs multiple times in the tree. Lower tree levels are
	// processed first, so only the lowest mention of each
	// alias will be applied at that layer. Higher layers will
	// just have a normal GetField expression to reference the
	// lower layer.
	appliedProjections := make(map[string]bool)
	child, same, err := transform.Node(project.Child, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var missingColumns []string
		switch node := node.(type) {
		case *plan.Sort, *plan.Filter:
			for _, expr := range node.(sql.Expressioner).Expressions() {
				sql.Inspect(expr, func(e sql.Expression) bool {
					if e != nil && e.Resolved() {
						return true
					}

					uc, ok := e.(column)
					if ok && uc.Table() == "" {
						if _, ok := projectedAliases[uc.Name()]; ok && !appliedProjections[uc.Name()] {
							missingColumns = append(missingColumns, uc.Name())
						}
					}

					return true
				})
			}
		default:
			return node, transform.SameTree, nil
		}

		if len(missingColumns) == 0 {
			return node, transform.SameTree, nil
		}

		// Only add the required columns for that node in the projection.
		child := node.Children()[0]
		schema := child.Schema()
		var projections = make([]sql.Expression, 0, len(schema)+len(missingColumns))
		for i, col := range schema {
			projections = append(projections, expression.NewGetFieldWithTable(
				i, col.Type, col.Source, col.Name, col.Nullable,
			))
		}

		for _, col := range missingColumns {
			if c, ok := projectedAliases[col]; ok && !appliedProjections[col] {
				projections = append(projections, c)
				appliedProjections[col] = true
			}
		}

		child = plan.NewProject(projections, child)
		switch node := node.(type) {
		case *plan.Filter:
			return plan.NewFilter(node.Expression, child), transform.NewTree, nil
		case *plan.Sort:
			return plan.NewSort(node.SortFields, child), transform.NewTree, nil
		default:
			return nil, transform.SameTree, ErrInvalidNodeType.New("reorderProjection", node)
		}
	})

	// If any subqueries reference these aliases, the child of the project also needs it. A subquery expression is just
	// like a child node in this respect -- it draws its outer scope schema from the child of the node in which it's
	// embedded. We identify any missing subquery columns by their being deferred or marked as an AliasReference
	// from a previous analyzer step.
	var deferredColumns []column
	for _, e := range project.Projections {
		if a, ok := e.(*expression.Alias); ok {
			e = a.Child
		}
		s, ok := e.(*plan.Subquery)
		if !ok {
			continue
		}

		deferredColumns = append(deferredColumns, findDeferredColumnsAndAliasReferences(s.Query)...)
	}

	if len(deferredColumns) > 0 {
		schema := child.Schema()
		var projections = make([]sql.Expression, 0, len(schema)+len(deferredColumns))
		for i, col := range schema {
			projections = append(projections, expression.NewGetFieldWithTable(
				i, col.Type, col.Source, col.Name, col.Nullable,
			))
		}

		// Add a projection for each missing column from the subqueries that has an alias
		for _, dc := range deferredColumns {
			if c, ok := projectedAliases[dc.Name()]; ok && dc.Table() == "" {
				projections = append(projections, c)
				same = transform.NewTree
			}
		}

		child = plan.NewProject(projections, child)
	}

	return child, same, err
}

// findDeferredColumnsAndAliasReferences returns all the deferredColumn and AliasReference expressions in the node given
func findDeferredColumnsAndAliasReferences(n sql.Node) []column {
	var cols []column
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		switch ee := e.(type) {
		case *deferredColumn:
			cols = append(cols, ee)
		case *expression.AliasReference:
			cols = append(cols, ee)
		}
		return true
	})

	return cols
}

// hasNaturalJoin checks whether there is a natural join at some point in the
// given node and its children.
func hasNaturalJoin(node sql.Node) bool {
	var found bool
	transform.Inspect(node, func(node sql.Node) bool {
		if j, ok := node.(*plan.JoinNode); ok {
			found = j.Op.IsNatural()
			return false
		}
		return true
	})
	return found
}
