package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// generateIndexScans generates indexscan alternatives for sql.IndexAddressableTable
// relations with filters.
func generateIndexScans(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("generate_index_scans")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	indexes, err := getIndexesByTable(ctx, a, n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	node, same, err := convertFiltersToIndexedAccess(ctx, a, n, scope, indexes, tableAliases)
	if err != nil {
		return nil, transform.SameTree, err
	}

	if !filterHasBindVar(n) {
		return node, same, err
	}

	// Wrap with DeferredFilteredTable if there are bindvars
	return transform.NodeWithOpaque(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if rt, ok := n.(*plan.ResolvedTable); ok {
			if _, ok := rt.Table.(sql.FilteredTable); ok {
				return plan.NewDeferredFilteredTable(rt), transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

// convertFiltersToIndexedAccess attempts to replace filter predicates with indexed accesses where possible
// TODO: this function doesn't actually remove filters that have been converted to index lookups,
// that optimization is handled in transformPushdownFilters.
func convertFiltersToIndexedAccess(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *plan.Scope,
	indexes indexLookupsByTable,
	tableAliases TableAliases,
) (sql.Node, transform.TreeIdentity, error) {
	childSelector := func(c transform.Context) bool {
		switch n := c.Node.(type) {
		// We can't push any indexes down to a table has already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		case *plan.RecursiveCte:
			// TODO: fix memory IndexLookup bugs that are not reproduceable in Dolt
			// this probably fails for *plan.SetOp also, we just don't have tests for it
			return false
		case *plan.JoinNode:
			// avoid changing anti and semi join condition indexes
			return !n.Op.IsPartial() && !n.Op.IsFullOuter()
		}

		switch n := c.Parent.(type) {
		// For IndexedJoins, if we are already using indexed access during query execution for the secondary table,
		// replacing the secondary table with an indexed lookup will have no effect on the result of the join, but
		// *will* inappropriately remove the filter from the predicate.
		// TODO: the analyzer should combine these indexed lookups better
		case *plan.JoinNode:
			if n.Op.IsMerge() {
				return false
			} else if n.Op.IsLookup() || n.Op.IsLeftOuter() {
				return c.ChildNum == 0
			}
		case *plan.TableAlias:
			// For a TableAlias, we apply this pushdown to the
			// TableAlias, but not to the resolved table directly
			// beneath it.
			return false
		case *plan.Window:
			// Windows operate across the rows they see and cannot
			// have filters pushed below them. If there is an index
			// pushdown, it will get picked up in the isolated pass
			// run by the filters pushdown transform.
			return false
		case *plan.Filter:
			// Can't push Filter Nodes below Limit Nodes
			if _, ok := c.Node.(*plan.Limit); ok {
				return false
			}
			if p, ok := c.Node.(*plan.Project); ok {
				if _, ok := p.Child.(*plan.Limit); ok {
					return false
				}
			}
		}
		return true
	}

	var handled []sql.Expression
	return transform.NodeWithCtx(n, childSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.Filter:
			filtersByTable := getFiltersByTable(n)
			filters := newFilterSet(n.Expression, filtersByTable, tableAliases)
			filters.markFiltersHandled(handled...)
			newF := removePushedDownPredicates(ctx, a, n, filters)
			if newF == nil {
				return n, transform.SameTree, nil
			}
			return newF, transform.NewTree, nil
		case *plan.TableAlias, *plan.ResolvedTable:
			nameable := n.(sql.NameableNode)
			ret, same, err, lookup := pushdownIndexesToTable(ctx, scope, a, nameable, indexes)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return c.Node, transform.SameTree, nil
			}
			// TODO  mark lookup fields as used
			handledF, err := getPredicateExprsHandledByLookup(ctx, a, nameable.Name(), lookup, tableAliases)
			if err != nil {
				return nil, transform.SameTree, err
			}
			handled = append(handled, handledF...)
			return ret, transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// pushdownIndexesToTable attempts to convert filter predicates to indexes on tables that implement
// sql.IndexAddressableTable
func pushdownIndexesToTable(ctx *sql.Context, scope *plan.Scope, a *Analyzer, tableNode sql.NameableNode, indexes indexLookupsByTable) (sql.Node, transform.TreeIdentity, error, *indexLookup) {
	var lookup *indexLookup
	ret, same, err := transform.Node(tableNode, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case sql.TableNode:
			dbName := ""
			if n.Database() != nil {
				dbName = n.Database().Name()
			}
			table := getTable(tableNode)
			if table == nil {
				return n, transform.SameTree, nil
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				if indexLookup, ok := indexes[sql.NewTableID(dbName, tableNode.Name())]; ok {
					if indexLookup.lookup.Index.IsFullText() {
						matchAgainst, ok := indexLookup.expr.(*expression.MatchAgainst)
						if !ok {
							return nil, transform.SameTree, fmt.Errorf("Full-Text index found in filter with unknown expression: %T", indexLookup.expr)
						}
						if matchAgainst.KeyCols.Type == fulltext.KeyType_None {
							return n, transform.SameTree, nil
						}
						ret := plan.NewStaticIndexedAccessForFullTextTable(n, indexLookup.lookup, &rowexec.FulltextFilterTable{
							MatchAgainst: matchAgainst,
							Table:        n,
						})
						// save reference
						lookup = indexLookup

						return ret, transform.NewTree, nil
					} else if indexLookup.lookup.Index.CanSupport(indexLookup.lookup.Ranges...) {
						a.Log("table %q transformed with pushdown of index", tableNode.Name())
						ita, err := plan.NewStaticIndexedAccessForTableNode(n, indexLookup.lookup)
						if plan.ErrInvalidLookupForIndexedTable.Is(err) {
							return n, transform.SameTree, nil
						}
						if err != nil {
							return nil, transform.SameTree, err
						}

						// save reference
						lookup = indexLookup
						return ita, transform.NewTree, nil
					}
				}
			}
		}
		return n, transform.SameTree, nil
	})
	return ret, same, err, lookup
}

// pushdownFiltersToTable attempts to push down filters to indexes that can accept them.
func getPredicateExprsHandledByLookup(ctx *sql.Context, a *Analyzer, name string, lookup *indexLookup, tableAliases TableAliases) ([]sql.Expression, error) {
	filteredIdx, ok := lookup.lookup.Index.(sql.FilteredIndex)
	if !ok {
		return nil, nil
	}
	// Spatial and Full-Text are lossy, so do not remove filter node above the lookup
	if filteredIdx.IsSpatial() || filteredIdx.IsFullText() {
		return nil, nil
	}

	idxFilters := expression.SplitConjunction(lookup.expr)
	if len(idxFilters) == 0 {
		return nil, nil
	}
	idxFilters = normalizeExpressions(tableAliases, idxFilters...)

	handled := filteredIdx.HandledFilters(idxFilters)
	if len(handled) == 0 {
		return nil, nil
	}

	a.Log(
		"table %q transformed with pushdown of filters to index %s, %d filters handled of %d",
		name,
		lookup.lookup.Index.ID(),
		len(handled),
		len(idxFilters),
	)
	return handled, nil
}

// filterHasBindVar looks for any BindVars found in filter nodes
func filterHasBindVar(filter sql.Node) bool {
	var hasBindVar bool
	transform.Inspect(filter, func(node sql.Node) bool {
		if fn, ok := node.(*plan.Filter); ok {
			for _, expr := range fn.Expressions() {
				if exprHasBindVar(expr) {
					hasBindVar = true
					return false
				}
			}
		}
		return !hasBindVar // stop recursing if bindvar already found
	})
	return hasBindVar
}

// exprHasBindVar looks for any BindVars found in expressions
func exprHasBindVar(expr sql.Expression) bool {
	var hasBindVar bool
	transform.InspectExpr(expr, func(e sql.Expression) bool {
		if _, ok := e.(*expression.BindVar); ok {
			hasBindVar = true
			return true
		}
		return false
	})
	return hasBindVar
}
