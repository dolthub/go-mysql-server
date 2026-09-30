package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// replaceIdxSort applies an IndexAccess when there is an `OrderBy` over a prefix of any columns with Indexes
func replaceIdxOrderByDistance(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return replaceIdxOrderByDistanceHelper(ctx, scope, n, nil, nil)
}

func replaceIdxOrderByDistanceHelper(ctx *sql.Context, scope *plan.Scope, node sql.Node, sortNode plan.Sortable, limit sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *plan.TopN:
		sortNode = n // lowest parent sort node
		limit = n.Limit
	case plan.Sortable:
		sortNode = n
	case *plan.Limit:
		limit = n.Limit
	case *plan.Offset:
		// Rows skipped by the offset still need to be produced by the index lookup
		if limit != nil {
			limit = expression.NewPlus(limit, n.Offset)
		}
	case *plan.Filter:
		// A filter between the limit and the table means the limit applies to the filtered rows so fetching only the
		// top rows from the index could return too few, so we fall back to an exact sort
		if sortNode != nil || limit != nil {
			return n, transform.SameTree, nil
		}
	case *plan.ResolvedTable:
		if sortNode == nil || limit == nil {
			return n, transform.SameTree, nil
		}

		table := n.UnderlyingTable()
		idxTbl, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		// A vector index only orders ascending (nearest first), so a descending sort must fall back to an exact sort
		sortConds := sortNode.GetSortConditions()
		if len(sortConds) != 1 || sortConds[0].Order != sql.Ascending {
			return n, transform.SameTree, nil
		}
		if indexSearchable, ok := table.(sql.IndexSearchableTable); ok && indexSearchable.SkipIndexCosting() {
			return n, transform.SameTree, nil
		}

		tableAliases, err := getTableAliases(ctx, sortNode, scope)
		if err != nil {
			return n, transform.SameTree, nil
		}

		var idx sql.Index
		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// Column references have not been assigned their final indexes yet, so do that for the ORDER BY expression now.
		// We can safely do this because an expression that references other tables won't pass `isSortFieldsValidPrefix` below.
		sortNode = offsetAssignIndexes(ctx, sortNode).(plan.Sortable)

		sortExprs := normalizeExpressions(ctx, tableAliases, nil, sortNode.GetSortConditions().ToExpressions()...)
		sortAliases := aliasedExpressionsInNode(sortNode)

		distance, isDistance := sortExprs[0].(vector.OrderableDistance)
		if !isDistance {
			return n, transform.SameTree, nil
		}

		expr, literal, ok := distance.TargetAndQuery()
		if !ok {
			return n, transform.SameTree, nil
		}

		for _, idxCandidate := range idxs {
			if idxCandidate.IsSpatial() {
				continue
			}
			if !idxCandidate.CanSupportOrderBy(distance) {
				continue
			}
			if sortExprsMatchIdxColExprs([]sql.Expression{expr}, sortAliases, idxCandidate.Expressions()) {
				idx = idxCandidate
				break
			}
		}
		if idx == nil {
			return n, transform.SameTree, nil
		}

		lookup := sql.IndexLookup{
			Index:  idx,
			Ranges: sql.MySQLRangeCollection{},
			VectorOrderAndLimit: sql.OrderAndLimit{
				OrderBy: distance,
				Limit:   limit,
				Literal: literal,
			},
		}
		nn, err := plan.NewStaticIndexedAccessForTableNode(ctx, n, lookup)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return nn, transform.NewTree, err
	}

	allSame := transform.SameTree
	newChildren := make([]sql.Node, len(node.Children()))
	for i, child := range node.Children() {
		var err error
		same := transform.SameTree
		switch c := child.(type) {
		case *plan.Project, *plan.TableAlias, *plan.ResolvedTable, *plan.Filter, *plan.Limit, *plan.TopN, *plan.Offset, *plan.Sort, *plan.IndexedTableAccess:
			newChildren[i], same, err = replaceIdxOrderByDistanceHelper(ctx, scope, child, sortNode, limit)
		default:
			newChildren[i] = c
		}
		if err != nil {
			return nil, transform.SameTree, err
		}
		allSame = allSame && same
	}

	if allSame {
		return node, transform.SameTree, nil
	}

	// if sort node was replaced with indexed access, drop sort node
	if node == sortNode {
		return newChildren[0], transform.NewTree, nil
	}

	newNode, err := node.WithChildren(ctx, newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}
