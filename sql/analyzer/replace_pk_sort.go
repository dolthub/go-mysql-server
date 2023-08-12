package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// replacePkSort applies an IndexAccess when there is an `OrderBy` over a prefix of any `PrimaryKey`s
func replacePkSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return replacePkSortHelper(ctx, scope, n, nil)
}

func replacePkSortHelper(ctx *sql.Context, scope *plan.Scope, node sql.Node, sortNode *plan.Sort) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *plan.Sort:
		sortNode = n // TODO: this only preserves the most recent Sort node
	// TODO: make this work with IndexedTableAccess, if we are statically sorting by the same col
	case *plan.ResolvedTable:
		// No sort node above this, so do nothing
		if sortNode == nil {
			return n, transform.SameTree, nil
		}
		tableAliases, err := getTableAliases(sortNode, scope)
		if err != nil {
			return n, transform.SameTree, nil
		}
		sfExprs := normalizeExpressions(tableAliases, sortNode.SortFields.ToExpressions()...)
		sfAliases := aliasedExpressionsInNode(sortNode)
		table := n.UnderlyingTable()
		idxTbl, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		var pkIndex sql.Index // TODO: support secondary indexes
		for _, idx := range idxs {
			if idx.ID() == "PRIMARY" {
				pkIndex = idx
				break
			}
		}
		if pkIndex == nil {
			return n, transform.SameTree, nil
		}

		pkColNames := pkIndex.Expressions()
		if len(sfExprs) > len(pkColNames) {
			return n, transform.SameTree, nil
		}
		for i, fieldExpr := range sfExprs {
			// TODO: could generalize this to more monotonic expressions.
			// For example, order by x+1 is ok, but order by mod(x) is not
			if sortNode.SortFields[0].Order != sortNode.SortFields[i].Order {
				return n, transform.SameTree, nil
			}
			fieldName := fieldExpr.String()
			if alias, ok := sfAliases[strings.ToLower(pkColNames[i])]; ok && alias == fieldName {
				continue
			}
			if strings.ToLower(pkColNames[i]) != strings.ToLower(fieldExpr.String()) {
				return n, transform.SameTree, nil
			}
		}

		// Create lookup based off of PrimaryKey
		indexBuilder := sql.NewIndexBuilder(pkIndex)
		lookup, err := indexBuilder.Build(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}
		lookup.IsReverse = sortNode.SortFields[0].Order == sql.Descending
		// Some Primary Keys (like doltHistoryTable) are not in order
		if oi, ok := pkIndex.(sql.OrderedIndex); ok && ((lookup.IsReverse && !oi.Reversible()) || oi.Order() == sql.IndexOrderNone) {
			return n, transform.SameTree, nil
		}
		if !pkIndex.CanSupport(lookup.Ranges...) {
			return n, transform.SameTree, nil
		}
		nn, err := plan.NewStaticIndexedAccessForResolvedTable(n, lookup)
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
		case *plan.Project, *plan.TableAlias, *plan.ResolvedTable, *plan.Filter, *plan.Limit, *plan.Offset, *plan.Sort:
			newChildren[i], same, err = replacePkSortHelper(ctx, scope, child, sortNode)
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

	newNode, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}
