package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
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
		sortNode = n // lowest parent sort node
	case *plan.IndexedTableAccess:
		if sortNode == nil || !isValidSortFieldOrder(sortNode.SortFields) {
			return n, transform.SameTree, nil
		}
		if !n.IsStatic() {
			return n, transform.SameTree, nil
		}
		lookup, err := n.GetLookup(ctx, nil)
		if err != nil {
			return nil, transform.SameTree, err
		}
		idxExprs := lookup.Index.Expressions()
		tableAliases, err := getTableAliases(sortNode, scope)
		if err != nil {
			return n, transform.SameTree, nil
		}
		if !isSortFieldsValidPrefix(sortNode, tableAliases, idxExprs) {
			return n, transform.SameTree, nil
		}

		// if the lookup does not need any reversing, do nothing
		if sortNode.SortFields[0].Order != sql.Descending {
			return n, transform.NewTree, nil
		}

		// modify existing lookup to preserve pushed down filters
		lookup.IsReverse = true

		// Some Primary Keys (like doltHistoryTable) are not in order
		if oi, ok := lookup.Index.(sql.OrderedIndex); ok && ((lookup.IsReverse && !oi.Reversible()) || oi.Order() == sql.IndexOrderNone) {
			return n, transform.SameTree, nil
		}
		nn, err := plan.NewStaticIndexedAccessForTableNode(n.TableNode, lookup)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return nn, transform.NewTree, err
	case *plan.ResolvedTable:
		if sortNode == nil || !isValidSortFieldOrder(sortNode.SortFields) {
			return n, transform.SameTree, nil
		}

		table := n.UnderlyingTable()
		idxTbl, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		// TODO: support secondary indexes
		pkIndex, err := getPKIndex(ctx, idxTbl)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if pkIndex == nil {
			return n, transform.SameTree, nil
		}
		pkColNames := pkIndex.Expressions()

		tableAliases, err := getTableAliases(sortNode, scope)
		if err != nil {
			return n, transform.SameTree, nil
		}
		if !isSortFieldsValidPrefix(sortNode, tableAliases, pkColNames) {
			return n, transform.SameTree, nil
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
		nn, err := plan.NewStaticIndexedAccessForTableNode(n, lookup)
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
		case *plan.Project, *plan.TableAlias, *plan.ResolvedTable, *plan.Filter, *plan.Limit, *plan.Offset, *plan.Sort, *plan.IndexedTableAccess:
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

// replaceAgg converts aggregate functions to order by + limit 1 when possible
func replaceAgg(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// project with groupby child
		proj, ok := n.(*plan.Project)
		if !ok {
			return n, transform.SameTree, nil
		}
		gb, ok := proj.Child.(*plan.GroupBy)
		if !ok {
			return n, transform.SameTree, nil
		}
		// TODO: optimize when there are multiple aggregations; use LATERAL JOINS
		if len(gb.SelectedExprs) != 1 || len(gb.GroupByExprs) != 0 {
			return n, transform.SameTree, nil
		}

		// TODO: support secondary indexes
		var pkIdx sql.Index
		switch t := gb.Child.(type) {
		case *plan.IndexedTableAccess:
			if _, ok := t.Table.(sql.IndexAddressableTable); ok {
				idx := t.Index()
				if idx.ID() != "PRIMARY" {
					return n, transform.SameTree, nil
				}
				pkIdx = idx
			}
		case *plan.ResolvedTable:
			if tbl, ok := t.UnderlyingTable().(sql.IndexAddressableTable); ok {
				idx, err := getPKIndex(ctx, tbl)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if idx == nil {
					return n, transform.SameTree, nil
				}
				pkIdx = idx
			}
		default:
			return n, transform.SameTree, nil
		}

		// generate sort fields from aggregations
		var sf sql.SortField
		switch agg := gb.SelectedExprs[0].(type) {
		case *aggregation.Max:
			gf, ok := agg.UnaryExpression.Child.(*expression.GetField)
			if !ok {
				return n, transform.SameTree, nil
			}
			sf = sql.SortField{
				Column: gf,
				Order:  sql.Descending,
			}
		case *aggregation.Min:
			gf, ok := agg.UnaryExpression.Child.(*expression.GetField)
			if !ok {
				return n, transform.SameTree, nil
			}
			sf = sql.SortField{
				Column: gf,
				Order:  sql.Ascending,
			}
		default:
			return n, transform.SameTree, nil
		}

		// since we're only supporting one aggregation, it must be on the first column of the primary key
		if !strings.EqualFold(pkIdx.Expressions()[0], sf.Column.String()) {
			return n, transform.SameTree, nil
		}

		// replace all aggs in proj.Projections with GetField
		name := gb.SelectedExprs[0].String()
		newProjs, _, err := transform.Exprs(proj.Projections, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if strings.EqualFold(e.String(), name) {
				return sf.Column, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		})
		if err != nil {
			return nil, transform.SameTree, err
		}
		newProj := plan.NewProject(newProjs, plan.NewSort(sql.SortFields{sf}, gb.Child))
		limit := plan.NewLimit(expression.NewLiteral(1, types.Int64), newProj)
		return limit, transform.NewTree, nil
	})
}

// isSortFieldsValidPrefix checks is the SortFields in sortNode are a valid prefix of the PrimaryKey
func isSortFieldsValidPrefix(sortNode *plan.Sort, tableAliases TableAliases, pkColNames []string) bool {
	sfExprs := normalizeExpressions(tableAliases, sortNode.SortFields.ToExpressions()...)
	sfAliases := aliasedExpressionsInNode(sortNode)
	if len(sfExprs) > len(pkColNames) {
		return false
	}

	for i, fieldExpr := range sfExprs {
		fieldName := fieldExpr.String()
		if alias, ok := sfAliases[strings.ToLower(pkColNames[i])]; ok && alias == fieldName {
			continue
		}
		if !strings.EqualFold(pkColNames[i], fieldName) {
			return false
		}
	}
	return true
}

// isValidSortFieldOrder checks if all the sortfields are in the same order
func isValidSortFieldOrder(sfs sql.SortFields) bool {
	for _, sf := range sfs {
		// TODO: could generalize this to more monotonic expressions.
		//   For example, order by x+1 is ok, but order by mod(x) is not
		if sfs[0].Order != sf.Order {
			return false
		}
	}
	return true
}

// getPKIndex returns the primary key index of an IndexAddressableTable
func getPKIndex(ctx *sql.Context, idxTbl sql.IndexAddressableTable) (sql.Index, error) {
	idxs, err := idxTbl.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	for _, idx := range idxs {
		if idx.ID() == "PRIMARY" {
			return idx, nil
		}
	}
	return nil, nil
}
