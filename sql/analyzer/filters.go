package analyzer

import (
	"reflect"

	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type filtersByTable map[string][]sql.Expression

func (f filtersByTable) merge(f2 filtersByTable) {
	for k, exprs := range f2 {
		f[k] = append(f[k], exprs...)
	}
}

// getFiltersByTable returns a map of table name to filter expressions on that table for the node provided
func getFiltersByTable(_ *sql.Context, n sql.Node) filtersByTable {
	filters := make(filtersByTable)
	plan.Inspect(n, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Filter:
			fs := exprToTableFilters(node.Expression)
			filters.merge(fs)
		}
		return true
	})

	return filters
}

// exprToTableFilters returns a map of table name to filter expressions on that table
func exprToTableFilters(expr sql.Expression) filtersByTable {
	filtersByTable := make(filtersByTable)
	for _, expr := range splitConjunction(expr) {
		var seenTables = make(map[string]bool)
		var lastTable string
		sql.Inspect(expr, func(e sql.Expression) bool {
			f, ok := e.(*expression.GetField)
			if ok {
				if !seenTables[f.Table()] {
					seenTables[f.Table()] = true
					lastTable = f.Table()
				}
			}

			return true
		})

		if len(seenTables) == 1 {
			filtersByTable[lastTable] = append(filtersByTable[lastTable], expr)
		}
	}

	return filtersByTable
}

type filterSet struct {
	filtersByTable      filtersByTable
	handledFilters      []sql.Expression
	handledIndexFilters []string
}

func newFilterSet(filtersByTable filtersByTable) *filterSet {
	return &filterSet{
		filtersByTable: filtersByTable,
	}
}

// availableFiltersForTable returns the filters that are still available for the table given (not previous marked
// handled)
func (fs *filterSet) availableFiltersForTable(table string) []sql.Expression {
	filters, ok := fs.filtersByTable[table]
	if !ok {
		return nil
	}
	return subtractExprStrs(subtractExprSet(filters, fs.handledFilters), fs.handledIndexFilters)
}

// markFilterUsed marks the filter given as handled, so it will no longer be returned by availableFiltersForTable
func (fs *filterSet) markFiltersHandled(exprs ...sql.Expression) {
	fs.handledFilters = append(fs.handledFilters, exprs...)
}

func (fs *filterSet) markIndexesHandled(indexes []sql.Index) {
	for _, index := range indexes {
		fs.handledIndexFilters = append(fs.handledIndexFilters, index.Expressions()...)
	}
}

// splitConjunction breaks AND expressions into their left and right parts, recursively
func splitConjunction(expr sql.Expression) []sql.Expression {
	and, ok := expr.(*expression.And)
	if !ok {
		return []sql.Expression{expr}
	}

	return append(
		splitConjunction(and.Left),
		splitConjunction(and.Right)...,
	)
}

// subtractExprSet returns all expressions in the first parameter that aren't present in the second.
func subtractExprSet(all, toSubtract []sql.Expression) []sql.Expression {
	var remainder []sql.Expression

	for _, e := range all {
		var found bool
		for _, s := range toSubtract {
			if reflect.DeepEqual(e, s) {
				found = true
				break
			}
		}

		if !found {
			remainder = append(remainder, e)
		}
	}

	return remainder
}

// subtractExprStrs returns all expressions in the first parameter that aren't present in the second parameter, using
// string representations of expressions to compare.
func subtractExprStrs(all []sql.Expression, toSubtract []string) []sql.Expression {
	var remainder []sql.Expression

	for _, e := range all {
		var found bool
		for _, s := range toSubtract {
			if e.String() == s {
				found = true
				break
			}
		}

		if !found {
			remainder = append(remainder, e)
		}
	}

	return remainder
}

