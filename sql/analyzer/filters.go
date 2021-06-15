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
	"reflect"

	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type filtersByTable struct {
	// filter expressions referring to a single table
	single map[string][]sql.Expression
	// filter expressions referring to multiple tables
	multi  []sql.Expression
}

func newFiltersByTable() filtersByTable {
	return filtersByTable{single: make(map[string][]sql.Expression)}
}

func (f filtersByTable) merge(f2 filtersByTable) {
	for k, exprs := range f2.single {
		f.single[k] = append(f.single[k], exprs...)
	}
	f.multi = append(f.multi, f2.multi...)
}

func (f filtersByTable) size() int {
	return len(f.single) + len(f.multi)
}

// getFiltersByTable returns a map of table name to filter expressions on that table for the node provided. Returns an
// error only the case that the filters contained in the node given cannot all be separated into tables (some of them
// have more than one table, or no table)
func getFiltersByTable(n sql.Node) (filtersByTable, error) {
	filters := newFiltersByTable()
	var err error
	plan.Inspect(n, func(node sql.Node) bool {
		if err != nil {
			return false
		}

		switch node := node.(type) {
		case *plan.Filter:
			var fs filtersByTable
			fs, err = exprToTableFilters(node.Expression)
			if err != nil {
				return false
			}
			filters.merge(fs)
		}
		if o, ok := node.(sql.OpaqueNode); ok {
			return !o.Opaque()
		}
		return true
	})

	if err != nil {
		return filtersByTable{}, err
	}

	return filters, err
}

// exprToTableFilters returns a map of table name to filter expressions on that table for all parts of the expression
// given, split at AND. Returns an error only the case that the expressions cannot all be separated into tables (some
// of them have more than one table, or no table)
func exprToTableFilters(expr sql.Expression) (filtersByTable, error) {
	filters := newFiltersByTable()
	for _, expr := range splitConjunction(expr) {
		var seenTables = make(map[string]bool)
		var lastTable string
		hasSubquery := false
		sql.Inspect(expr, func(e sql.Expression) bool {
			f, ok := e.(*expression.GetField)
			if ok {
				if !seenTables[f.Table()] {
					seenTables[f.Table()] = true
					lastTable = f.Table()
				}
			} else if _, isSubquery := e.(*plan.Subquery); isSubquery {
				hasSubquery = true
				return false
			}

			return true
		})

		if hasSubquery {
			return // handledCount returns the number of filter expressions that have been marked as handled
			func (fs *filterSet) handledCount() int {
				return len(fs.handledIndexFilters) + len(fs.handledFilters)
			}{}, fmt.Errorf("cannot factor tables in expression because it contains a subquery: %s", expr.String())
		}
		if len(seenTables) == 1 {
			filters.single[lastTable] = append(filters.single[lastTable], expr)
		} else {
			filters.multi = append(filters.multi, expr)
		}
	}

	return filters, nil
}

type filterSet struct {
	filtersByTable      filtersByTable
	handledFilters      []sql.Expression
	handledIndexFilters []string
	tableAliases        TableAliases
}

// newFilterSet returns a new filter set that will track available filters with the filters and aliases given. Aliases
// are necessary to normalize expressions from indexes when in the presence of aliases.
func newFilterSet(filtersByTable filtersByTable, tableAliases TableAliases) *filterSet {
	return &filterSet{
		filtersByTable: filtersByTable,
		tableAliases:   tableAliases,
	}
}

// availableFiltersForTable returns the filters that are still available for the table given (not previously marked
// handled)
func (fs *filterSet) availableFiltersForTable(ctx *sql.Context, table string) []sql.Expression {
	// don't provide multi-table filter expressions
	filters, ok := fs.filtersByTable.single[table]
	if !ok {
		return nil
	}
	return fs.subtractUsedIndexes(ctx, subtractExprSet(filters, fs.handledFilters))
}

// availableFilters returns the filters that are still available (not previously marked handled)
func (fs *filterSet) availableFilters(ctx *sql.Context) []sql.Expression {
	var available []sql.Expression
	for _, es := range fs.filtersByTable.single {
		available = append(available, fs.subtractUsedIndexes(ctx, subtractExprSet(es, fs.handledFilters))...)
	}
	available = append(available, fs.filtersByTable.multi...)
	return available
}

// handledCount returns the number of filter expressions that have been marked as handled
func (fs *filterSet) handledCount() int {
	return len(fs.handledIndexFilters) + len(fs.handledFilters)
}

// markFilterUsed marks the filter given as handled, so it will no longer be returned by availableFiltersForTable
func (fs *filterSet) markFiltersHandled(exprs ...sql.Expression) {
	fs.handledFilters = append(fs.handledFilters, exprs...)
}

// markIndexesHandled marks the indexes given as handled, so expressions on them will no longer be returned by
// availableFiltersForTable
// TODO: this is currently unused because we can't safely remove indexed predicates from the filter in all cases
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

// subtractUsedIndexes returns the filter expressions given with used indexes subtracted off.
func (fs *filterSet) subtractUsedIndexes(ctx *sql.Context, all []sql.Expression) []sql.Expression {
	var remainder []sql.Expression

	// Careful: index expressions are always normalized (contain actual table names), whereas filter expressions can
	// contain aliases for both expressions and table names. We want to normalize all expressions for comparison, but
	// return the original expressions.
	normalized := normalizeExpressions(ctx, fs.tableAliases, all...)

	for i, e := range normalized {
		var found bool

		cmpStr := e.String()
		comparable, ok := e.(expression.Comparer)
		if ok {
			left, right := comparable.Left(), comparable.Right()
			if _, ok := left.(*expression.GetField); ok {
				cmpStr = left.String()
			} else {
				cmpStr = right.String()
			}
		}

		for _, s := range fs.handledIndexFilters {
			if cmpStr == s {
				found = true
				break
			}
		}

		if !found {
			remainder = append(remainder, all[i])
		}
	}

	return remainder
}
