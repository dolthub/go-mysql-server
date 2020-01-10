package analyzer

import (
	"reflect"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

type filters map[string][]sql.Expression

func (f filters) merge(f2 filters) {
	for k, exprs := range f2 {
		f[k] = append(f[k], exprs...)
	}
}

func exprToTableFilters(expr sql.Expression) filters {
	filtersByTable := make(filters)
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

func getUnhandledFilters(all, handled []sql.Expression) []sql.Expression {
	var unhandledFilters []sql.Expression

	for _, f := range all {
		var isHandled bool
		for _, hf := range handled {
			if reflect.DeepEqual(f, hf) {
				isHandled = true
				break
			}
		}

		if !isHandled {
			unhandledFilters = append(unhandledFilters, f)
		}
	}

	return unhandledFilters
}
