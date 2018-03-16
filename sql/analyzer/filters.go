package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

type filters map[string][]sql.Expression

func (f filters) merge(f2 filters) {
	for k, exprs := range f2 {
		f[k] = append(f[k], exprs...)
	}
}

func exprToTableFilters(expr sql.Expression) filters {
	filtersByTable := make(filters)
	for _, expr := range splitExpression(expr) {
		var tables []string
		_, _ = expr.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			f, ok := e.(*expression.GetField)
			if ok {
				tables = append(tables, f.Table())
			}

			return e, nil
		})

		if len(tables) == 1 {
			filtersByTable[tables[0]] = append(filtersByTable[tables[0]], expr)
		}
	}

	return filtersByTable
}

func splitExpression(expr sql.Expression) []sql.Expression {
	and, ok := expr.(*expression.And)
	if !ok {
		return []sql.Expression{expr}
	}

	return append(
		splitExpression(and.Left),
		splitExpression(and.Right)...,
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
