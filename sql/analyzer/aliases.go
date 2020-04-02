package analyzer

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

type ExprAliases map[string]sql.Expression
type TableAliases map[string]*plan.ResolvedTable

// getTableAliases returns a map of all aliased resolved tables in the node, keyed by their alias name
func getTableAliases(n sql.Node) TableAliases {
	aliases := make(TableAliases)
	var aliasFn func(node sql.Node) bool
	aliasFn = func(node sql.Node) bool {
		if node == nil {
			return false
		}

		if at, ok := node.(*plan.TableAlias); ok {
			switch t := at.Child.(type) {
			case *plan.ResolvedTable:
				aliases[at.Name()] = t
			case *plan.UnresolvedTable:
				panic("Table not resolved")
			default:
				panic(fmt.Sprintf("Unexpected child type of TableAlias: %T", at.Child))
			}
			return false
		}

		for _, child := range node.Children() {
			plan.Inspect(child, aliasFn)
		}

		return true
	}

	plan.Inspect(n, aliasFn)
	return aliases
}

// getExpressionAliases returns a map of all expressions aliased in the SELECT clause, keyed by their alias name
func getExpressionAliases(node sql.Node) ExprAliases {
	aliases := make(ExprAliases)
	var findAliasExpressionsFn func(node sql.Node) bool
	findAliasExpressionsFn = func(n sql.Node) bool {
		if n == nil {
			return true
		}

		if prj, ok := n.(*plan.Project); ok {
			for _, ex := range prj.Expressions() {
				if alias, ok := ex.(*expression.Alias); ok {
					if _, ok := aliases[alias.Name()]; !ok {
						aliases[alias.Name()] = alias.Child
					}
				}
			}
		} else {
			for _, ch := range n.Children() {
				plan.Inspect(ch, findAliasExpressionsFn)
			}
		}

		return true
	}

	plan.Inspect(node, findAliasExpressionsFn)
	return aliases
}

// normalizeExpressions returns the expressions given after normalizing them to replace table and expression aliases
// with their underlying names. This is necessary to match such expressions against those declared by implementors of
// various interfaces that declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpressions(exprAliases ExprAliases, tableAliases TableAliases, expr ...sql.Expression) []sql.Expression {
	expressions := make([]sql.Expression, len(expr))

	for i, e := range expr {
		expressions[i] = normalizeExpression(exprAliases, tableAliases, e)
	}

	return expressions
}

// normalizeExpression returns the expressions given after normalizing them to replace table and expression aliases
// with their underlying names. This is necessary to match such expressions against those declared by implementors of
// various interfaces that declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpression(exprAliases ExprAliases, tableAliases TableAliases, e sql.Expression) sql.Expression {
	uex := e
	name := e.String()
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	}

	if exprAliases != nil && len(exprAliases) > 0 {
		if alias, ok := exprAliases[name]; ok {
			uex = alias
		}
	}

	return uex
}