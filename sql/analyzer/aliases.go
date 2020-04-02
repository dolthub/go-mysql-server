package analyzer

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

// getTableAliases returns a map of all aliased resolved tables in the node, keyed by their alias name
func getTableAliases(n sql.Node) map[string]*plan.ResolvedTable {
	aliases := make(map[string]*plan.ResolvedTable)
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
func getExpressionAliases(node sql.Node) map[string]sql.Expression {
	aliases := make(map[string]sql.Expression)
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