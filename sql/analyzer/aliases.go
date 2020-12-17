package analyzer

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type ExprAliases map[string]sql.Expression
type TableAliases map[string]sql.Node

// add adds the given table alias referring to the node given. Adding a case insensitive alias that already exists
// returns an error.
func (ta TableAliases) add(alias sql.Nameable, target sql.Node) error {
	lowerName := strings.ToLower(alias.Name())
	if _, ok := ta[lowerName]; ok {
		return sql.ErrDuplicateAliasOrTable.New(alias.Name())
	}

	ta[lowerName] = target
	return nil
}

// putAll adds all aliases in the given aliases to the receiver. Silently overwrites existing entries.
func (ta TableAliases) putAll(other TableAliases) {
	for alias, target := range other {
		ta[alias] = target
	}
}

// getTableAliases returns a map of all aliases of resolved tables / subqueries in the node, keyed by their alias name
func getTableAliases(n sql.Node, scope *Scope) (TableAliases, error) {
	var passAliases TableAliases
	var aliasFn func(node sql.Node) bool
	var analysisErr error

	aliasFn = func(node sql.Node) bool {
		if node == nil {
			return false
		}


		if at, ok := node.(*plan.TableAlias); ok {
			switch t := at.Child.(type) {
			case *plan.ResolvedTable, *plan.SubqueryAlias:
				analysisErr = passAliases.add(at, t)
				if analysisErr != nil {
					return false
				}
			case *plan.DecoratedNode:
				rt := getResolvedTable(at.Child)
				passAliases.add(at, rt)
			case *plan.IndexedTableAccess:
				rt := getResolvedTable(at.Child)
				passAliases.add(at, rt)
			case *plan.UnresolvedTable:
				panic("Table not resolved")
			default:
				panic(fmt.Sprintf("Unexpected child type of TableAlias: %T", at.Child))
			}
			return false
		}

		switch node := node.(type) {
		case *plan.ResolvedTable, *plan.SubqueryAlias:
			analysisErr = passAliases.add(node.(sql.Nameable), node)
			if analysisErr != nil {
				return false
			}
		case *plan.DecoratedNode:
			rt := getResolvedTable(node.Child)
			passAliases.add(rt, node)
		case *plan.IndexedTableAccess:
			rt := getResolvedTable(node.ResolvedTable)
			passAliases.add(rt, node)
		case *plan.UnresolvedTable:
			panic("Table not resolved")
		}

		return true
	}

	// Inspect all of the scopes, outer to inner. Within a single scope, a name conflict is an error. But an inner scope
	// can overwrite a name in an outer scope, and it's not an error.
	aliases := make(TableAliases)
	for _, scopeNode := range scope.OuterToInner() {
		passAliases = make(TableAliases)
		plan.Inspect(scopeNode, aliasFn)
		if analysisErr != nil {
			return nil, analysisErr
		}
		aliases.putAll(passAliases)
	}

	passAliases = make(TableAliases)
	plan.Inspect(n, aliasFn)
	if analysisErr != nil {
		return nil, analysisErr
	}
	aliases.putAll(passAliases)

	return aliases, analysisErr
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

// normalizeTableName returns the underlying table for the aliased table name given, if it's an alias.
func normalizeTableName(tableAliases TableAliases, tableName string) string {
	if rt, ok := tableAliases[tableName]; ok {
		return nodeName(rt)
	}

	return tableName
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

// normalizeExpression returns the expression given after normalizing it to replace table and expression aliases
// with their underlying names. This is necessary to match such expressions against those declared by implementors of
// various interfaces that declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpression(exprAliases ExprAliases, tableAliases TableAliases, e sql.Expression) sql.Expression {
	name := e.String()
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	}

	// If the query has any aliases that match the expression given, return them
	if exprAliases != nil && len(exprAliases) > 0 {
		if alias, ok := exprAliases[name]; ok {
			return alias
		}
	}

	// If the query has table aliases, use them to replace any table aliases in column expressions
	normalized, _ := expression.TransformUp(e, func(e sql.Expression) (sql.Expression, error) {
		if field, ok := e.(*expression.GetField); ok {
			table := field.Table()
			if rt, ok := tableAliases[table]; ok {
				return field.WithTable(nodeName(rt)), nil
			}
		}

		return e, nil
	})

	return normalized
}

func nodeName(node sql.Node) string {
	if nameable, ok := node.(sql.Nameable); ok {
		return nameable.Name()
	}
	return node.String()
}
