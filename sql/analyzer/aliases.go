package analyzer

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"strings"
)

type ExprAliases map[string]sql.Expression
type TableAliases map[string]sql.Nameable

func (ta TableAliases) add(alias *plan.TableAlias, target sql.Nameable) error {
	lowerName := strings.ToLower(alias.Name())
	if _, ok := ta[lowerName]; ok {
		return sql.ErrDuplicateAliasOrTable.New(target.Name())
	}

	ta[lowerName] = target
	return nil
}

// getTableAliases returns a map of all aliases of resolved tables / subqueries in the node, keyed by their alias name
func getTableAliases(n sql.Node) (TableAliases, error) {
	aliases := make(TableAliases)
	var aliasFn func(node sql.Node) bool
	var analysisErr error

	aliasFn = func(node sql.Node) bool {
		if node == nil {
			return false
		}

		if at, ok := node.(*plan.TableAlias); ok {
			switch t := at.Child.(type) {
			case *plan.ResolvedTable, *plan.SubqueryAlias:
				analysisErr = aliases.add(at, t.(sql.Nameable))
				if analysisErr != nil {
					return false
				}
			case *plan.DecoratedNode:
				rt := getResolvedTable(at.Child)
				aliases.add(at, rt)
			case *plan.UnresolvedTable:
				panic("Table not resolved")
			default:
				panic(fmt.Sprintf("Unexpected child type of TableAlias: %T", at.Child))
			}
			return false
		}

		return true
	}

	plan.Inspect(n, aliasFn)
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
		return rt.Name()
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
				return field.WithTable(rt.Name()), nil
			}
		}

		return e, nil
	})

	return normalized
}