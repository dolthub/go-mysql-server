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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type TableAliases map[string]sql.Nameable

// add adds the given table alias referring to the node given. Adding a case insensitive alias that already exists
// returns an error.
func (ta TableAliases) add(alias sql.Nameable, target sql.Nameable) error {
	lowerName := strings.ToLower(alias.Name())
	if _, ok := ta[lowerName]; ok && lowerName != plan.DualTableName {
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

// findConflicts returns a list of aliases that are in both sets of aliases, and a list of aliases that are just in
// the current set of aliases.
func (ta TableAliases) findConflicts(other TableAliases) (conflicts []string, nonConflicted []string) {
	conflicts = []string{}
	nonConflicted = []string{}

	for alias := range other {
		if _, ok := ta[alias]; ok {
			conflicts = append(conflicts, alias)
		} else {
			nonConflicted = append(nonConflicted, alias)
		}
	}

	return
}

// getTableAliases returns a map of all aliases of resolved tables / subqueries in the node, keyed by their alias name.
// Unaliased tables are returned keyed by their original lower-cased name.
func getTableAliases(n sql.Node, scope *Scope) (TableAliases, error) {
	var passAliases TableAliases
	var aliasFn func(node sql.Node) bool
	var analysisErr error
	var recScope *Scope
	if !scope.IsEmpty() {
		recScope = recScope.withMemos(scope.memos)
	}

	aliasFn = func(node sql.Node) bool {
		if node == nil {
			return false
		}

		if at, ok := node.(*plan.TableAlias); ok {
			switch t := at.Child.(type) {
			case *plan.RecursiveCte:
			case sql.NameableNode:
				analysisErr = passAliases.add(at, t)
			case *plan.UnresolvedTable:
				panic("Table not resolved")
			default:
				panic(fmt.Sprintf("Unexpected child type of TableAlias: %T", at.Child))
			}
			return false
		}

		switch node := node.(type) {
		case *plan.CreateTrigger:
			// trigger bodies are evaluated separately
			rt := getResolvedTable(node.Table)
			analysisErr = passAliases.add(rt, rt)
			return false
		case *plan.Procedure:
			return false
		case *plan.Block:
			// blocks should not be parsed as a whole, just their statements individually
			for _, child := range node.Children() {
				_, analysisErr = getTableAliases(child, recScope)
				if analysisErr != nil {
					break
				}
			}
			return false
		case *plan.InsertInto:
			rt := getResolvedTable(node.Destination)
			analysisErr = passAliases.add(rt, rt)
			return false
		case *plan.IndexedTableAccess:
			rt := getResolvedTable(node.ResolvedTable)
			analysisErr = passAliases.add(rt, node)
			return false
		case sql.Nameable:
			analysisErr = passAliases.add(node, node)
			return false
		case *plan.UnresolvedTable:
			panic("Table not resolved")
		default:
		}

		if opaque, ok := node.(sql.OpaqueNode); ok && opaque.Opaque() {
			return false
		}

		return true
	}
	if analysisErr != nil {
		return nil, analysisErr
	}

	// Inspect all of the scopes, outer to inner. Within a single scope, a name conflict is an error. But an inner scope
	// can overwrite a name in an outer scope, and it's not an error.
	aliases := make(TableAliases)
	for _, scopeNode := range scope.OuterToInner() {
		passAliases = make(TableAliases)
		transform.Inspect(scopeNode, aliasFn)
		if analysisErr != nil {
			return nil, analysisErr
		}
		recScope = recScope.newScope(scopeNode)
		aliases.putAll(passAliases)
	}

	passAliases = make(TableAliases)
	transform.Inspect(n, aliasFn)
	if analysisErr != nil {
		return nil, analysisErr
	}
	aliases.putAll(passAliases)

	return aliases, analysisErr
}

// aliasedExpressionsInNode returns a map of the aliased expressions defined in the first Projector node found (starting
// the search from the specified node), mapped from the expression string to the alias name. Returned
// map keys are normalized to lower case.
func aliasedExpressionsInNode(n sql.Node) map[string]string {
	projector := findFirstProjectorNode(n)
	if projector == nil {
		return nil
	}
	aliasesFromExpressionToName := make(map[string]string)
	for _, e := range projector.ProjectedExprs() {
		alias, ok := e.(*expression.Alias)
		if ok {
			aliasesFromExpressionToName[strings.ToLower(alias.Child.String())] = alias.Name()
		}
	}

	return aliasesFromExpressionToName
}

// aliasesDefinedInNode returns the expression aliases that are defined in the first Projector node found, starting
// the search from the specified node. All returned alias names are normalized to lower case.
func aliasesDefinedInNode(n sql.Node) []string {
	projector := findFirstProjectorNode(n)
	if projector == nil {
		return nil
	}

	var aliases []string
	for _, e := range projector.ProjectedExprs() {
		alias, ok := e.(*expression.Alias)
		if ok {
			aliases = append(aliases, strings.ToLower(alias.Name()))
		}
	}

	return aliases
}

// normalizeExpressions returns the expressions given after normalizing them to replace table and expression aliases
// with their underlying names. This is necessary to match such expressions against those declared by implementors of
// various interfaces that declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpressions(tableAliases TableAliases, expr ...sql.Expression) []sql.Expression {
	expressions := make([]sql.Expression, len(expr))

	for i, e := range expr {
		expressions[i] = normalizeExpression(tableAliases, e)
	}

	return expressions
}

// normalizeExpression returns the expression given after normalizing it to replace table aliases with their underlying
// names. This is necessary to match such expressions against those declared by implementors of various interfaces that
// declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpression(tableAliases TableAliases, e sql.Expression) sql.Expression {
	// If the query has table aliases, use them to replace any table aliases in column expressions
	normalized, _, _ := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if field, ok := e.(*expression.GetField); ok {
			table := strings.ToLower(field.Table())
			if rt, ok := tableAliases[table]; ok {
				return field.WithTable(rt.Name()), transform.NewTree, nil
			}
		}

		return e, transform.SameTree, nil
	})

	return normalized
}

// renameAliasesInExpressions returns expressions where any table references are renamed to the new table name.
func renameAliasesInExpressions(expressions []sql.Expression, oldNameLower string, newName string) ([]sql.Expression, error) {
	for i, e := range expressions {
		newExpression, same, err := renameAliasesInExp(e, oldNameLower, newName)
		if err != nil {
			return nil, err
		}
		if !same {
			expressions[i] = newExpression
		}
	}
	return expressions, nil
}

// renameAliasesInExp returns an expression where any table references are renamed to the new table name.
func renameAliasesInExp(exp sql.Expression, oldNameLower string, newName string) (sql.Expression, transform.TreeIdentity, error) {
	return transform.Expr(exp, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			if strings.EqualFold(e.Table(), oldNameLower) {
				gf := e.WithTable(newName)
				return gf, transform.NewTree, nil
			}
		case *expression.UnresolvedColumn:
			if strings.EqualFold(e.Table(), oldNameLower) {
				return expression.NewUnresolvedQualifiedColumn(newName, e.Name()), transform.NewTree, nil
			}
		case *plan.Subquery:
			newSubquery, tree, err := renameAliases(e.Query, oldNameLower, newName)
			if err != nil {
				return nil, tree, err
			}
			if tree == transform.NewTree {
				e.WithQuery(newSubquery)
			}
			return e, tree, nil
		}
		return e, transform.SameTree, nil
	})
}

// renameAliasesInExp returns a node where any table references are renamed to the new table name.
func renameAliases(node sql.Node, oldNameLower string, newName string) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		newNode := node
		allSame := transform.SameTree

		// update node
		if nameable, ok := node.(sql.Nameable); ok && strings.EqualFold(nameable.Name(), oldNameLower) {
			allSame = transform.NewTree
			if renameable, ok := node.(sql.RenameableNode); ok {
				newNode = renameable.WithName(newName)
			} else {
				newNode = plan.NewTableAlias(newName, node)
			}
		}

		// update expressions
		newNode, same, err := transform.NodeExprs(newNode, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			return renameAliasesInExp(e, oldNameLower, newName)
		})
		if err != nil {
			return nil, transform.SameTree, err
		}

		allSame = allSame && same
		if allSame {
			return node, transform.SameTree, nil
		} else {
			return newNode, transform.NewTree, nil
		}
	})
}

func disambiguateTableFunctions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var i int
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *expression.UnresolvedTableFunction:
			i++
			return plan.NewTableAlias(fmt.Sprintf("%s_%d", n.Name(), i), n), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}
