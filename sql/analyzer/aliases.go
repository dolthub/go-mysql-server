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
)

type TableAliases map[string]sql.Nameable

// add adds the given table alias referring to the node given. Adding a case insensitive alias that already exists
// returns an error.
func (ta TableAliases) add(alias sql.Nameable, target sql.Nameable) error {
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

// getTableAliases returns a map of all aliases of resolved tables / subqueries in the node, keyed by their alias name.
// Unaliased tables are returned keyed by their original lower-cased name.
func getTableAliases(n sql.Node, scope *Scope) (TableAliases, error) {
	var passAliases TableAliases
	var aliasFn func(node sql.Node) bool
	var analysisErr error
	var recScope *Scope
	if scope != nil {
		recScope = recScope.withMemos(scope.memos)
	}

	aliasFn = func(node sql.Node) bool {
		if node == nil {
			return false
		}

		if opaque, ok := node.(sql.OpaqueNode); ok && opaque.Opaque() {
			return false
		}

		if at, ok := node.(*plan.TableAlias); ok {
			switch t := at.Child.(type) {
			case *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.TransformedNamedNode:
				analysisErr = passAliases.add(at, t.(NameableNode))
			case *plan.DecoratedNode:
				rt := getResolvedTable(at.Child)
				analysisErr = passAliases.add(at, rt)
			case *plan.IndexedTableAccess:
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
		case *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.TransformedNamedNode:
			analysisErr = passAliases.add(node.(sql.Nameable), node.(sql.Nameable))
			return false
		case *plan.DecoratedNode:
			rt := getResolvedTable(node.Child)
			analysisErr = passAliases.add(rt, rt)
			return false
		case *plan.IndexedTableAccess:
			rt := getResolvedTable(node.ResolvedTable)
			analysisErr = passAliases.add(rt, node)
			return false
		case *plan.UnresolvedTable:
			panic("Table not resolved")
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
		plan.Inspect(scopeNode, aliasFn)
		if analysisErr != nil {
			return nil, analysisErr
		}
		recScope = recScope.newScope(scopeNode)
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

// aliasesDefinedInNode returns the expression aliases that are defined in the node given
func aliasesDefinedInNode(n sql.Node) []string {
	var exprs []sql.Expression
	switch n := n.(type) {
	case *plan.GroupBy:
		exprs = n.SelectedExprs
	case sql.Expressioner:
		exprs = n.Expressions()
	}

	var aliases []string
	for _, e := range exprs {
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
func normalizeExpressions(ctx *sql.Context, tableAliases TableAliases, expr ...sql.Expression) []sql.Expression {
	expressions := make([]sql.Expression, len(expr))

	for i, e := range expr {
		expressions[i] = normalizeExpression(ctx, tableAliases, e)
	}

	return expressions
}

// normalizeExpression returns the expression given after normalizing it to replace table aliases with their underlying
// names. This is necessary to match such expressions against those declared by implementors of various interfaces that
// declare expressions to handle, such as Index.Expressions(), FilteredTable, etc.
func normalizeExpression(ctx *sql.Context, tableAliases TableAliases, e sql.Expression) sql.Expression {
	// If the query has table aliases, use them to replace any table aliases in column expressions
	normalized, _ := expression.TransformUp(ctx, e, func(e sql.Expression) (sql.Expression, error) {
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
