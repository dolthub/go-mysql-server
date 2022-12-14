// Copyright 2021 Dolthub, Inc.
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

type aliasScope struct {
	p       *aliasScope
	aliases map[string]string
}

func newScope() *aliasScope {
	return &aliasScope{
		aliases: make(map[string]string),
	}
}
func (s *aliasScope) add(a, t string) error {
	lowerName := strings.ToLower(a)
	if _, ok := s.aliases[lowerName]; ok && lowerName != plan.DualTableName {
		return sql.ErrDuplicateAliasOrTable.New(a)
	}
	s.aliases[lowerName] = t
	return nil
}

func (s *aliasScope) new() *aliasScope {
	ret := newScope()
	ret.p = s
	for k, v := range s.aliases {
		ret.aliases[k] = v
	}
	return ret
}

func (s *aliasScope) get(n string) string {
	if t, ok := s.aliases[n]; ok {
		return t
	}
	if s.p == nil {
		return ""
	}
	return s.p.get(n)
}

// applyIndexesForSubqueryComparisons converts a `Filter(id = (SELECT ...),
// Child)` or a `Filter(id in (SELECT ...), Child)` to be iterated lookups on
// the Child instead. This analysis phase is currently very concrete. It only
// applies when:
// 1. There is a single `=` or `IN` expression in the Filter.
// 2. The Subquery is on the right hand side of the expression.
// 3. The left hand side is a GetField expression against the Child.
// 4. The Child is a *plan.ResolvedTable.
// 5. The referenced field in the Child is indexed.
func applyIndexesForSubqueryComparisons(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var recurse func(n sql.Node, inScope *aliasScope) (sql.Node, transform.TreeIdentity, error)
	recurse = func(n sql.Node, inScope *aliasScope) (sql.Node, transform.TreeIdentity, error) {
		var alias, target string
		var err error
		switch n := n.(type) {
		case *plan.TableAlias:
			alias = n.Name()
			switch c := n.Child.(type) {
			case *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.TransformedNamedNode, *plan.RecursiveTable, *plan.DeferredAsOfTable:
				target = c.(sql.Nameable).Name()
			case *plan.IndexedTableAccess:
				target = c.Name()
			case *plan.RecursiveCte:
			case *plan.UnresolvedTable:
				panic("Table not resolved")
			default:
				panic(fmt.Sprintf("Unexpected child type of TableAlias: %T", c))
			}
		default:
		}

		if alias != "" {
			err = inScope.add(alias, target)
			if err != nil {
				return n, transform.SameTree, err
			}
		}

		_, isOp := n.(sql.OpaqueNode)
		outScope := inScope
		if isOp {
			outScope = inScope.new()
		}

		children := n.Children()
		var newChildren []sql.Node
		for i, c := range children {
			child, same, _ := recurse(c, outScope)
			if !same {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
		}

		ret := n
		same := transform.SameTree
		if len(newChildren) > 0 {
			ret, _ = ret.WithChildren(newChildren...)
			same = transform.NewTree
		}

		var replacement sql.Node
		switch n := ret.(type) {
		case *plan.Filter:
			switch e := n.Expression.(type) {
			case *expression.Equals:
				if rt, ok := n.Child.(*plan.ResolvedTable); ok {
					replacement = getIndexedInSubqueryFilter(ctx, e.Left(), e.Right(), rt, true, inScope)
				}
			case *plan.InSubquery:
				if rt, ok := n.Child.(*plan.ResolvedTable); ok {
					replacement = getIndexedInSubqueryFilter(ctx, e.Left, e.Right, rt, false, inScope)
				}
			default:
			}
		default:
		}
		if replacement != nil {
			ret = replacement
			same = transform.NewTree
		}

		if same {
			return n, transform.SameTree, nil
		}
		return ret, transform.NewTree, nil
	}
	return recurse(n, newScope())
}

func getIndexedInSubqueryFilter(
	ctx *sql.Context,
	left, right sql.Expression,
	rt *plan.ResolvedTable,
	equals bool,
	scope *aliasScope,
) sql.Node {
	gf, isGetField := left.(*expression.GetField)
	subq, isSubquery := right.(*plan.Subquery)
	if !isGetField || !isSubquery || !subq.CanCacheResults() {
		return nil
	}
	indexes, err := newIndexAnalyzerForNode(ctx, rt)
	if err != nil {
		return nil
	}
	defer indexes.releaseUsedIndexes()
	if rt := scope.get(strings.ToLower(gf.Table())); rt != "" {
		gf = gf.WithTable(rt)
	}
	idx := indexes.MatchingIndex(ctx, ctx.GetCurrentDatabase(), rt.Name(), gf)
	if idx == nil {
		return nil
	}
	keyExpr := gf.WithIndex(0)
	// We currently only support *expresssion.Equals and *InSubquery; neither matches null.
	nullmask := []bool{false}
	ita, err := plan.NewIndexedAccessForResolvedTable(rt, plan.NewLookupBuilder(idx, []sql.Expression{keyExpr}, nullmask))
	if err != nil {
		return nil
	}
	if canBuildIndex, err := ita.CanBuildIndex(ctx); err != nil || !canBuildIndex {
		return nil
	}
	return plan.NewIndexedInSubqueryFilter(subq, ita, len(rt.Schema()), gf, equals)
}
