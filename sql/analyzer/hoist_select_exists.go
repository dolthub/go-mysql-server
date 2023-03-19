// Copyright 2022 Dolthub, Inc.
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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/plan"
	"github.com/gabereiser/go-mysql-server/sql/transform"
)

type aliasDisambiguator struct {
	n                   sql.Node
	scope               *Scope
	aliases             TableAliases
	disambiguationIndex int
}

func (ad *aliasDisambiguator) GetAliases() (TableAliases, error) {
	if ad.aliases == nil {
		aliases, err := getTableAliases(ad.n, ad.scope)
		if err != nil {
			return nil, err
		}
		ad.aliases = aliases
	}
	return ad.aliases, nil
}

func (ad *aliasDisambiguator) Disambiguate(alias string) (string, error) {
	nodeAliases, err := ad.GetAliases()
	if err != nil {
		return "", err
	}

	// all renamed aliases will be of the form <alias>_<disambiguationIndex++>
	for {
		ad.disambiguationIndex++
		aliasName := fmt.Sprintf("%s_%d", alias, ad.disambiguationIndex)
		if _, ok := nodeAliases[aliasName]; !ok {
			return aliasName, nil
		}
	}
}

func newAliasDisambiguator(n sql.Node, scope *Scope) *aliasDisambiguator {
	return &aliasDisambiguator{n: n, scope: scope}
}

// hoistSelectExists merges a WHERE EXISTS subquery scope with its outer
// scope when the subquery filters on columns from the outer scope.
//
// For example:
// select * from a where exists (select 1 from b where a.x = b.x)
// =>
// select * from a semi join b on a.x = b.x
func hoistSelectExists(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	sel RuleSelector,
) (sql.Node, transform.TreeIdentity, error) {
	aliasDisambig := newAliasDisambiguator(n, scope)

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}
		return hoistExistSubqueries(scope, a, f, len(f.Schema())+len(scope.Schema()), aliasDisambig)
	})
}

// simplifyPartialJoinParents discards nodes that will not affect an existence check.
func simplifyPartialJoinParents(n sql.Node) sql.Node {
	ret := n
	for {
		switch n := ret.(type) {
		case *plan.Project, *plan.GroupBy, *plan.Limit, *plan.Sort, *plan.Distinct, *plan.TopN:
			ret = n.Children()[0]
		case *plan.Filter:
			panic("unhandled filter")
		default:
			return ret
		}
	}
}

// hoistExistSubqueries scans a filter for [NOT] WHERE EXISTS, and then attempts to
// extract the subquery, correlated filters, a modified outer scope (net subquery and filters),
// and the new target joinType
func hoistExistSubqueries(scope *Scope, a *Analyzer, filter *plan.Filter, scopeLen int, aliasDisambig *aliasDisambiguator) (sql.Node, transform.TreeIdentity, error) {

	ret := filter.Child
	var retFilters []sql.Expression
	same := transform.SameTree
	for _, f := range splitConjunction(filter.Expression) {
		var joinType plan.JoinType
		var s *hoistSubquery
		var err error
		switch e := f.(type) {
		case *plan.ExistsSubquery:
			joinType = plan.JoinTypeSemi
			s, err = decorrelateOuterCols(e.Query, scopeLen, aliasDisambig)
			if err != nil {
				return nil, transform.SameTree, err
			}

		case *expression.Not:
			if esq, ok := e.Child.(*plan.ExistsSubquery); ok {
				joinType = plan.JoinTypeAnti
				s, err = decorrelateOuterCols(esq.Query, scopeLen, aliasDisambig)
				if err != nil {
					return nil, transform.SameTree, err
				}
			}
		default:
		}

		if s == nil {
			retFilters = append(retFilters, f)
			continue
		}

		// if we reached here, |s| contains the state we need to
		// decorrelate the subquery expression into a new node
		outerFilters, _, err := FixFieldIndexesOnExpressions(scope, a, append(ret.Schema(), s.inner.Schema()...), s.outerFilters...)
		if err != nil {
			return filter, transform.SameTree, err
		}

		retFilters = append(retFilters, s.innerFilters...)

		var comment string
		if c, ok := ret.(sql.CommentedNode); ok {
			comment = c.Comment()
		}

		switch joinType {
		case plan.JoinTypeAnti:
			ret = plan.NewAntiJoin(ret, s.inner, expression.JoinAnd(outerFilters...)).WithComment(comment)
		case plan.JoinTypeSemi:
			ret = plan.NewSemiJoin(ret, s.inner, expression.JoinAnd(outerFilters...)).WithComment(comment)
		default:
			panic("expected JoinTypeSemi or JoinTypeAnti")
		}
		same = transform.NewTree

	}

	if same {
		return filter, transform.SameTree, nil
	}
	if len(retFilters) > 0 {
		ret = plan.NewFilter(expression.JoinAnd(retFilters...), ret)
	}
	log.Debug(sql.DebugString(ret))
	return ret, transform.NewTree, nil
}

type hoistSubquery struct {
	inner        sql.Node
	innerFilters []sql.Expression
	outerFilters []sql.Expression
}

type fakeNameable struct {
	name string
}

var _ sql.Nameable = (*fakeNameable)(nil)

func (f fakeNameable) Name() string { return f.name }

// decorrelateOuterCols returns an optionally modified subquery and extracted filters referencing an outer scope.
// If the subquery has aliases that conflict withoutside aliases, the internal aliases will be renamed to avoid
// name collisions.
func decorrelateOuterCols(e *plan.Subquery, scopeLen int, aliasDisambig *aliasDisambiguator) (*hoistSubquery, error) {
	var outerFilters []sql.Expression
	var innerFilters []sql.Expression
	n, same, _ := transform.Node(e.Query, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}
		filters := splitConjunction(f.Expression)
		for _, f := range filters {
			var outerRef bool
			transform.InspectExpr(f, func(e sql.Expression) bool {
				gf, ok := e.(*expression.GetField)
				if ok && gf.Index() < scopeLen {
					// has to be from out of scope
					outerRef = true
					return true
				}
				return false
			})
			if outerRef {
				outerFilters = append(outerFilters, f)
			} else {
				innerFilters = append(innerFilters, f)
			}
		}
		return f.Child, transform.NewTree, nil
	})

	if same || len(outerFilters) == 0 {
		return nil, nil
	}

	nodeAliases, err := getTableAliases(n, nil)
	if err != nil {
		return nil, err
	}

	outsideAliases, err := aliasDisambig.GetAliases()
	if err != nil {
		return nil, err
	}
	conflicts, nonConflicted := outsideAliases.findConflicts(nodeAliases)
	for _, goodAlias := range nonConflicted {
		target, ok := nodeAliases[goodAlias]
		if !ok {
			return nil, fmt.Errorf("node alias %s is not in nodeAliases", goodAlias)
		}
		nameable := fakeNameable{name: goodAlias}
		err = outsideAliases.add(nameable, target)
		if err != nil {
			return nil, err
		}
	}

	if len(conflicts) > 0 {
		for _, conflict := range conflicts {

			// conflict, need to rename
			newAlias, err := aliasDisambig.Disambiguate(conflict)
			if err != nil {
				return nil, err
			}
			var tree transform.TreeIdentity
			n, tree, err = renameAliases(n, conflict, newAlias)
			if err != nil {
				return nil, err
			}
			if tree == transform.SameTree {
				return nil, fmt.Errorf("tree is unchanged after attempted rename")
			}

			// rename the aliases in the expressions
			innerFilters, err = renameAliasesInExpressions(innerFilters, conflict, newAlias)
			if err != nil {
				return nil, err
			}
			outerFilters, err = renameAliasesInExpressions(outerFilters, conflict, newAlias)
			if err != nil {
				return nil, err
			}

			// alias was renamed, need to get the renamed target before adding to the outside aliases collection
			nodeAliases, err = getTableAliases(n, nil)
			if err != nil {
				return nil, err
			}

			// retrieve the new target
			target, ok := nodeAliases[newAlias]
			if !ok {
				return nil, fmt.Errorf("node alias %s is not in nodeAliases", newAlias)
			}

			// add the new target to the outside aliases collection
			nameable := fakeNameable{name: newAlias}
			err = outsideAliases.add(nameable, target)
			if err != nil {
				return nil, err
			}
		}
	}

	return &hoistSubquery{
		inner:        simplifyPartialJoinParents(n),
		innerFilters: innerFilters,
		outerFilters: outerFilters,
	}, nil
}
