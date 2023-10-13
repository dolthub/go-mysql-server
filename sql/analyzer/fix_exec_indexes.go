// Copyright 2023 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql/planbuilder"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// assignExecIndexes walks a query plan in-order and rewrites GetFields to use
// execution appropriate indexing.
func assignExecIndexes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	s := &idxScope{}
	if !scope.IsEmpty() {
		// triggers
		s.addSchema(scope.Schema())
		s = s.push()
	}
	ret, _, err := assignIndexesHelper(n, s)
	if err != nil {
		return n, transform.SameTree, err
	}
	return ret, transform.NewTree, nil
}

func assignIndexesHelper(n sql.Node, inScope *idxScope) (sql.Node, *idxScope, error) {
	// copy scope, otherwise parent/lateral edits have non-local effects
	outScope := inScope.copy()
	err := outScope.visitChildren(n)
	if err != nil {
		return nil, nil, err
	}
	err = outScope.visitSelf(n)
	if err != nil {
		return nil, nil, err
	}
	ret, err := outScope.finalizeSelf(n)
	return ret, outScope, err
}

// idxScope accumulates the information needed to rewrite node column
// references for execution, including parent/child scopes, lateral
// scopes (if in the middle of a join tree), and child nodes and expressions.
// Collecting this info in one place makes it easier to compartmentalize
// finalization into an after phase.
type idxScope struct {
	parentScopes  []*idxScope
	lateralScopes []*idxScope
	childScopes   []*idxScope
	columns       []string
	children      []sql.Node
	expressions   []sql.Expression
	checks        sql.CheckConstraints
}

func (s *idxScope) addSchema(sch sql.Schema) {
	for _, c := range sch {
		if c.Source == "" {
			s.columns = append(s.columns, c.Name)
		} else {
			s.columns = append(s.columns, fmt.Sprintf("%s.%s", c.Source, c.Name))
		}
	}
}

func (s *idxScope) addScope(other *idxScope) {
	s.columns = append(s.columns, other.columns...)
}

func (s *idxScope) addLateral(other *idxScope) {
	s.lateralScopes = append(s.lateralScopes, other)
}

func (s *idxScope) addParent(other *idxScope) {
	s.parentScopes = append(s.parentScopes, other)
}

// unqualify is a helper function to remove the table prefix from a column, if it's present.
func unqualify(s string) string {
	if strings.Contains(s, ".") {
		return strings.Split(s, ".")[1]
	}
	return s
}

func (s *idxScope) getIdx(n string) (int, bool) {
	// We match the column closet to our current scope. We have already
	// resolved columns, so there will be no in-scope collisions.
	for i := len(s.columns) - 1; i >= 0; i-- {
		if strings.EqualFold(n, s.columns[i]) {
			return i, true
		}
	}
	// This should only apply to column names for set_op, where we have two different tables
	n = unqualify(n)
	for i := len(s.columns) - 1; i >= 0; i-- {
		if strings.EqualFold(n, unqualify(s.columns[i])) {
			return i, true
		}
	}
	return -1, false
}

func (s *idxScope) copy() *idxScope {
	if s == nil {
		return &idxScope{}
	}
	var varsCopy []string
	if len(s.columns) > 0 {
		varsCopy = make([]string, len(s.columns))
		copy(varsCopy, s.columns)
	}
	var lateralCopy []*idxScope
	if len(s.lateralScopes) > 0 {
		lateralCopy = make([]*idxScope, len(s.lateralScopes))
		copy(lateralCopy, s.lateralScopes)
	}
	var parentCopy []*idxScope
	if len(s.parentScopes) > 0 {
		parentCopy = make([]*idxScope, len(s.parentScopes))
		copy(parentCopy, s.parentScopes)
	}
	if len(s.columns) > 0 {
		varsCopy = make([]string, len(s.columns))
		copy(varsCopy, s.columns)
	}
	return &idxScope{
		lateralScopes: lateralCopy,
		parentScopes:  parentCopy,
		columns:       varsCopy,
	}
}

func (s *idxScope) push() *idxScope {
	return &idxScope{
		parentScopes: []*idxScope{s},
	}
}

// visitChildren walks children and gathers schema info for this node
func (s *idxScope) visitChildren(n sql.Node) error {
	switch n := n.(type) {
	case *plan.JoinNode:
		lateralScope := s.copy()
		for _, c := range n.Children() {
			newC, cScope, err := assignIndexesHelper(c, lateralScope)
			if err != nil {
				return err
			}
			// child scope is always a child to the current scope
			s.childScopes = append(s.childScopes, cScope)
			if n.Op.IsLateral() {
				// lateral promotes the scope to parent relative to other join children
				lateralScope.addParent(cScope)
			} else {
				// child scope is lateral scope to join children, hidden by default from
				// most expressions
				lateralScope.addLateral(cScope)
			}
			s.children = append(s.children, newC)
		}
	case *plan.SubqueryAlias:
		sqScope := s.copy()
		if !n.OuterScopeVisibility && !n.IsLateral {
			// Subqueries with no visibility have no parent scopes. Lateral
			// join subquery aliases continue to enjoy full visibility.
			sqScope.parentScopes = sqScope.parentScopes[:0]
			sqScope.lateralScopes = sqScope.lateralScopes[:0]
		}
		newC, cScope, err := assignIndexesHelper(n.Child, sqScope)
		if err != nil {
			return err
		}
		s.childScopes = append(s.childScopes, cScope)
		s.children = append(s.children, newC)
	case *plan.SetOp:
		var keepScope *idxScope
		for i, c := range n.Children() {
			newC, cScope, err := assignIndexesHelper(c, s)
			if err != nil {
				return err
			}
			if i == 0 {
				keepScope = cScope
			}
			s.children = append(s.children, newC)
		}
		// keep only the first union scope to avoid double counting
		s.childScopes = append(s.childScopes, keepScope)
	case *plan.InsertInto:
		newSrc, _, err := assignIndexesHelper(n.Source, s)
		if err != nil {
			return err
		}
		newDst, dScope, err := assignIndexesHelper(n.Destination, s)
		if err != nil {
			return err
		}
		s.children = append(s.children, newSrc)
		s.children = append(s.children, newDst)
		s.childScopes = append(s.childScopes, dScope)
	case *plan.Procedure:
	default:
		for _, c := range n.Children() {
			newC, cScope, err := assignIndexesHelper(c, s)
			if err != nil {
				return err
			}
			s.childScopes = append(s.childScopes, cScope)
			s.children = append(s.children, newC)
		}
	}
	return nil
}

// visitSelf fixes expression indexes for this node. Assumes |s.childScopes|
// is set, any partial |s.lateralScopes| are filled, and the self scope is
// unset.
func (s *idxScope) visitSelf(n sql.Node) error {
	switch n := n.(type) {
	case *plan.JoinNode:
		// join on expressions see everything
		scopes := append(append(s.parentScopes, s.lateralScopes...), s.childScopes...)
		for _, e := range n.Expressions() {
			s.expressions = append(s.expressions, fixExprToScope(e, scopes...))
		}
	case *plan.RangeHeap:
		siblingScope := s.lateralScopes[len(s.lateralScopes)-1]
		// value indexes other side of join
		newValue := fixExprToScope(n.ValueColumnGf, siblingScope)
		// min/are this child
		newMin := fixExprToScope(n.MinColumnGf, s.childScopes...)
		newMax := fixExprToScope(n.MaxColumnGf, s.childScopes...)
		n.MaxColumnGf = newMax
		n.MinColumnGf = newMin
		n.ValueColumnGf = newValue
		n.MaxColumnIndex = newMax.(*expression.GetField).Index()
		n.MinColumnIndex = newMin.(*expression.GetField).Index()
		n.ValueColumnIndex = newValue.(*expression.GetField).Index()
	case *plan.HashLookup:
		// right entry has parent and self visibility, no lateral join scope
		rightScopes := append(s.parentScopes, s.childScopes...)
		s.expressions = append(s.expressions, fixExprToScope(n.RightEntryKey, rightScopes...))
		// left probe is the join context accumulation
		leftScopes := append(s.parentScopes, s.lateralScopes...)
		s.expressions = append(s.expressions, fixExprToScope(n.LeftProbeKey, leftScopes...))
	case *plan.IndexedTableAccess:
		var scope []*idxScope
		switch n.Typ {
		case plan.ItaTypeStatic:
			// self-visibility
			scope = append(s.parentScopes, s.childScopes...)
		case plan.ItaTypeLookup:
			// join siblings
			scope = append(s.parentScopes, s.lateralScopes...)
		}
		for _, e := range n.Expressions() {
			s.expressions = append(s.expressions, fixExprToScope(e, scope...))
		}
	case *plan.ShowVariables:
		if n.Filter != nil {
			selfScope := s.copy()
			selfScope.addSchema(n.Schema())
			scope := append(s.parentScopes, selfScope)
			for _, e := range n.Expressions() {
				s.expressions = append(s.expressions, fixExprToScope(e, scope...))
			}
		}
	case *plan.JSONTable:
		scopes := append(s.parentScopes, s.lateralScopes...)
		for _, e := range n.Expressions() {
			s.expressions = append(s.expressions, fixExprToScope(e, scopes...))
		}
	case *plan.InsertInto:
		rightSchema := make(sql.Schema, len(n.Destination.Schema())*2)
		// schema = [oldrow][newrow]
		for oldRowIdx, c := range n.Destination.Schema() {
			rightSchema[oldRowIdx] = c
			newRowIdx := len(n.Destination.Schema()) + oldRowIdx
			if _, ok := n.Source.(*plan.Values); !ok && len(n.Destination.Schema()) == len(n.Source.Schema()) {
				// find source index that aligns with dest column
				var matched bool
				for j, sourceCol := range n.ColumnNames {
					if strings.EqualFold(c.Name, sourceCol) {
						rightSchema[newRowIdx] = n.Source.Schema()[j]
						matched = true
						break
					}
				}
				if !matched {
					// todo: this is only used for load data. load data errors
					//  without a fallback, and fails to resolve defaults if I
					//  define the columns upfront.
					rightSchema[newRowIdx] = n.Source.Schema()[oldRowIdx]
				}
			} else {
				newC := c.Copy()
				newC.Source = planbuilder.OnDupValuesPrefix
				rightSchema[newRowIdx] = newC
			}
		}
		rightScope := &idxScope{}
		rightScope.addSchema(rightSchema)
		dstScope := s.childScopes[0]

		for _, e := range n.OnDupExprs {
			set, ok := e.(*expression.SetField)
			if !ok {
				return fmt.Errorf("on duplicate update expressions should be *expression.SetField; found %T", e)
			}
			// left uses destination schema
			// right uses |rightSchema|
			newLeft := fixExprToScope(set.Left, dstScope)
			newRight := fixExprToScope(set.Right, rightScope)
			s.expressions = append(s.expressions, expression.NewSetField(newLeft, newRight))
		}
		for _, c := range n.Checks() {
			newE := fixExprToScope(c.Expr, dstScope)
			newCheck := *c
			newCheck.Expr = newE
			s.checks = append(s.checks, &newCheck)
		}
	case *plan.Update:
		newScope := s.copy()
		srcScope := s.childScopes[0]
		// schema is |old_row|-|new_row|; checks only recieve half
		newScope.columns = append(newScope.columns, srcScope.columns[:len(srcScope.columns)/2]...)
		for _, c := range n.Checks() {
			newE := fixExprToScope(c.Expr, newScope)
			newCheck := *c
			newCheck.Expr = newE
			s.checks = append(s.checks, &newCheck)
		}
	case *plan.CreateTable:
		scope := s.copy()
		scope.addSchema(n.CreateSchema.Schema)
		for _, c := range n.Checks() {
			newE := fixExprToScope(c.Expr, scope)
			newCheck := *c
			newCheck.Expr = newE
			s.checks = append(s.checks, &newCheck)
		}
	default:
		if ne, ok := n.(sql.Expressioner); ok {
			scope := append(s.parentScopes, s.childScopes...)
			for _, e := range ne.Expressions() {
				// default nodes can't see lateral join nodes, unless we're in lateral
				// join and lateral scopes are promoted to parent status
				s.expressions = append(s.expressions, fixExprToScope(e, scope...))
			}
		}
	}
	return nil
}

// finalizeSelf builds the output node and fixes the return scope
func (s *idxScope) finalizeSelf(n sql.Node) (sql.Node, error) {
	// assumes children scopes have been set
	switch n := n.(type) {
	case *plan.InsertInto:
		s.addSchema(n.Destination.Schema())
		nn := *n
		nn.Source = s.children[0]
		nn.Destination = s.children[1]
		nn.OnDupExprs = s.expressions
		return nn.WithChecks(s.checks), nil
	default:
		// child scopes don't account for projections
		s.addSchema(n.Schema())
		var err error
		if s.children != nil {
			n, err = n.WithChildren(s.children...)
			if err != nil {
				return nil, err
			}
		}
		if ne, ok := n.(sql.Expressioner); ok && s.expressions != nil {
			n, err = ne.WithExpressions(s.expressions...)
			if err != nil {
				return nil, err
			}
		}
		if nc, ok := n.(sql.CheckConstraintNode); ok && s.checks != nil {
			n = nc.WithChecks(s.checks)
		}
		if jn, ok := n.(*plan.JoinNode); ok {
			if len(s.parentScopes) == 0 {
				return n, nil
			}
			// TODO: combine scopes?
			scopeLen := len(s.parentScopes[0].columns)
			if scopeLen == 0 {
				return n, nil
			}
			n = jn.WithScopeLen(scopeLen)
			n, err = n.WithChildren(
				plan.NewStripRowNode(jn.Left(), scopeLen),
				plan.NewStripRowNode(jn.Right(), scopeLen),
			)
			if err != nil {
				return nil, err
			}
		}
		return n, nil
	}
}

func fixExprToScope(e sql.Expression, scopes ...*idxScope) sql.Expression {
	newScope := &idxScope{}
	for _, s := range scopes {
		newScope.addScope(s)
	}
	ret, _, _ := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// TODO: this is a swallowed error. It triggers falsely in queries involving the dual table, or queries where
			//  the columns being selected are only found in subqueries
			idx, _ := newScope.getIdx(e.String())
			return e.WithIndex(idx), transform.NewTree, nil
		case *plan.Subquery:
			// this |outScope| prepends the subquery scope
			newQ, _, err := assignIndexesHelper(e.Query, newScope.push())
			if err != nil {
				return nil, transform.SameTree, err
			}
			return e.WithQuery(newQ), transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
	return ret
}
