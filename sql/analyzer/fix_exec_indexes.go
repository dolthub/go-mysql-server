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
		s.triggerScope = true
		s.addSchema(scope.Schema())
		s = s.push()
	}
	switch n := n.(type) {
	case *plan.InsertInto:
		if n.LiteralValueSource && len(n.Checks()) == 0 && len(n.OnDupExprs) == 0 {
			return n, transform.SameTree, nil
		}
	default:
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
	ids           []sql.ColumnId
	columns       []string
	children      []sql.Node
	expressions   []sql.Expression
	checks        sql.CheckConstraints
	triggerScope  bool
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
	s.ids = append(s.ids, other.ids...)
}

func (s *idxScope) addLateral(other *idxScope) {
	s.lateralScopes = append(s.lateralScopes, other)
}

func (s *idxScope) addParent(other *idxScope) {
	s.parentScopes = append(s.parentScopes, other)
}

func isQualified(s string) bool {
	return strings.Contains(s, ".")
}

// unqualify is a helper function to remove the table prefix from a column, if it's present.
func unqualify(s string) string {
	if isQualified(s) {
		return strings.Split(s, ".")[1]
	}
	return s
}

func (s *idxScope) getIdxId(id sql.ColumnId, name string) (int, bool) {
	if s.triggerScope || id == 0 {
		// todo: add expr ids for trigger columns and procedure params
		return s.getIdx(name)
	}
	for i, c := range s.ids {
		if c == id {
			return i, true
		}
	}
	// todo: fix places where this is necessary
	return s.getIdx(name)
}

func (s *idxScope) getIdx(n string) (int, bool) {
	// We match the column closet to our current scope. We have already
	// resolved columns, so there will be no in-scope collisions.
	if isQualified(n) {
		for i := len(s.columns) - 1; i >= 0; i-- {
			if strings.EqualFold(n, s.columns[i]) {
				return i, true
			}
		}
		// TODO: we do not have a good way to match columns over set_ops where the column has the same name, but are
		//  from different tables and have different types.
		n = unqualify(n)
		for i := len(s.columns) - 1; i >= 0; i-- {
			if strings.EqualFold(n, s.columns[i]) {
				return i, true
			}
		}
	} else {
		for i := len(s.columns) - 1; i >= 0; i-- {
			if strings.EqualFold(n, unqualify(s.columns[i])) {
				return i, true
			}
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
	var idsCopy []sql.ColumnId
	if len(s.ids) > 0 {
		idsCopy = make([]sql.ColumnId, len(s.ids))
		copy(idsCopy, s.ids)
	}
	return &idxScope{
		lateralScopes: lateralCopy,
		parentScopes:  parentCopy,
		columns:       varsCopy,
		ids:           idsCopy,
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
			// TODO: this should not apply to subqueries inside of lateral joins
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
	case *plan.Procedure, *plan.CreateTable:
		// do nothing
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
		// value indexes other side of join
		newValue := fixExprToScope(n.ValueColumnGf, s.lateralScopes...)
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
			newLeft := fixExprToScope(set.LeftChild, dstScope)
			newRight := fixExprToScope(set.RightChild, rightScope)
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
		s.ids = columnIdsForNode(n)

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

// columnIdsForNode collects the column ids of a node's return schema.
// Projector nodes can return a subset of the full sql.PrimaryTableSchema.
// todo: pruning projections should update plan.TableIdNode .Columns()
// to avoid schema/column discontinuities.
func columnIdsForNode(n sql.Node) []sql.ColumnId {
	var ret []sql.ColumnId
	switch n := n.(type) {
	case sql.Projector:
		for _, e := range n.ProjectedExprs() {
			if ide, ok := e.(sql.IdExpression); ok {
				ret = append(ret, ide.Id())
			} else {
				ret = append(ret, 0)
			}
		}
	case *plan.TableCountLookup:
		ret = append(ret, n.Id())
	case *plan.TableAlias:
		// Table alias's child either exposes 1) child ids or 2) is custom
		// table function. We currently do not update table columns in response
		// to table pruning, so we need to manually distinguish these cases.
		// todo: prune columns should update column ids and table alias ids
		switch n.Child.(type) {
		case sql.TableFunction:
			// todo: table functions that implement sql.Projector are not going
			// to work. Need to fix prune.
			n.Columns().ForEach(func(col sql.ColumnId) {
				ret = append(ret, col)
			})
		default:
			ret = append(ret, columnIdsForNode(n.Child)...)
		}
	case plan.TableIdNode:
		if rt, ok := n.(*plan.ResolvedTable); ok && plan.IsDualTable(rt.Table) {
			ret = append(ret, 0)
			break
		}

		cols := n.(plan.TableIdNode).Columns()
		if tn, ok := n.(sql.TableNode); ok {
			if pkt, ok := tn.UnderlyingTable().(sql.PrimaryKeyTable); ok && len(pkt.PrimaryKeySchema().Schema) != len(n.Schema()) {
				firstcol, _ := cols.Next(1)
				for _, c := range n.Schema() {
					ord := pkt.PrimaryKeySchema().IndexOfColName(c.Name)
					colId := firstcol + sql.ColumnId(ord)
					ret = append(ret, colId)
				}
				break
			}
		}
		cols.ForEach(func(col sql.ColumnId) {
			ret = append(ret, col)
		})
	case *plan.JoinNode:
		if n.Op.IsPartial() {
			ret = append(ret, columnIdsForNode(n.Left())...)
		} else {
			ret = append(ret, columnIdsForNode(n.Left())...)
			ret = append(ret, columnIdsForNode(n.Right())...)
		}
	case *plan.ShowStatus:
		for i := range n.Schema() {
			ret = append(ret, sql.ColumnId(i+1))
		}
	case *plan.Concat:
		ret = append(ret, columnIdsForNode(n.Left())...)
	default:
		for _, c := range n.Children() {
			ret = append(ret, columnIdsForNode(c)...)
		}
	}
	return ret
}

func fixExprToScope(e sql.Expression, scopes ...*idxScope) sql.Expression {
	newScope := &idxScope{}
	for _, s := range scopes {
		newScope.triggerScope = newScope.triggerScope || s.triggerScope
		newScope.addScope(s)
	}
	ret, _, _ := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// TODO: this is a swallowed error in some cases. It triggers falsely in queries involving the dual table, or
			//  queries where the columns being selected are only found in subqueries. Conversely, we actually want to ignore
			//  this error for the case of DEFAULT in a `plan.Values`, since we analyze the insert source in isolation (we
			//  don't have the destination schema, and column references in default values are determined in the build phase)
			idx, _ := newScope.getIdxId(e.Id(), e.String())
			if idx >= 0 {
				return e.WithIndex(idx), transform.NewTree, nil
			}
			return e, transform.SameTree, nil
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
