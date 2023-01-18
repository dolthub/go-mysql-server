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
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// constructJoinPlan finds an optimal table ordering and access plan
// for the tables in the query.
func constructJoinPlan(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("construct_join_plan")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	if plan.IsNoRowNode(n) {
		return n, transform.SameTree, nil
	}

	_, isUpdate := n.(*plan.Update)

	reorder := true
	transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.JSONTable:
			// TODO make a JoinTypeJSONTable[Cross], and use have its TES
			// treated the same way as a left join for reordering.
			reorder = false
		case *plan.Project:
			// TODO: fix natural joins, their project nodes should apply
			// to the top-level scope not the middle of a join tree.
			switch c.Parent.(type) {
			case *plan.JoinNode:
				reorder = false
			}
		case *plan.JoinNode:
			if n.JoinType().IsPhysical() {
				// TODO: nested subqueries attempt to replan joins, which
				// is not ideal but not the end of the world.
				reorder = false
			}
		default:
		}
		return n, transform.SameTree, nil
	})
	return inOrderReplanJoin(ctx, a, scope, nil, n, reorder, isUpdate)
}

// inOrderReplanJoin either fixes field indexes for join nodes or
// replans a join.
// TODO: fixing JSONTable and natural joins makes this unnecessary
func inOrderReplanJoin(
	ctx *sql.Context,
	a *Analyzer,
	scope *Scope,
	sch sql.Schema,
	n sql.Node,
	reorder, isUpdate bool,
) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := n.(sql.OpaqueNode); ok {
		return n, transform.SameTree, nil
	}

	children := n.Children()
	var newChildren []sql.Node
	allSame := transform.SameTree
	j, ok := n.(*plan.JoinNode)
	if !ok {
		for i := range children {
			newChild, same, err := inOrderReplanJoin(ctx, a, scope, sch, children[i], reorder, isUpdate)
			if err != nil {
				return n, transform.SameTree, err
			}
			if !same {
				if len(newChildren) == 0 {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = newChild
				allSame = transform.NewTree
			}
		}
		if allSame {
			return n, transform.SameTree, nil
		}
		ret, err := n.WithChildren(newChildren...)
		if err != nil {
			return nil, transform.SameTree, nil
		}
		return ret, transform.NewTree, err
	}

	// two different base cases, depending on whether we reorder or not
	if reorder {
		ret, err := replanJoin(ctx, j, a, scope)
		if err != nil {
			return nil, transform.SameTree, fmt.Errorf("failed to replan join: %w", err)
		}
		if isUpdate {
			ret = plan.NewProject(expression.SchemaToGetFields(n.Schema()), ret)
		}
		return ret, transform.NewTree, nil
	}

	l, lSame, err := inOrderReplanJoin(ctx, a, scope, sch, j.Left(), reorder, isUpdate)
	if err != nil {
		return nil, transform.SameTree, err
	}
	rView := append(sch, j.Left().Schema()...)
	r, rSame, err := inOrderReplanJoin(ctx, a, scope, rView, j.Right(), reorder, isUpdate)
	if err != nil {
		return nil, transform.SameTree, err
	}
	ret, err := j.WithChildren(l, r)
	if err != nil {
		return n, transform.SameTree, nil
	}
	if j.JoinCond() != nil {
		selfView := append(sch, j.Schema()...)
		f, fSame, err := FixFieldIndexes(scope, a, selfView, j.JoinCond())
		if lSame && rSame && fSame {
			return n, transform.SameTree, nil
		}
		ret, err = j.WithExpressions(f)
		if err != nil {
			return n, transform.SameTree, nil
		}
	}
	return ret, transform.NewTree, nil
}

func replanJoin(ctx *sql.Context, n *plan.JoinNode, a *Analyzer, scope *Scope) (sql.Node, error) {
	stats, err := a.Catalog.Statistics(ctx)
	if err != nil {
		return nil, err
	}

	m := NewMemo(ctx, stats, scope)

	j := newJoinOrderBuilder(m)
	j.reorderJoin(n)

	addRightSemiJoins(m)
	addLookupJoins(m)
	addHashJoins(m)
	addMergeJoins(m)

	if a.Verbose && a.Debug {
		a.Log(m.String())
	}

	if hint := extractJoinHint(n); !hint.IsEmpty() {
		// this should probably happen earlier, but the root is not
		// populated before reordering
		m.WithJoinOrder(hint)
	}

	m.optimizeRoot()
	return m.bestRootPlan()
}

// addLookupJoins prefixes memo join group expressions with indexed join
// alternatives to join plans added by joinOrderBuilder. We can assume that a
// join with a non-nil join filter is not degenerate, and we can apply indexed
// joins for any join plan where the right child is i) an indexable relation,
// ii) with an index that matches a prefix of the indexable relation's free
// attributes in the join filter. Costing is responsible for choosing the most
// appropriate execution plan among options added to an expression group.
func addLookupJoins(m *Memo) error {
	var aliases = make(TableAliases)
	seen := make(map[GroupId]struct{})
	return dfsExprGroup(m.root, m, seen, func(e relExpr) error {
		var right *exprGroup
		var join *joinBase
		switch e := e.(type) {
		case *innerJoin:
			right = e.right
			join = e.joinBase
		case *leftJoin:
			right = e.right
			join = e.joinBase
		//TODO fullouterjoin
		case *semiJoin:
			right = e.right
			join = e.joinBase
		case *antiJoin:
			right = e.right
			join = e.joinBase
		default:
			return nil
		}

		if len(join.filter) == 0 {
			return nil
		}

		attrSource, indexes, err := lookupCandidates(m.ctx, right.first, aliases)
		if err != nil {
			return err
		}

		if or, ok := join.filter[0].(*expression.Or); ok && len(join.filter) == 1 {
			// Special case disjoint filter. The execution plan will perform an index
			// lookup for each predicate leaf in the OR tree.
			// TODO: memoize equality expressions, index lookup, concat so that we
			// can consider multiple index options. Otherwise the search space blows
			// up.
			conds := splitDisjunction(or)
			concat := splitIndexableOr(conds, indexes, attrSource, aliases)
			if len(concat) != len(conds) {
				return nil
			}
			rel := &concatJoin{
				joinBase: join.copy(),
				concat:   concat,
			}
			for _, l := range concat {
				l.parent = rel.joinBase
			}
			e.group().prepend(rel)
			return nil
		}

		conds := collectJoinConds(attrSource, join.filter...)
		for _, idx := range indexes {
			keyExprs, nullmask := indexMatchesKeyExprs(idx, conds, aliases)
			if len(keyExprs) == 0 {
				continue
			}
			rel := &lookupJoin{
				joinBase: join.copy(),
				lookup: &lookup{
					source:   attrSource,
					index:    idx,
					keyExprs: keyExprs,
					nullmask: nullmask,
				},
			}
			rel.lookup.parent = rel.joinBase
			e.group().prepend(rel)
		}
		return nil
	})
}

// addRightSemiJoins allows for a reversed semiJoin operator when
// the join attributes of the left side are provably unique.
func addRightSemiJoins(m *Memo) error {
	var aliases = make(TableAliases)
	seen := make(map[GroupId]struct{})
	return dfsExprGroup(m.root, m, seen, func(e relExpr) error {
		semi, ok := e.(*semiJoin)
		if !ok {
			return nil
		}

		if len(semi.filter) == 0 {
			return nil
		}
		attrSource, indexes, err := lookupCandidates(m.ctx, semi.left.first, aliases)
		if err != nil {
			return err
		}

		// check that the right side is unique on the join keys
		conds := collectJoinConds(attrSource, semi.filter...)
		for _, idx := range indexes {
			if !idx.IsUnique() {
				continue
			}
			keyExprs, nullmask := indexMatchesKeyExprs(idx, conds, aliases)
			if len(keyExprs) == 0 {
				continue
			}
			if len(keyExprs) != len(idx.Expressions()) {
				continue
			}

			rel := &lookupJoin{
				joinBase: semi.joinPrivate().copy(),
				lookup: &lookup{
					source:   attrSource,
					index:    idx,
					keyExprs: keyExprs,
					nullmask: nullmask,
				},
			}
			rel.op = plan.JoinTypeRightSemi
			rel.left, rel.right = rel.right, rel.left
			rel.lookup.parent = rel.joinBase
			e.group().prepend(rel)
		}
		return nil
	})
}

// lookupCandidates returns a normalized table name and a list of available
// candidate indexes as replacements for the given relExpr, or empty values
// if there are no suitable indexes.
func lookupCandidates(ctx *sql.Context, rel relExpr, aliases TableAliases) (string, []sql.Index, error) {
	switch n := rel.(type) {
	case *tableAlias:
		return tableAliasLookupCand(ctx, n.table, aliases)
	case *tableScan:
		return tableScanLookupCand(ctx, n.table)
	case *selectSingleRel:
		switch t := n.table.Rel.(type) {
		case *plan.TableAlias:
			return tableAliasLookupCand(ctx, t, aliases)
		case *plan.ResolvedTable:
			return tableScanLookupCand(ctx, t)
		default:
		}
	default:
	}
	return "", nil, nil

}

func tableScanLookupCand(ctx *sql.Context, n *plan.ResolvedTable) (string, []sql.Index, error) {
	attributeSource := strings.ToLower(n.Name())
	table := n.Table
	if w, ok := table.(sql.TableWrapper); ok {
		table = w.Underlying()
	}
	indexableTable, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return "", nil, nil
	}
	indexes, err := indexableTable.GetIndexes(ctx)
	if err != nil {
		return "", nil, err
	}
	return attributeSource, indexes, nil
}

func tableAliasLookupCand(ctx *sql.Context, n *plan.TableAlias, aliases TableAliases) (string, []sql.Index, error) {
	attributeSource := strings.ToLower(n.Name())
	rt, ok := n.Child.(*plan.ResolvedTable)
	if !ok {
		return "", nil, nil
	}
	table := rt.Table
	if w, ok := table.(sql.TableWrapper); ok {
		table = w.Underlying()
	}
	indexableTable, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return "", nil, nil
	}
	aliases.add(n, indexableTable)
	indexes, err := indexableTable.GetIndexes(ctx)
	if err != nil {
		return "", nil, nil
	}
	return attributeSource, indexes, nil
}

// dfsExprGroup runs a callback |cb| on all execution plans in the memo expression
// group. An expression group is defined by 1) a set of child expression
// groups that serve as logical inputs to this operator, and 2) a set of logically
// equivalent plans for executing this expression group's operator. We recursively
// walk to expression group leaves, and then traverse every execution plan in leaf
// groups before working upwards back to the root group.
func dfsExprGroup(grp *exprGroup, m *Memo, seen map[GroupId]struct{}, cb func(rel relExpr) error) error {
	if _, ok := seen[grp.id]; ok {
		return nil
	} else {
		seen[grp.id] = struct{}{}
	}
	n := grp.first
	for n != nil {
		for _, c := range n.children() {
			err := dfsExprGroup(c, m, seen, cb)
			if err != nil {
				return err
			}
		}
		err := cb(n)
		if err != nil {
			return err
		}
		n = n.next()
	}
	return nil
}

func collectJoinConds(attributeSource string, filters ...sql.Expression) []*joinColExpr {
	var conds []*joinColExpr
	var outer []sql.Expression
	for i := range filters {
		l, r := extractJoinColumnExpr(filters[i])
		if l == nil || r == nil {
			// unusable as lookup
			outer = append(outer, filters[i])
			continue
		}
		// TODO(max): expression algebra to isolate arithmetic
		// ex: (b.i = c.i 	+ 1) cannot use a c.i lookup without converting the
		// expression to (b.i - 1 = c.i), so that (b.i - 1) is a proper lookup
		// key
		if _, ok := l.colExpr.(*expression.GetField); ok && strings.ToLower(l.col.Table()) == attributeSource {
			conds = append(conds, l)
		} else if _, ok := r.colExpr.(*expression.GetField); ok && strings.ToLower(r.col.Table()) == attributeSource {
			conds = append(conds, r)
		} else {
			outer = append(outer, filters[i])
		}
	}
	return conds
}

// indexMatchesKeyExprs returns keyExprs and nullmask for a parametrized
// lookup from the outer scope (row) into the given index for a join condition.
// For example, the filters: [(ab.a + 1 = xy.y), (ab.b <=> xy.x)] will cover
// the the index xy(x,y). The second filter is not null rejecting, so the nullmask
// will be [0,1]. The keyExprs will be [(ab.a + 1), (ab.b)], which project into
// the table lookup (xy.x, xy.y).
func indexMatchesKeyExprs(
	i sql.Index,
	joinColExprs []*joinColExpr,
	tableAliases TableAliases,
) ([]sql.Expression, []bool) {
	idxExprs := i.Expressions()
	count := len(idxExprs)
	if count > len(joinColExprs) {
		count = len(joinColExprs)
	}
	keyExprs := make([]sql.Expression, count)
	nullmask := make([]bool, count)

IndexExpressions:
	for i := 0; i < count; i++ {
		for j, col := range joinColExprs {
			// check same column name
			if strings.ToLower(idxExprs[i]) == strings.ToLower(normalizeExpression(tableAliases, col.col).String()) {
				// get field into left table
				keyExprs[i] = joinColExprs[j].comparand
				nullmask[i] = joinColExprs[j].matchnull
				continue IndexExpressions
			}
		}
		return nil, nil
	}

	// TODO: better way of validating that we can apply an index lookup
	lb := plan.NewLookupBuilder(i, keyExprs, nullmask)
	look, err := lb.GetLookup(lb.GetZeroKey())
	if err != nil {
		return nil, nil
	}
	if !i.CanSupport(look.Ranges...) {
		return nil, nil
	}

	return keyExprs, nullmask
}

// splitIndexableOr attempts to build a list of index lookups for a disjoint
// filter expression. The prototypical pattern will be a tree of OR and equality
// expressions: [eq] OR [eq] OR [eq] ...
func splitIndexableOr(filters []sql.Expression, indexes []sql.Index, attributeSource string, aliases TableAliases) []*lookup {
	var concat []*lookup
	for _, f := range filters {
		if eq, ok := f.(*expression.Equals); ok {
			i := firstMatchingIndex(eq, indexes, attributeSource, aliases)
			if i == nil {
				return nil
			}
			concat = append(concat, i)
		}
	}
	return concat
}

// firstMatchingIndex returns first index that |e| can use as a lookup.
// This simplifies index selection for concatJoin to avoid building
// memo objects for equality expressions and indexes.
func firstMatchingIndex(e *expression.Equals, indexes []sql.Index, attributeSource string, aliases TableAliases) *lookup {
	for _, lIdx := range indexes {
		lConds := collectJoinConds(attributeSource, e)
		lKeyExprs, lNullmask := indexMatchesKeyExprs(lIdx, lConds, aliases)
		if len(lKeyExprs) == 0 {
			continue
		}

		return &lookup{
			index:    lIdx,
			keyExprs: lKeyExprs,
			nullmask: lNullmask,
		}
	}
	return nil
}

func addHashJoins(m *Memo) error {
	seen := make(map[GroupId]struct{})
	return dfsExprGroup(m.root, m, seen, func(e relExpr) error {
		switch e.(type) {
		case *innerJoin, *leftJoin:
		default:
			return nil
		}

		join := e.(joinRel).joinPrivate()
		if len(join.filter) == 0 {
			return nil
		}

		var innerExpr, outerExpr []sql.Expression
		for _, f := range join.filter {
			switch f := f.(type) {
			case *expression.Equals:
				if exprMapsToSource(f.Left(), join.left, m.tableProps) &&
					exprMapsToSource(f.Right(), join.right, m.tableProps) {
					innerExpr = append(innerExpr, f.Left())
					outerExpr = append(outerExpr, f.Right())
				} else if exprMapsToSource(f.Right(), join.left, m.tableProps) &&
					exprMapsToSource(f.Left(), join.right, m.tableProps) {
					innerExpr = append(innerExpr, f.Right())
					outerExpr = append(outerExpr, f.Left())
				} else {
					return nil
				}
			default:
				return nil
			}
		}
		rel := &hashJoin{
			joinBase:   join.copy(),
			innerAttrs: innerExpr,
			outerAttrs: outerExpr,
		}
		e.group().prepend(rel)
		return nil
	})
}

// exprMapsToSource returns true if all GetFields in the expression
// source outputs from |grp|
func exprMapsToSource(e sql.Expression, grp *exprGroup, tProps *tableProps) bool {
	outerOnly := true
	transform.InspectExpr(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			if id, ok := tProps.getId(strings.ToLower(e.Table())); ok {
				exprTable := sql.NewFastIntSet(int(tableIdForSource(id)))
				outerOnly = outerOnly && exprTable.Intersects(grp.relProps.OutputTables())
			}
		default:
		}
		return !outerOnly
	})
	return outerOnly
}

// addMergeJoins will add merge join operators to join relations
// with native indexes providing sort enforcement.
// TODO: sort-merge joins
func addMergeJoins(m *Memo) error {
	var aliases = make(TableAliases)
	seen := make(map[GroupId]struct{})
	return dfsExprGroup(m.root, m, seen, func(e relExpr) error {
		var join *joinBase
		switch e := e.(type) {
		case *innerJoin:
			join = e.joinBase
		case *leftJoin:
			join = e.joinBase
			//TODO semijoin, antijoin, fullouterjoin
		default:
			return nil
		}

		if len(join.filter) == 0 {
			return nil
		}

		lIScan, err := findSortedIndexScanForRel(m.ctx, join.left.first, join.filter, aliases)
		if err != nil {
			return err
		} else if lIScan == nil {
			return nil
		}

		rIScan, err := findSortedIndexScanForRel(m.ctx, join.right.first, join.filter, aliases)
		if err != nil {
			return err
		} else if rIScan == nil {
			return nil
		}

		var newFilters []sql.Expression
		for _, f := range join.filter {
			if e, ok := f.(*expression.Equals); ok {
				// filter must bisect the rel attributes the merge comparison
				// result to be monotonic
				lTab, ok := attrsRefSingleRel(e.Left())
				if !ok {
					return nil
				}
				rTab, ok := attrsRefSingleRel(e.Right())
				if !ok {
					return nil
				}
				if lTab == rIScan.source && rTab == lIScan.source {
					// comparison direction determines next iterator increment
					newFilters = append(newFilters, expression.NewEquals(e.Right(), e.Left()))
				} else {
					newFilters = append(newFilters, f)
				}
			} else {
				return nil
			}
		}

		jb := join.copy()
		jb.filter = newFilters
		rel := &mergeJoin{
			joinBase:  jb,
			innerScan: lIScan,
			outerScan: rIScan,
		}
		rel.innerScan.parent = rel.joinBase
		rel.outerScan.parent = rel.joinBase
		e.group().prepend(rel)
		return nil
	})
}

// findSortedIndexScanForRel returns the first indexScan found for a relation
// that provide a prefix for the joinFilters rel free attributes. I.e. the
// indexScan will return the same rows as the rel, but sorted for every expression
// for the table referenced in the join condition.
func findSortedIndexScanForRel(
	ctx *sql.Context,
	rel relExpr,
	joinFilters []sql.Expression,
	aliases TableAliases,
) (*indexScan, error) {
	attrSource, indexes, err := lookupCandidates(ctx, rel, aliases)
	if err != nil {
		return nil, err
	}

	conds := collectJoinConds(attrSource, joinFilters...)
	for _, idx := range indexes {
		keyExprs, _ := indexMatchesKeyExprs(idx, conds, aliases)
		if len(keyExprs) == 0 {
			continue
		}
		return &indexScan{
			source: attrSource,
			idx:    idx,
		}, nil
	}
	return nil, nil
}

// attrsRefSingleRel returns false if there are
// getFields sourced from more than one table.
func attrsRefSingleRel(e sql.Expression) (string, bool) {
	var name string
	var invalid bool
	transform.InspectExpr(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			newName := strings.ToLower(e.Table())
			if name == "" && !invalid {
				name = newName
			} else if name != newName {
				invalid = true
			}
		default:
		}
		return invalid
	})
	return name, !invalid
}

func extractJoinHint(n *plan.JoinNode) JoinOrderHint {
	if n.Comment() != "" {
		return parseJoinHint(n.Comment())
	}
	return EmptyJoinOrder
}

var hintRegex = regexp.MustCompile("(\\s*[a-z_]+\\([^\\(]+\\)\\s*)+")

// TODO: this is pretty nasty. Should be done in the parser instead.
func parseJoinHint(comment string) JoinOrderHint {
	comment = strings.TrimPrefix(comment, "/*+")
	comment = strings.TrimSuffix(comment, "*/")
	comment = strings.ToLower(strings.TrimSpace(comment))

	hints := hintRegex.FindAll([]byte(comment), -1)

	for _, hint := range hints {
		hintStr := strings.TrimSpace(string(hint))
		if strings.HasPrefix(string(hintStr), "join_order(") {
			var tables []string
			var table strings.Builder
			for _, b := range hintStr[len("join_order("):] {
				switch b {
				case ',', ')':
					tables = append(tables, strings.TrimSpace(table.String()))
					table = strings.Builder{}
				default:
					table.WriteRune(b)
				}
			}

			return JoinOrderHint{
				tables: tables,
			}
		}
	}

	return EmptyJoinOrder
}

type QueryHint interface {
	fmt.Stringer
	HintType() string
}

type JoinOrderHint struct {
	tables []string
}

var EmptyJoinOrder = JoinOrderHint{}

func (j JoinOrderHint) String() string {
	return "JOIN_ORDER(" + strings.Join(j.tables, ",") + ")"

}

func (j JoinOrderHint) HintType() string {
	return "JOIN_ORDER"
}

func (j JoinOrderHint) IsEmpty() bool {
	return len(j.tables) == 0
}

// joinOrderDeps encodes a groups relational dependencies in a bitset.
// This is equivalent to an expression group's base table inputs but
// reordered by the join hint table order.
type joinOrderDeps struct {
	groups map[GroupId]vertexSet
	cache  map[uint64]bool
	order  map[GroupId]uint64
}

func newJoinOrderDeps(order map[GroupId]uint64) *joinOrderDeps {
	return &joinOrderDeps{
		groups: make(map[GroupId]vertexSet),
		cache:  make(map[uint64]bool),
		order:  order,
	}
}

func (o joinOrderDeps) build(grp *exprGroup) {
	s := vertexSet(0)
	// convert global table order to hint order
	inputs := grp.relProps.OutputTables()
	for idx, ok := inputs.Next(0); ok; idx, ok = inputs.Next(idx + 1) {
		if i, ok := o.order[GroupId(idx+1)]; ok {
			// If group |idx+1| is a dependency of this table, record the
			// ordinal position of that group given by the hint order.
			s = s.add(i)
		}
	}
	o.groups[grp.id] = s

	for _, g := range grp.children() {
		if _, ok := o.groups[g.id]; !ok {
			// avoid duplicate work
			o.build(g)
		}
	}
}

func (o joinOrderDeps) isValid() bool {
	for _, v := range o.groups {
		if v == vertexSet(0) {
			// invalid hint table name, fallback
			return false
		}
	}
	return true
}

func (o joinOrderDeps) obeysOrder(n relExpr) bool {
	key := relKey(n)
	if v, ok := o.cache[key]; ok {
		return v
	}
	switch n := n.(type) {
	case joinRel:
		base := n.joinPrivate()
		if !base.left.orderSatisfied || !base.right.orderSatisfied {
			return false
		}
		l := o.groups[base.left.id]
		r := o.groups[base.right.id]
		valid := o.ordered(l, r) && o.compact(l, r)
		o.cache[key] = valid
		return valid
	default:
		return true
	}
}

func (o joinOrderDeps) ordered(s1, s2 vertexSet) bool {
	return s1 < s2
}

func (o joinOrderDeps) compact(s1, s2 vertexSet) bool {
	if s1 == 0 || s2 == 0 {
		panic("unexpected nil vertex set")
	}
	union := s1.union(s2)
	last, _ := union.next(0)
	next, ok := union.next(last + 1)
	for ok {
		if last+1 != next {
			return false
		}
		last = next
		next, ok = union.next(next + 1)
	}

	// sets are compact, s1 higher than s2
	return true
}

func transposeRightJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.JoinNode:
			if n.Op.IsRightOuter() {
				return plan.NewLeftOuterJoin(n.Right(), n.Left(), n.Filter), transform.NewTree, nil
			}
		default:
		}
		return n, transform.SameTree, nil
	})
}
