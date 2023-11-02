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
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// costedIndexScans matches a Filter-ResolvedTable pattern, and tries to
// use those filters to create a better IndexedTableAccess plan. We first
// convert the filter into a format that separates index-supported and
// unsupported filters, the unsupported remaining in the Filter parent.
// We then attempt to construct index scans using each table index and the
// set of index-supported filters. Each individual index greedily consumes
// filters. We use statistical cost and functional dependencies to compare
// indexScan options. Then we use metadata for the best indexScan to
// (1) convert the included filters to a sql.RangeCollection needed and
// then a sql.IndexLookup, and (2) collect the unused filters as a
// replacement parent Filter.
//
// It is worth noting that AND and OR filters behave differently. An OR
// filter can only be converted into an index scan if its entire child
// tree can be converted into a sql.Range. An AND filter can convert a
// fraction of its conjunctions into an indexScan, with the excluded
// remaining in the parent filter. Much of the format conversions focus
// on maintaining this invariant.
func costedIndexScans(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		filter, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}

		rt, ok := filter.Child.(*plan.ResolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		statistics, err := a.Catalog.StatsProvider.GetTableStats(ctx, strings.ToLower(rt.Database().Name()), strings.ToLower(rt.Name()))
		if err != nil {
			return n, transform.SameTree, err
		}

		var qualToStat map[sql.StatQualifier]sql.Statistic
		if len(statistics) == 0 {
			qualToStat, err = uniformDistStatistics(ctx, rt)
		} else {
			qualToStat = make(map[sql.StatQualifier]sql.Statistic)
			for _, stat := range statistics {
				if prev, ok := qualToStat[stat.Qualifier()]; !ok || ok && len(stat.Columns()) > len(prev.Columns()) {
					qualToStat[stat.Qualifier()] = stat
				}
			}
		}

		// flatten expression tree for costing
		c := newIndexCoster()
		root, leftover := c.flatten(filter.Expression)

		// run each index through coster, save the cheapest
		for _, stat := range qualToStat {
			err := c.cost(root, stat)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		iat := rt.Table.(sql.IndexAddressableTable)
		indexes, err := iat.GetIndexes(ctx)
		if err != nil {
			return n, transform.SameTree, err
		}

		targetId := c.bestStat.Qualifier().Index()
		var idx sql.Index
		for _, i := range indexes {
			if strings.EqualFold(i.ID(), targetId) {
				idx = i
				break
			}
		}
		if idx == nil {
			return n, transform.SameTree, fmt.Errorf("tried building indexScan with unknown statistic index: %s", targetId)
		}

		// separate |include| and |exclude| filters
		b := newIndexScanRangeBuilder(ctx, idx, c.bestFilters, c.idToExpr)
		b.leftover = append(b.leftover, leftover)
		b.buildRangeCollection(root)

		ranges, err := sql.RemoveOverlappingRanges(b.allRanges...)
		if err != nil {
			return n, transform.SameTree, err
		}

		// create ranges, lookup, ITA for best indexScan
		// TODO pass up FALSE filter information
		lookup := sql.NewIndexLookup(idx, ranges, false, false, idx.IsSpatial(), false)

		ita, err := plan.NewStaticIndexedAccessForTableNode(rt, lookup)
		if err != nil {
			return n, transform.SameTree, nil
		}

		// excluded from tree + not included in index scan => filter above scan
		newFilter := expression.JoinAnd(b.leftover...)

		return plan.NewFilter(newFilter, ita), transform.NewTree, nil
	})
}

func newIndexCoster() *indexCoster {
	return &indexCoster{
		i:        1,
		idToExpr: make(map[indexScanId]sql.Expression),
	}
}

type indexCoster struct {
	i indexScanId
	// idToExpr is a record of conj decomposition so we can remove duplicates later
	idToExpr map[indexScanId]sql.Expression
	// bestStat is the lowest cardinality indexScan option
	bestStat sql.Statistic
	// bestFilters is the set of conjunctions used to create bestStat
	bestFilters sql.FastIntSet
	// invalid are expressions not considered for indexing
	invalid []sql.Expression
}

// cost tries to build the lowest cardinality index scan for an expression
// tree rooted at |f| on an index whose statistics are represented by |stats|.
func (c *indexCoster) cost(f indexFilter, stat sql.Statistic) error {
	ordinals := ordinalsForStat(stat)

	var newStat sql.Statistic
	var filters sql.FastIntSet
	var err error
	var ok bool

	switch f := f.(type) {
	case *iScanAnd:
		newStat, filters, err = c.costIndexScanAnd(f, stat, ordinals)
		if err != nil {
			return err
		}

	case *iScanOr:
		newStat, ok, err = c.costIndexScanOr(f, stat, ordinals)
		if err != nil {
			return err
		}
		if ok {
			filters.Add(int(f.id))
		}
	case *iScanLeaf:
		newStat, ok, err = c.costIndexScanLeaf(f, stat, ordinals)
		if err != nil {
			return err
		}
		if ok {
			filters.Add(int(f.id))
		}
	default:
		panic("unreachable")
	}

	c.updateBest(newStat, filters)
	return nil
}

func (c *indexCoster) updateBest(s sql.Statistic, filters sql.FastIntSet) {
	if c.bestStat == nil || s.RowCount() < c.bestStat.RowCount() {
		c.bestStat = s
		c.bestFilters = filters
	}
}

// flatten converts a filter into a tree of indexFilter, a format designed
// to make costing index scans easier. We return the root of the new tree
// and a conjunction of filters that cannot be pushed into index scans.
func (c *indexCoster) flatten(e sql.Expression) (indexFilter, sql.Expression) {
	switch e := e.(type) {
	case *expression.And:
		c.idToExpr[c.i] = e
		newAnd := newIScanAnd(c.i)
		c.i++
		invalid := c.flattenAnd(e, newAnd)
		var leftovers []sql.Expression
		for i, hasMore := invalid.Next(1); hasMore; i, hasMore = invalid.Next(i + 1) {
			f, ok := c.idToExpr[indexScanId(i)]
			if !ok {
				panic("todo filter map not working")
			}
			leftovers = append(leftovers, f)
		}

		return newAnd, expression.JoinAnd(leftovers...)

	case *expression.Or:
		c.idToExpr[c.i] = e
		newOr := &iScanOr{id: c.i}
		c.i++
		valid := c.flattenOr(e, newOr)
		if !valid {
			return nil, e
		}
		return newOr, nil

	default:
		c.idToExpr[c.i] = e
		leaf, ok := newLeaf(c.i, e)
		c.i++
		if !ok {
			return nil, e
		}
		return leaf, nil
	}
}

func (c *indexCoster) flattenAnd(e *expression.And, and *iScanAnd) sql.FastIntSet {
	var invalid sql.FastIntSet
	for _, e := range e.Children() {
		switch e := e.(type) {
		case *expression.And:
			c.idToExpr[c.i] = e
			c.i++
			inv := c.flattenAnd(e, and)
			invalid = invalid.Union(inv)
		case *expression.Or:
			c.idToExpr[c.i] = e
			newOr := &iScanOr{id: c.i}
			c.i++
			ok := c.flattenOr(e, newOr)
			if !ok {
				// this or is invalid
				invalid.Add(int(newOr.Id()))
			} else {
				and.orChildren = append(and.orChildren, newOr)
			}
		default:
			c.idToExpr[c.i] = e
			leaf, ok := newLeaf(c.i, e)
			if !ok {
				invalid.Add(int(c.i))
			} else {
				and.newLeaf(leaf)
			}
			// keep a ref to the invalid |e|
			c.i++
		}
	}
	return invalid
}

func (c *indexCoster) flattenOr(e *expression.Or, or *iScanOr) bool {
	for _, e := range e.Children() {
		switch e := e.(type) {
		case *expression.And:
			c.idToExpr[c.i] = e
			newAnd := &iScanAnd{id: c.i}
			c.i++
			inv := c.flattenAnd(e, newAnd)
			if !inv.Empty() {
				return false
			}
			or.children = append(or.children, newAnd)
		case *expression.Or:
			c.idToExpr[c.i] = e
			c.i++
			ok := c.flattenOr(e, or)
			if !ok {
				return false
			}
		default:
			c.idToExpr[c.i] = e
			leaf, ok := newLeaf(c.i, e)
			if !ok {
				return false
			} else {
				c.i++
				newAnd := &iScanAnd{id: c.i, leafChildren: make(map[string][]*iScanLeaf)}
				newAnd.newLeaf(leaf)
				or.children = append(or.children, newAnd)
			}
		}
	}
	return true
}

func newIndexScanRangeBuilder(ctx *sql.Context, idx sql.Index, include sql.FastIntSet, idToExpr map[indexScanId]sql.Expression) *indexScanRangeBuilder {
	return &indexScanRangeBuilder{
		ctx:      ctx,
		idx:      idx,
		include:  include,
		idToExpr: idToExpr,
	}
}

type indexScanRangeBuilder struct {
	ctx       *sql.Context
	idx       sql.Index
	include   sql.FastIntSet
	idToExpr  map[indexScanId]sql.Expression
	conjIb    *sql.IndexBuilder
	allRanges sql.RangeCollection
	leftover  []sql.Expression
}

// buildRangeCollection converts our representation of the best index scan
// into the format that represents an index lookup, a list of sql.Range.
func (b *indexScanRangeBuilder) buildRangeCollection(f indexFilter) {
	inIndexScan := b.include.Contains(int(f.Id()))
	if inIndexScan {
		// this expr and children included in index scan
		partBuilder := sql.NewIndexBuilder(b.idx)
		b.rangeBuildOp(partBuilder, f)
		b.allRanges = append(b.allRanges, partBuilder.Ranges(b.ctx)...)
		return
	}

	switch f := f.(type) {
	case *iScanAnd:
		// AND children can be partitioned
		for _, or := range f.orChildren {
			b.buildRangeCollection(or)
		}
		for _, leaf := range f.leaves() {
			b.buildRangeCollection(leaf)
		}
	default:
		// OR / leaf node excluded from index scan
		e, ok := b.idToExpr[f.Id()]
		if !ok {
			panic("all ids should have corresponding expr in map")
		}
		b.leftover = append(b.leftover, e)
	}
}

func (b *indexScanRangeBuilder) rangeBuildOp(bb *sql.IndexBuilder, f indexFilter) {
	switch f := f.(type) {
	case *iScanAnd:
		b.rangeBuildAnd(bb, f)
	case *iScanOr:
		b.rangeBuildOr(bb, f)
	case *iScanLeaf:
		b.rangeBuildLeaf(bb, f)
	default:
		panic(fmt.Sprintf("unknown indexFilter type: %T", f))
	}
	return
}

func (b *indexScanRangeBuilder) rangeBuildAnd(bb *sql.IndexBuilder, f *iScanAnd) {
	var orRanges sql.RangeCollection
	for _, or := range f.orChildren {
		// separate range builder for each, before UNIONing
		partBuilder := sql.NewIndexBuilder(b.idx)
		b.rangeBuildOp(partBuilder, or)

		orRanges = append(orRanges, partBuilder.Ranges(b.ctx)...)
	}

	var err error
	b.allRanges, err = b.allRanges.Intersect(orRanges)
	if err != nil {
		// todo errors
	}

	partBuilder := sql.NewIndexBuilder(b.idx)

	for _, leaf := range f.leaves() {
		b.rangeBuildOp(partBuilder, leaf)
	}

	b.allRanges = append(b.allRanges, partBuilder.Ranges(b.ctx)...)

	return
}

func (b *indexScanRangeBuilder) rangeBuildOr(bb *sql.IndexBuilder, e *iScanOr) {
	//todo union the or ranges
}

func (b *indexScanRangeBuilder) rangeBuildLeaf(bb *sql.IndexBuilder, f *iScanLeaf) {
	switch f.Op() {
	case indexScanOpEq:
		bb.Equals(b.ctx, strings.ToLower(f.gf.Name()), f.value)
	case indexScanOpGt:
		bb.GreaterThan(b.ctx, strings.ToLower(f.gf.Name()), f.value)
	case indexScanOpGte:
		bb.GreaterOrEqual(b.ctx, strings.ToLower(f.gf.Name()), f.value)
	case indexScanOpLt:
		bb.LessThan(b.ctx, strings.ToLower(f.gf.Name()), f.value)
	case indexScanOpLte:
		bb.LessOrEqual(b.ctx, strings.ToLower(f.gf.Name()), f.value)
	case indexScanOpIsNotNull:
		bb.IsNotNull(b.ctx, strings.ToLower(f.gf.Name()))
	case indexScanOpIsNull:
		bb.IsNull(b.ctx, strings.ToLower(f.gf.Name()))
	default:
		panic(fmt.Sprintf("unknown indexScanOp: %d", f.Op()))
	}
}

// indexFilter decomposes filter conjunction into a format
// amenable for checking index prefix alignment
type indexFilter interface {
	Op() indexScanOp
	Id() indexScanId
}

type iScanLeaf struct {
	op    indexScanOp
	id    indexScanId
	gf    *expression.GetField
	value interface{}
}

func (l *iScanLeaf) Id() indexScanId {
	return l.id
}

func (l *iScanLeaf) Op() indexScanOp {
	return l.op
}

type iScanOr struct {
	id       indexScanId
	children []*iScanAnd
}

func (o *iScanOr) Id() indexScanId {
	return o.id
}

func (o *iScanOr) Op() indexScanOp {
	return indexScanOpOr
}

func newIScanAnd(id indexScanId) *iScanAnd {
	return &iScanAnd{
		id: id,
	}
}

type iScanAnd struct {
	id           indexScanId
	leafChildren map[string][]*iScanLeaf
	orChildren   []indexFilter
	cnt          int
}

func (a *iScanAnd) Op() indexScanOp {
	return indexScanOpAnd
}

func (a *iScanAnd) Id() indexScanId {
	return a.id
}

func (a *iScanAnd) newLeaf(l *iScanLeaf) {
	if a.leafChildren == nil {
		a.leafChildren = make(map[string][]*iScanLeaf)
	}
	a.leafChildren[strings.ToLower(l.gf.Name())] = append(a.leafChildren[strings.ToLower(l.gf.Name())], l)
}

// leaves returns a list of this nodes leaf filters, sorted by id
func (a *iScanAnd) leaves() []*iScanLeaf {
	var ret []*iScanLeaf
	for _, colLeaves := range a.leafChildren {
		for _, leaf := range colLeaves {
			ret = append(ret, leaf)
		}
	}
	sort.SliceStable(ret, func(i, j int) bool {
		return ret[i].id < ret[j].id
	})
	return ret
}

func (a *iScanAnd) childCnt() int {
	if a.cnt > 0 {
		return a.cnt
	}
	cnt := len(a.orChildren)
	for _, leaves := range a.leafChildren {
		cnt += len(leaves)
	}
	a.cnt = cnt
	return a.cnt
}

func formatIndexFilter(f indexFilter) string {
	b := &strings.Builder{}
	formatIndexFilterRec(b, 0, f)
	return b.String()
}

func formatIndexFilterRec(b *strings.Builder, nesting int, f indexFilter) {
	if f == nil {
		return
	}
	switch f := f.(type) {
	case *iScanAnd:
		for i := 0; i < nesting; i++ {
			b.WriteString("  ")
		}
		fmt.Fprintf(b, "(%d: and", f.Id())
		for _, leaf := range f.leaves() {
			fmt.Fprintf(b, "\n")
			formatIndexFilterRec(b, nesting+1, leaf)
		}
		for _, or := range f.orChildren {
			fmt.Fprintf(b, "\n")
			formatIndexFilterRec(b, nesting+1, or)
		}

		fmt.Fprintf(b, ")")

	case *iScanOr:
		for i := 0; i < nesting; i++ {
			b.WriteString("  ")
		}
		fmt.Fprintf(b, "(%d: or", f.Id())

		for _, c := range f.children {
			fmt.Fprintf(b, "\n")
			formatIndexFilterRec(b, nesting+1, c)
		}
		fmt.Fprintf(b, ")")

	case *iScanLeaf:
		for i := 0; i < nesting; i++ {
			b.WriteString("  ")
		}
		switch f.Op() {
		case indexScanOpIsNull, indexScanOpIsNotNull:
			fmt.Fprintf(b, "(%d: %s %s)", f.Id(), f.gf, f.Op())
		default:
			fmt.Fprintf(b, "(%d: %s %s %v)", f.Id(), f.gf, f.Op(), f.value)
		}

	default:
		panic(fmt.Sprintf("unknown indexFilter type :%T", f))
	}
}

type indexScanId uint16

func ordinalsForStat(stat sql.Statistic) map[string]int {
	ret := make(map[string]int)
	for i, c := range stat.Columns() {
		ret[c] = i
	}
	return ret
}

func (c *indexCoster) costIndexScanAnd(filter *iScanAnd, s sql.Statistic, ordinals map[string]int) (sql.Statistic, sql.FastIntSet, error) {

	// first step finds the conjunctions that match index prefix columns.
	// we divide into eqFilters and rangeFilters

	ret := s
	var exact sql.FastIntSet

	if len(filter.orChildren) > 0 {
		for _, or := range filter.orChildren {
			childStat, ok, err := c.costIndexScanOr(or.(*iScanOr), s, ordinals)
			if err != nil {
				return nil, sql.FastIntSet{}, err
			}
			// if valid, INTERSECT
			if ok {
				ret = stats.Intersect(ret, childStat)
				exact.Add(int(or.Id()))
			}
		}
	}

	conj := newConjCollector(ret, ordinals)
	for _, c := range s.Columns() {
		if colFilters, ok := filter.leafChildren[c]; ok {
			for _, f := range colFilters {
				conj.add(f)
			}
		}
	}

	if exact.Len()+conj.applied.Len() == filter.childCnt() {
		// matched all filters
		return conj.stat, sql.NewFastIntSet(int(filter.id)), nil
	}

	return conj.stat, exact.Union(conj.applied), nil

}

func (c *indexCoster) costIndexScanOr(filter *iScanOr, s sql.Statistic, ordinals map[string]int) (sql.Statistic, bool, error) {
	// OR just unions the statistics from each child?
	// if one of the children is invalid, we balk and return false
	// otherwise we union the buckets between the children
	ret := s
	for _, child := range filter.children {
		childStat, ids, err := c.costIndexScanAnd(child, s, ordinals)
		if err != nil {
			return nil, false, err
		}
		if ids.Len() != 1 || !ids.Contains(int(child.Id())) {
			// scan option missed some filters
			return nil, false, nil
		}
		ret = stats.Union(s, childStat)
	}
	return ret, true, nil
}

func (c *indexCoster) costIndexScanLeaf(filter *iScanLeaf, s sql.Statistic, ordinals map[string]int) (sql.Statistic, bool, error) {
	_, ok := ordinals[filter.gf.Name()]
	if !ok {
		return nil, false, nil
	}

	ret := s

	conj := newConjCollector(ret, ordinals)
	conj.add(filter)
	return conj.stat, conj.applied.Len() == 1, nil
}

type indexScanOp uint8

//go:generate stringer -type=indexScanOp -linecomment

const (
	indexScanOpEq        indexScanOp = iota // =
	indexScanOpGt                           // >
	indexScanOpGte                          // >=
	indexScanOpLt                           // <
	indexScanOpLte                          // <=
	indexScanOpAnd                          // &&
	indexScanOpOr                           // ||
	indexScanOpIsNull                       // IS NULL
	indexScanOpIsNotNull                    // IS NOT NULL
)

func newLeaf(id indexScanId, e sql.Expression) (*iScanLeaf, bool) {
	var op indexScanOp
	var left sql.Expression
	var right sql.Expression
	switch e := e.(type) {
	case *expression.Equals:
		op = indexScanOpEq
		right = e.Right()
		left = e.Left()
	case *expression.LessThan:
		left = e.Left()
		right = e.Right()
		op = indexScanOpLt
	case *expression.GreaterThanOrEqual:
		left = e.Left()
		right = e.Right()
		op = indexScanOpGte
	case *expression.GreaterThan:
		left = e.Left()
		right = e.Right()
		op = indexScanOpGt
	case *expression.LessThanOrEqual:
		left = e.Left()
		right = e.Right()
		op = indexScanOpLte
	case *expression.IsNull:
		left = e.Child
		op = indexScanOpIsNull
		// todo not null

	default:
		return nil, false
	}

	if _, ok := left.(*expression.GetField); !ok {
		left, right = right, left
	}

	gf, ok := left.(*expression.GetField)
	if !ok {
		return nil, false
	}

	if op == indexScanOpIsNull {
		return &iScanLeaf{id: id, gf: gf, op: op}, true
	}

	if !isEvaluable(right) {
		return nil, false
	}

	value, err := right.Eval(nil, nil)
	if err != nil {
		return nil, false
	}

	return &iScanLeaf{id: id, gf: gf, op: op, value: value}, true
}

func uniformDistStatistics(ctx *sql.Context, rt *plan.ResolvedTable) (map[sql.StatQualifier]sql.Statistic, error) {
	iat := rt.Table.(sql.IndexAddressableTable)
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, nil
	}

	var rowCount uint64
	var avgSize uint64
	if st, ok := rt.Table.(sql.StatisticsTable); ok {
		rowCount, _, err = st.RowCount(ctx)
		if err != nil {
			return nil, err

		}
		dataSize, err := st.DataLength(ctx)
		if err != nil {
			return nil, err
		}
		avgSize = dataSize / rowCount
	}

	ret := make(map[sql.StatQualifier]sql.Statistic)

	for _, idx := range indexes {
		// fill in dummy values if no stats
		tablePrefix := fmt.Sprintf("%s.", iat.Name())

		distinctCount := rowCount
		nullCount := uint64(0)
		if !idx.IsUnique() {
			distinctCount = uint64(float64(distinctCount) * .90)
			nullCount = uint64(float64(distinctCount) * .10)
		}

		var cols []string
		var types []sql.Type
		for _, exp := range idx.ColumnExpressionTypes() {
			cols = append(cols, strings.TrimPrefix(exp.Expression, tablePrefix))
			types = append(types, exp.Type)
		}

		qual := sql.NewStatQualifier(strings.ToLower(rt.Database().Name()), strings.ToLower(rt.Name()), strings.ToLower(idx.ID()))
		newStat := stats.NewStatistic(rowCount, distinctCount, nullCount, avgSize, time.Now(), qual, cols, types, nil)
		ret[qual] = newStat
	}
	return ret, nil
}

func newConjCollector(s sql.Statistic, ordinals map[string]int) *conjCollector {
	return &conjCollector{
		stat:     s,
		ordinals: ordinals,
		eqVals:   make([]interface{}, len(ordinals)),
	}
}

// conjCollector is used to stack and track changes to
// an index histogram for a list of conjugate filters
type conjCollector struct {
	stat          sql.Statistic
	ordinals      map[string]int
	missingPrefix sql.ColumnId
	constant      sql.ColSet
	eqVals        []interface{}
	applied       sql.FastIntSet
	isFalse       bool
}

func (c *conjCollector) add(f *iScanLeaf) {
	c.applied.Add(int(f.Id()))
	switch f.Op() {
	case indexScanOpEq:
		c.addEq(f.gf.Name(), f.value)
	case indexScanOpGt:
		c.addGt(f.gf.Name(), f.value)
	case indexScanOpLt:
		c.addLt(f.gf.Name(), f.value)
	}
}

func (c *conjCollector) addEq(col string, val interface{}) {
	// make constant
	ord := sql.ColumnId(c.ordinals[col])
	if c.constant.Contains(ord) {
		if c.eqVals[ord] != val {
			// FALSE filter
			c.isFalse = true
			return
		}
		return
	}

	c.constant.Add(ord)
	c.eqVals[ord] = val

	if ord == c.missingPrefix {
		// we are interested in the cases where the index prefix
		// key is extended
		if int(ord) == len(c.eqVals)-1 {
			// full prefix
			c.missingPrefix++
		} else {
			// extended prefix
			// find new and truncate
			_, hasNext := c.constant.Next(c.missingPrefix + 1)
			if !hasNext {
				c.missingPrefix++
			} else {
				nextFilled, _ := c.constant.Next(c.missingPrefix + 2)
				// convert from bit position to index is -1
				// convert from next filled to first missing is -1
				c.missingPrefix = nextFilled - 2
			}
		}

		// truncate buckets
		c.stat = stats.PrefixKey(c.stat, c.eqVals[:ord+1])
	}
}

func (c *conjCollector) addGt(col string, val interface{}) {
	ord := c.ordinals[col]
	c.cmpFirstCol(indexScanOpGt, val)
	c.truncateMcvs(ord, indexScanOpGt, val)
}

func (c *conjCollector) addLt(col string, val interface{}) {
	ord := c.ordinals[col]
	c.cmpFirstCol(indexScanOpLt, val)
	c.truncateMcvs(ord, indexScanOpGt, val)
}

// cmpFirstCol checks whether we should try to range truncate the first
// column in the index
func (c *conjCollector) cmpFirstCol(op indexScanOp, val interface{}) {
	// check if first col already constant
	// otherwise attempt to truncate histogram
	if c.constant.Contains(1) {
		return
	}
	switch op {
	case indexScanOpGt:
		c.stat = stats.PrefixGt(c.stat, val)
	case indexScanOpGte:
		c.stat = stats.PrefixGte(c.stat, val)
	case indexScanOpLt:
		c.stat = stats.PrefixLt(c.stat, val)
	case indexScanOpLte:
		c.stat = stats.PrefixLte(c.stat, val)
	case indexScanOpIsNull:
		c.stat = stats.PrefixIsNull(c.stat, val)
	case indexScanOpIsNotNull:
		c.stat = stats.PrefixIsNotNull(c.stat, val)
	}
}

func (c *conjCollector) truncateMcvs(i int, op indexScanOp, val interface{}) {
	switch op {
	case indexScanOpGt:
		c.stat = stats.McvIndexGt(c.stat, i, val)
	case indexScanOpGte:
		c.stat = stats.McvIndexGte(c.stat, i, val)
	case indexScanOpLt:
		c.stat = stats.McvIndexLt(c.stat, i, val)
	case indexScanOpLte:
		c.stat = stats.McvIndexLte(c.stat, i, val)
	case indexScanOpIsNull:
		c.stat = stats.McvIndexIsNull(c.stat, i, val)
	case indexScanOpIsNotNull:
		c.stat = stats.McvIndexIsNotNull(c.stat, i, val)
	}
}
