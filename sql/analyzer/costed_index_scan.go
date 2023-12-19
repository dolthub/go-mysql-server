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
	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
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
func costedIndexScans(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		filter, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}

		var rt sql.TableNode
		var aliasName string
		switch n := filter.Child.(type) {
		case *plan.ResolvedTable:
			rt = n
		case *plan.TableAlias:
			rt, _ = n.Child.(sql.TableNode)
			aliasName = n.Name()
		}
		if rt == nil {
			return n, transform.SameTree, nil
		}

		if is, ok := rt.UnderlyingTable().(sql.IndexSearchableTable); ok && is.SkipIndexCosting() {
			lookup, err := is.LookupForExpressions(ctx, expression.SplitConjunction(filter.Expression))
			if err != nil {
				return n, transform.SameTree, err
			}
			if lookup.IsEmpty() {
				return n, transform.SameTree, nil
			}
			ret, err := plan.NewStaticIndexedAccessForTableNode(rt, lookup)
			if err != nil {
				return n, transform.SameTree, err
			}
			return plan.NewFilter(filter.Expression, ret), transform.NewTree, nil
		} else if iat, ok := rt.UnderlyingTable().(sql.IndexAddressableTable); ok {
			indexes, err := iat.GetIndexes(ctx)
			if err != nil {
				return n, transform.SameTree, err
			}
			ita, _, filters, err := getCostedIndexScan(ctx, a.Catalog, rt, indexes, expression.SplitConjunction(filter.Expression))
			if err != nil || ita == nil {
				return n, transform.SameTree, err
			}
			var ret sql.Node = ita
			if aliasName != "" {
				ret = plan.NewTableAlias(aliasName, ret)
			}
			// excluded from tree + not included in index scan => filter above scan
			if len(filters) > 0 {
				ret = plan.NewFilter(expression.JoinAnd(filters...), ret)
			}
			return ret, transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
}

func getCostedIndexScan(ctx *sql.Context, statsProv sql.StatsProvider, rt sql.TableNode, indexes []sql.Index, filters []sql.Expression) (*plan.IndexedTableAccess, sql.Statistic, []sql.Expression, error) {
	statistics, err := statsProv.GetTableStats(ctx, strings.ToLower(rt.Database().Name()), strings.ToLower(rt.Name()))
	if err != nil {
		return nil, nil, nil, err
	}

	qualToStat := make(map[sql.StatQualifier]sql.Statistic)
	for _, stat := range statistics {
		if prev, ok := qualToStat[stat.Qualifier()]; !ok || ok && len(stat.Columns()) > len(prev.Columns()) {
			qualToStat[stat.Qualifier()] = stat
		}
	}

	// flatten expression tree for costing
	c := newIndexCoster(rt.Name())
	root, leftover, imprecise := c.flatten(expression.JoinAnd(filters...))
	if root == nil {
		return nil, nil, nil, err
	}

	iat, ok := rt.UnderlyingTable().(sql.IndexAddressableTable)
	if !ok {
		return nil, nil, nil, err
	}

	// run each index through coster, save the cheapest
	var dbName string
	if dbTab, ok := rt.UnderlyingTable().(sql.Databaseable); ok {
		dbName = strings.ToLower(dbTab.Database())
	}
	tableName := strings.ToLower(rt.UnderlyingTable().Name())

	if len(qualToStat) > 0 {
		// don't mix and match real and default stats
		for _, idx := range indexes {
			qual := sql.NewStatQualifier(dbName, tableName, strings.ToLower(idx.ID()))
			_, ok := qualToStat[qual]
			if !ok {
				qualToStat = nil
				break
			}
		}
	}

	for _, idx := range indexes {
		qual := sql.NewStatQualifier(dbName, tableName, strings.ToLower(idx.ID()))
		stat, ok := qualToStat[qual]
		if !ok {
			stat, err = uniformDistStatisticsForIndex(ctx, iat, idx)
		}
		err := c.cost(root, stat, idx)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	if c.bestStat == nil || c.bestFilters.Empty() {
		return nil, nil, nil, err
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
		return nil, nil, nil, fmt.Errorf("tried building indexScan with unknown statistic index: %s", targetId)
	}

	// separate |include| and |leftover| filters
	b := newIndexScanRangeBuilder(ctx, idx, c.bestFilters, imprecise, c.idToExpr)
	if leftover != nil {
		b.leftover = append(b.leftover, leftover)
	}
	ranges, err := b.buildRangeCollection(root)
	if err != nil {
		return nil, nil, nil, err
	}

	var emptyLookup bool
	if len(ranges) == 0 {
		emptyLookup = true
	} else if len(ranges) == 1 {
		emptyLookup, err = ranges[0].IsEmpty()
		if err != nil {
			return nil, nil, nil, err
		}
		allRange := true
		for i, r := range ranges[0] {
			_, uok := r.UpperBound.(sql.AboveAll)
			_, lok := r.LowerBound.(sql.BelowNull)
			allRange = allRange && uok && lok
			if i == 0 && allRange {
				// no prefix restriction
				return nil, nil, nil, err
			}
		}
		if allRange {
			return nil, nil, nil, err
		}
	}

	if !idx.CanSupport(ranges...) {
		return nil, nil, nil, err
	}

	if idx.IsSpatial() && len(ranges) > 1 {
		// spatials don't support disjunct ranges
		return nil, nil, nil, err
	}

	// create ranges, lookup, ITA for best indexScan
	// TODO: use FALSE filters to replace empty tables
	lookup := sql.NewIndexLookup(idx, ranges, false, emptyLookup, idx.IsSpatial(), false)

	var ret *plan.IndexedTableAccess
	if idx.IsFullText() {
		id, _ := c.bestFilters.Next(1)
		ma := c.idToExpr[indexScanId(id)]
		matchAgainst, ok := ma.(*expression.MatchAgainst)
		if !ok {
			return nil, nil, nil, fmt.Errorf("Full-Text index found in filter with unknown expression: %T", ma)
		}
		if matchAgainst.KeyCols.Type == fulltext.KeyType_None {
			return nil, nil, nil, err
		}
		ret = plan.NewStaticIndexedAccessForFullTextTable(rt, lookup, &rowexec.FulltextFilterTable{
			MatchAgainst: matchAgainst,
			Table:        rt,
		})
	} else {
		ret, err = plan.NewStaticIndexedAccessForTableNode(rt, lookup)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var retFilters []sql.Expression
	if !iat.PreciseMatch() {
		// cannot drop any filters
		retFilters = filters
	} else if len(b.leftover) > 0 {
		// excluded from tree + not included in index scan => filter above scan
		retFilters = b.leftover
	}

	return ret, c.bestStat, retFilters, nil
}

func addIndexScans(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		filter, ok := e.(*memo.Filter)
		if !ok {
			return nil
		}

		var rt sql.TableNode
		var aliasName string
		switch n := filter.Child.First.(type) {
		case *memo.TableScan:
			rt = n.Table.(sql.TableNode)
		case *memo.TableAlias:
			rt, ok = n.Table.Child.(sql.TableNode)
			if !ok {
				return nil
			}
			aliasName = n.Name()
		default:
			return nil
		}

		indexes := filter.Child.First.(memo.SourceRel).Indexes()

		if is, ok := rt.UnderlyingTable().(sql.IndexSearchableTable); ok && is.SkipIndexCosting() {
			lookup, err := is.LookupForExpressions(m.Ctx, filter.Filters)
			if err != nil {
				m.HandleErr(err)
			}
			if lookup.IsEmpty() {
				return nil
			}
			ret, err := plan.NewStaticIndexedAccessForTableNode(rt, lookup)
			if err != nil {
				m.HandleErr(err)

			}
			// TODO add ITA to filter group
			// todo memoize ITA
			// we explicitly put ITA as child of filter group for this shortcut
			var idx *memo.Index
			for _, i := range indexes {
				if i.SqlIdx().ID() == lookup.Index.ID() {
					idx = i
					break
				}
			}
			itaGroup := m.MemoizeIndexScan(nil, ret, aliasName, idx)
			m.MemoizeFilter(filter.Group(), itaGroup, filter.Filters)
		} else {
			sqlIndexes := make([]sql.Index, len(indexes))
			for i, idx := range indexes {
				sqlIndexes[i] = idx.SqlIdx()
			}
			ita, stat, filters, err := getCostedIndexScan(m.Ctx, m.StatsProvider(), rt, sqlIndexes, filter.Filters)
			if err != nil {
				m.HandleErr(err)
			}
			if ita != nil {
				var idx *memo.Index
				for _, i := range indexes {
					if ita.Index().ID() == i.SqlIdx().ID() {
						idx = i
						break
					}
				}
				var itaGrp *memo.ExprGroup
				if len(filters) > 0 {
					itaGrp = m.MemoizeIndexScan(nil, ita, aliasName, idx)
					m.MemoizeFilter(filter.Group(), itaGrp, filters)
				} else {
					itaGrp = m.MemoizeIndexScan(filter.Group(), ita, aliasName, idx)
				}

				// todo: we should always interpolate the estimated row count even
				// if we are missing index statistics
				if stat.RowCount() > 0 {
					if stat.Histogram().IsEmpty() {
						// if we don't have stats, set arbitrarily low non-zero row count
						// to prefer indexScan over filter option
						itaGrp.RelProps.SetStats(stat.WithRowCount(1))
					} else {
						itaGrp.RelProps.SetStats(stat)
					}
				}
				if stat.FuncDeps().HasMax1Row() {
					itaGrp.RelProps.SetStats(stat.WithRowCount(1))
				}
			}
		}
		return nil
	})
}

func newIndexCoster(underlyingName string) *indexCoster {
	return &indexCoster{
		i:              1,
		idToExpr:       make(map[indexScanId]sql.Expression),
		underlyingName: underlyingName,
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
	// bestConstant are the constant best filters
	bestConstant   sql.FastIntSet
	underlyingName string
}

// cost tries to build the lowest cardinality index scan for an expression
// tree rooted at |f| on the index |idx| whose statistics are represented by |stat|.
func (c *indexCoster) cost(f indexFilter, stat sql.Statistic, idx sql.Index) error {
	ordinals := ordinalsForStat(stat)

	newStat := stat
	var filters sql.FastIntSet
	var err error
	var ok bool

	switch f := f.(type) {
	case *iScanAnd:
		newStat, filters, err = c.costIndexScanAnd(f, stat, ordinals, idx)
		if err != nil {
			return err
		}

	case *iScanOr:
		newStat, ok, err = c.costIndexScanOr(f, stat, ordinals, idx)
		if err != nil {
			return err
		}
		if ok {
			filters.Add(int(f.id))
		}
	case *iScanLeaf:
		newStat, ok, err = c.costIndexScanLeaf(f, stat, ordinals, idx)
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
	if s == nil || filters.Len() == 0 {
		return
	}

	var update bool
	defer func() {
		if update {
			c.bestStat = s
			c.bestFilters = filters
		}
	}()

	if c.bestStat == nil || s.RowCount() < c.bestStat.RowCount() {
		update = true
		return
	} else if c.bestStat.FuncDeps().HasMax1Row() {
		return
	} else if s.RowCount() == c.bestStat.RowCount() {
		// hand rules when stats don't exist or match exactly
		cmp := s.FuncDeps()
		best := c.bestStat.FuncDeps()
		if cmp.HasMax1Row() {
			update = true
			return
		}

		bestKey, bok := best.StrictKey()
		cmpKey, cok := cmp.StrictKey()
		if cok && !bok {
			// prefer unique key
			update = true
			return
		} else if bok && !cok {
			// prefer unique key
			return
		} else if cok && bok {
			// prefer shorter strict key
			if cmpKey.Len() < bestKey.Len() {
				update = true
				return
			}
		}

		// the one below is sketchy, this is why we need costing
		// prefer unique key even if non-unique has more constants
		_, bestHasLax := best.LaxKey()
		_, cmpHasLax := cmp.LaxKey()
		if cmp.Constants().Len() > best.Constants().Len() {
			if bestHasLax && !cmpHasLax {
				// keep unique key
				return
			}
			update = true
			return
		} else if cmp.Constants().Len() < best.Constants().Len() {
			if cmpHasLax && !bestHasLax {
				// keep unique key
				update = true
			}
			return
		}

		if filters.Len() > c.bestFilters.Len() {
			update = true
			return
		}

		if s.ColSet().Len()-filters.Len() < c.bestStat.ColSet().Len()-c.bestFilters.Len() {
			// prefer 1 range filter over 1 column index (1 - 1 = 0)
			// vs. 1 range filter over 2 column index (2 - 1 = 1)
			update = true
			return
		}

		{
			// if no unique keys, prefer equality over ranges
			bestConst, bestIsNull := c.getConstAndNullFilters(c.bestFilters)
			cmpConst, cmpIsNull := c.getConstAndNullFilters(c.bestFilters)
			if cmpConst.Len() > bestConst.Len() {
				update = true
				return
			}
			if cmpIsNull.Len() > bestIsNull.Len() {
				update = true
				return
			}
		}

		{
			if strings.EqualFold(s.Qualifier().Index(), "primary") {
				update = true
				return
			} else if strings.EqualFold(c.bestStat.Qualifier().Index(), "primary") {
				return
			}
			if strings.Compare(s.Qualifier().Index(), c.bestStat.Qualifier().Index()) < 0 {
				// if they are still equal, use index name to make deterministic
				update = true
				return
			}
		}
	}
}

func (c *indexCoster) getConstAndNullFilters(filters sql.FastIntSet) (sql.FastIntSet, sql.FastIntSet) {
	var isConst sql.FastIntSet
	var isNull sql.FastIntSet
	for i, hasNext := filters.Next(0); hasNext; i, hasNext = filters.Next(i + 1) {
		e := c.idToExpr[indexScanId(i)]
		switch e.(type) {
		case *expression.Equals:
			isConst.Add(i)
		case *expression.IsNull:
			isNull.Add(i)
		case *expression.NullSafeEquals:
			isConst.Add(i)
			isNull.Add(i)
		}
	}
	return isConst, isNull
}

// flatten converts a filter into a tree of indexFilter, a format designed
// to make costing index scans easier. We return the root of the new tree
// and a conjunction of filters that cannot be pushed into index scans.
func (c *indexCoster) flatten(e sql.Expression) (indexFilter, sql.Expression, sql.FastIntSet) {
	switch e := e.(type) {
	case *expression.And:
		c.idToExpr[c.i] = e
		newAnd := &iScanAnd{id: c.i}
		c.i++
		invalid, imprecise := c.flattenAnd(e, newAnd)
		var leftovers []sql.Expression
		for i, hasMore := invalid.Next(1); hasMore; i, hasMore = invalid.Next(i + 1) {
			f, ok := c.idToExpr[indexScanId(i)]
			if !ok {
				panic("todo filter map not working")
			}
			leftovers = append(leftovers, f)
		}
		return newAnd, expression.JoinAnd(leftovers...), imprecise

	case *expression.Or:
		c.idToExpr[c.i] = e
		newOr := &iScanOr{id: c.i}
		c.i++
		valid, imp := c.flattenOr(e, newOr)
		if !valid {
			return nil, e, sql.FastIntSet{}
		}
		var imprecise sql.FastIntSet
		if imp {
			imprecise.Add(int(newOr.id))
		}
		return newOr, nil, imprecise

	default:
		c.idToExpr[c.i] = e
		leaf, ok := newLeaf(c.i, e, c.underlyingName)
		c.i++
		if !ok {
			return nil, e, sql.FastIntSet{}
		}
		var imprecise sql.FastIntSet
		if !expression.PreciseComparison(e) {
			imprecise.Add(int(leaf.id))
		}
		return leaf, nil, imprecise
	}
}

// flattenAnd return two bitsets to indicate invalid index filter ids, and imprecise filter ids
func (c *indexCoster) flattenAnd(e *expression.And, and *iScanAnd) (sql.FastIntSet, sql.FastIntSet) {
	var invalid sql.FastIntSet
	var imprecise sql.FastIntSet
	for _, e := range e.Children() {
		switch e := e.(type) {
		case *expression.And:
			c.idToExpr[c.i] = e
			c.i++
			inv, imp := c.flattenAnd(e, and)
			invalid = invalid.Union(inv)
			imprecise = invalid.Union(imp)
		case *expression.Or:
			c.idToExpr[c.i] = e
			newOr := &iScanOr{id: c.i}
			c.i++
			valid, imp := c.flattenOr(e, newOr)
			if !valid {
				// this or is invalid
				invalid.Add(int(newOr.Id()))
			} else {
				and.orChildren = append(and.orChildren, newOr)
				if imp {
					imprecise.Add(int(newOr.id))
				}
			}
		default:
			c.idToExpr[c.i] = e
			leaf, ok := newLeaf(c.i, e, c.underlyingName)
			if !ok {
				invalid.Add(int(c.i))
			} else {
				and.newLeaf(leaf)
				if !expression.PreciseComparison(e) {
					imprecise.Add(int(leaf.id))
				}
			}
			// keep a ref to the invalid |e|
			c.i++
		}
	}
	return invalid, imprecise
}

func (c *indexCoster) flattenOr(e *expression.Or, or *iScanOr) (bool, bool) {
	var imprecise bool
	for _, e := range e.Children() {
		switch e := e.(type) {
		case *expression.And:
			c.idToExpr[c.i] = e
			newAnd := &iScanAnd{id: c.i}
			c.i++
			inv, imp := c.flattenAnd(e, newAnd)
			if !inv.Empty() {
				return false, false
			}
			or.children = append(or.children, newAnd)
			imprecise = imprecise || !imp.Empty()
		case *expression.Or:
			c.idToExpr[c.i] = e
			c.i++
			ok, imp := c.flattenOr(e, or)
			if !ok {
				return false, false
			}
			imprecise = imprecise || imp
		default:
			c.idToExpr[c.i] = e
			leaf, ok := newLeaf(c.i, e, c.underlyingName)
			if !ok {
				return false, false
			} else {
				c.i++
				or.children = append(or.children, leaf)
				if !expression.PreciseComparison(e) {
					imprecise = true
				}
			}
		}
	}
	return true, imprecise
}

func newIndexScanRangeBuilder(ctx *sql.Context, idx sql.Index, include, imprecise sql.FastIntSet, idToExpr map[indexScanId]sql.Expression) *indexScanRangeBuilder {
	return &indexScanRangeBuilder{
		ctx:       ctx,
		idx:       idx,
		include:   include,
		imprecise: imprecise,
		idToExpr:  idToExpr,
	}
}

type indexScanRangeBuilder struct {
	ctx       *sql.Context
	idx       sql.Index
	include   sql.FastIntSet
	imprecise sql.FastIntSet
	idToExpr  map[indexScanId]sql.Expression
	conjIb    *sql.IndexBuilder
	allRanges sql.RangeCollection
	leftover  []sql.Expression
	tableName string
}

// buildRangeCollection converts our representation of the best index scan
// into the format that represents an index lookup, a list of sql.Range.
func (b *indexScanRangeBuilder) buildRangeCollection(f indexFilter) (sql.RangeCollection, error) {
	inScan := b.include.Contains(int(f.Id()))

	var ranges sql.RangeCollection
	var err error
	switch f := f.(type) {
	case *iScanAnd:
		ranges, err = b.rangeBuildAnd(f, inScan)
	case *iScanOr:
		ranges, err = b.rangeBuildOr(f, inScan)
	case *iScanLeaf:
		ranges, err = b.rangeBuildLeaf(f, inScan)
	default:
		return nil, fmt.Errorf("unknown indexFilter type: %T", f)
	}

	if err != nil {
		return nil, err
	}
	return sql.RemoveOverlappingRanges(ranges...)
}

func (b *indexScanRangeBuilder) Ranges() (sql.RangeCollection, error) {
	return sql.RemoveOverlappingRanges(b.allRanges...)
}

func (b *indexScanRangeBuilder) rangeBuildAnd(f *iScanAnd, inScan bool) (sql.RangeCollection, error) {
	// no leftover check for AND, it's children may be included in scan
	inScan = inScan || b.include.Contains(int(f.Id()))

	var ret sql.RangeCollection
	for _, or := range f.orChildren {
		// separate range builder for each, before UNIONing
		ranges, err := b.rangeBuildOr(or.(*iScanOr), inScan)
		if err != nil {
			return nil, err
		}
		if ranges == nil {
			continue
		}
		if ret == nil {
			ret = ranges
			continue
		}
		ret, err = ret.Intersect(ranges)
		if err != nil {
			return nil, err
		}
	}

	partBuilder := sql.NewIndexBuilder(b.idx)
	for _, leaf := range f.leaves() {
		switch leaf.Op() {
		case indexScanOpSpatialEq:
			ranges, err := b.rangeBuildSpatialLeaf(leaf, inScan)
			if err != nil {
				return nil, err
			}
			if ranges != nil {
				ret, err = ret.Intersect(partBuilder.Ranges(b.ctx))
				if err != nil {
					return nil, err
				}
			}
		case indexScanOpFulltextEq:
			ranges, err := b.rangeBuildFulltextLeaf(leaf, inScan)
			if err != nil {
				return nil, err
			}
			if ranges != nil {
				ret, err = ret.Intersect(partBuilder.Ranges(b.ctx))
				if err != nil {
					return nil, err
				}
			}
		default:
			b.rangeBuildDefaultLeaf(partBuilder, leaf, inScan)
		}
	}

	if _, err := partBuilder.Build(b.ctx); err != nil {
		return nil, err
	}

	if ret == nil {
		return partBuilder.Ranges(b.ctx), nil
	}

	ret, err := ret.Intersect(partBuilder.Ranges(b.ctx))
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (b *indexScanRangeBuilder) rangeBuildOr(f *iScanOr, inScan bool) (sql.RangeCollection, error) {
	inScan = !b.markLeftover(f, inScan)
	if !inScan {
		return nil, nil
	}

	// imprecise filters cannot be removed
	b.markImprecise(f)

	//todo union the or ranges
	var ret sql.RangeCollection
	for _, c := range f.children {
		var ranges sql.RangeCollection
		var err error
		switch c := c.(type) {
		case *iScanAnd:
			ranges, err = b.rangeBuildAnd(c, inScan)
		case *iScanLeaf:
			ranges, err = b.rangeBuildLeaf(c, inScan)
		default:
			return nil, fmt.Errorf("invalid *iScanOr child: %T", c)
		}
		if err != nil {
			return nil, err
		}
		ret = append(ret, ranges...)
	}
	return ret, nil
}

func (b *indexScanRangeBuilder) rangeBuildSpatialLeaf(f *iScanLeaf, inScan bool) (sql.RangeCollection, error) {
	inScan = !b.markLeftover(f, inScan)
	if inScan {
		// always mark leftover
		b.leftover = append(b.leftover, b.idToExpr[f.Id()])
	} else {
		return nil, nil
	}

	g, ok := f.litValue.(types.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New()
	}
	minX, minY, maxX, maxY := g.BBox()
	lower := types.Point{X: minX, Y: minY}
	upper := types.Point{X: maxX, Y: maxY}

	return sql.RangeCollection{{{
		LowerBound: sql.Below{Key: lower},
		UpperBound: sql.Above{Key: upper},
		Typ:        f.gf.Type(),
	}}}, nil
}

func (b *indexScanRangeBuilder) rangeBuildFulltextLeaf(f *iScanLeaf, inScan bool) (sql.RangeCollection, error) {
	// fulltext leaf doesn't use ranges
	inScan = !b.markLeftover(f, inScan)
	if inScan {
		// always mark leftover
		b.leftover = append(b.leftover, b.idToExpr[f.Id()])
	} else {
		return nil, nil
	}
	return sql.RangeCollection{{sql.EmptyRangeColumnExpr(f.gf.Type())}}, nil
}

func (b *indexScanRangeBuilder) rangeBuildLeaf(f *iScanLeaf, inScan bool) (sql.RangeCollection, error) {
	switch f.Op() {
	case indexScanOpSpatialEq:
		return b.rangeBuildSpatialLeaf(f, inScan)
	case indexScanOpFulltextEq:
		return b.rangeBuildFulltextLeaf(f, inScan)
	default:
		bb := sql.NewIndexBuilder(b.idx)
		b.rangeBuildDefaultLeaf(bb, f, inScan)
		if _, err := bb.Build(b.ctx); err != nil {
			return nil, err
		}
		return bb.Ranges(b.ctx), nil
	}
}

func (b *indexScanRangeBuilder) rangeBuildDefaultLeaf(bb *sql.IndexBuilder, f *iScanLeaf, inScan bool) {
	inScan = !b.markLeftover(f, inScan)
	if !inScan {
		return
	}

	b.markImprecise(f)

	name := f.normString()
	switch f.Op() {
	case indexScanOpEq:
		bb.Equals(b.ctx, name, f.litValue)
	case indexScanOpNotEq:
		bb.NotEquals(b.ctx, name, f.litValue)
	case indexScanOpInSet:
		bb.Equals(b.ctx, name, f.setValues...)
	case indexScanOpNotInSet:
		for _, v := range f.setValues {
			bb.NotEquals(b.ctx, name, v)
		}
	case indexScanOpGt:
		bb.GreaterThan(b.ctx, name, f.litValue)
	case indexScanOpGte:
		bb.GreaterOrEqual(b.ctx, name, f.litValue)
	case indexScanOpLt:
		bb.LessThan(b.ctx, name, f.litValue)
	case indexScanOpLte:
		bb.LessOrEqual(b.ctx, name, f.litValue)
	case indexScanOpIsNotNull:
		bb.IsNotNull(b.ctx, name)
	case indexScanOpIsNull:
		bb.IsNull(b.ctx, name)
	case indexScanOpNullSafeEq:
		if f.litValue == nil {
			bb.IsNull(b.ctx, name)
		} else {
			bb.Equals(b.ctx, name, f.litValue)
		}
	default:
		panic(fmt.Sprintf("unknown indexScanOp: %d", f.Op()))
	}
}

// markLeftover is used to check if leaf nodes and OR filters are left out
// of the index lookup. We omit this check for AND filters because a portion
// of their children can contribute to the scan.
func (b *indexScanRangeBuilder) markLeftover(f indexFilter, inScan bool) bool {
	if !inScan && !b.include.Contains(int(f.Id())) {
		b.leftover = append(b.leftover, b.idToExpr[f.Id()])
		return true
	}
	return false
}

func (b *indexScanRangeBuilder) markImprecise(f indexFilter) {
	if b.imprecise.Contains(int(f.Id())) {
		b.leftover = append(b.leftover, b.idToExpr[f.Id()])
	}
}

// indexFilter decomposes filter conjunction into a format
// amenable for checking index prefix alignment
type indexFilter interface {
	Op() indexScanOp
	Id() indexScanId
}

type iScanLeaf struct {
	op            indexScanOp
	id            indexScanId
	gf            *expression.GetField
	underlying    string
	litValue      interface{}
	setValues     []interface{}
	fulltextIndex string
}

func (l *iScanLeaf) normString() string {
	if l.underlying != "" {
		return fmt.Sprintf("%s.%s", strings.ToLower(l.underlying), strings.ToLower(l.gf.Name()))
	}
	return strings.ToLower(l.gf.String())
}

func (l *iScanLeaf) Id() indexScanId {
	return l.id
}

func (l *iScanLeaf) Op() indexScanOp {
	return l.op
}

type iScanOr struct {
	id       indexScanId
	children []indexFilter
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
		case indexScanOpInSet, indexScanOpNotInSet:
			var valStrs []string
			for _, v := range f.setValues {
				valStrs = append(valStrs, fmt.Sprintf("%v", v))
			}
			fmt.Fprintf(b, "(%d: %s %s (%s))", f.Id(), f.gf, f.Op(), strings.Join(valStrs, ", "))
		default:
			fmt.Fprintf(b, "(%d: %s %s %v)", f.Id(), f.gf, f.Op(), f.litValue)
		}

	default:
		panic(fmt.Sprintf("unknown indexFilter type :%T", f))
	}
}

type indexScanId uint16

func ordinalsForStat(stat sql.Statistic) map[string]int {
	ret := make(map[string]int)
	for i, c := range stat.Columns() {
		ret[strings.ToLower(c)] = i
	}
	return ret
}

func (c *indexCoster) costIndexScanAnd(filter *iScanAnd, s sql.Statistic, ordinals map[string]int, idx sql.Index) (sql.Statistic, sql.FastIntSet, error) {
	// first step finds the conjunctions that match index prefix columns.
	// we divide into eqFilters and rangeFilters

	ret := s
	var exact sql.FastIntSet

	if len(filter.orChildren) > 0 {
		for _, or := range filter.orChildren {
			childStat, ok, err := c.costIndexScanOr(or.(*iScanOr), s, ordinals, idx)
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

func (c *indexCoster) costIndexScanOr(filter *iScanOr, s sql.Statistic, ordinals map[string]int, idx sql.Index) (sql.Statistic, bool, error) {
	// OR just unions the statistics from each child?
	// if one of the children is invalid, we balk and return false
	// otherwise we union the buckets between the children
	ret := s

	for _, child := range filter.children {
		switch child := child.(type) {
		case *iScanAnd:
			childStat, ids, err := c.costIndexScanAnd(child, s, ordinals, idx)
			if err != nil {
				return nil, false, err
			}
			if ids.Len() != 1 || !ids.Contains(int(child.Id())) {
				// scan option missed some filters
				return nil, false, nil
			}
			ret = stats.Union(s, childStat)

		case *iScanLeaf:
			var ok bool
			childStat, ok, err := c.costIndexScanLeaf(child, s, ordinals, idx)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			ret = stats.Union(s, childStat)

		default:
			return nil, false, fmt.Errorf("invalid *iScanOr child: %T", child)
		}
	}
	return ret, true, nil
}

// indexHasContentHashedFieldForFilter returns true if the given index |idx| has a content-hashed field that is used
// by the given filter |filter|. |ordinals| provides a mapping from filter expression to position in |idx|. Indexes
// with content-hashed fields can only be used for a subset of filter operations.
func indexHasContentHashedFieldForFilter(filter *iScanLeaf, idx sql.Index, ordinals map[string]int) bool {
	// Only unique indexes are currently able to use content-hashed fields
	if !idx.IsUnique() {
		return false
	}

	i := ordinals[filter.gf.Name()]
	columnExpressionType := idx.ColumnExpressionTypes()[i]

	// Only TEXT/BLOB types can currently use content-hashes in indexes
	if !types.IsTextBlob(columnExpressionType.Type) {
		return false
	}

	prefixLength := uint16(0)
	if len(idx.PrefixLengths()) > i {
		prefixLength = idx.PrefixLengths()[i]
	}
	return prefixLength == 0
}

func (c *indexCoster) costIndexScanLeaf(filter *iScanLeaf, s sql.Statistic, ordinals map[string]int, idx sql.Index) (sql.Statistic, bool, error) {
	ord, ok := ordinals[strings.ToLower(filter.gf.Name())]
	if !ok {
		return nil, false, nil
	}

	// indexes with content-hashed fields can be used to test equality or compare with NULL,
	// but can't be used for other comparisons, such as less than or greater than.
	if indexHasContentHashedFieldForFilter(filter, idx, ordinals) {
		switch filter.op {
		case indexScanOpEq, indexScanOpNotEq, indexScanOpNullSafeEq, indexScanOpIsNull, indexScanOpIsNotNull:
		default:
			return nil, false, nil
		}
	}

	switch filter.op {
	case indexScanOpSpatialEq:
		return c.costSpatial(filter, s, ord)
	case indexScanOpFulltextEq:
		return c.costFulltext(filter, s, ord)
	default:
		conj := newConjCollector(s, ordinals)
		conj.add(filter)
		return conj.stat, true, nil
	}
}

func (c *indexCoster) costSpatial(filter *iScanLeaf, s sql.Statistic, ordinal int) (sql.Statistic, bool, error) {
	return s, s.IndexClass() == sql.IndexClassSpatial && ordinal == 0 && filter.litValue != nil, nil
}

func (c *indexCoster) costFulltext(filter *iScanLeaf, s sql.Statistic, ordinal int) (sql.Statistic, bool, error) {
	// check that the filter's index matches the fulltext index
	return s, s.IndexClass() == sql.IndexClassFulltext && s.Qualifier().Index() == filter.fulltextIndex, nil
}

type indexScanOp uint8

//go:generate stringer -type=indexScanOp -linecomment

const (
	indexScanOpEq         indexScanOp = iota // =
	indexScanOpNullSafeEq                    // <=>
	indexScanOpInSet                         // =
	indexScanOpNotInSet                      // !=
	indexScanOpNotEq                         // !=
	indexScanOpGt                            // >
	indexScanOpGte                           // >=
	indexScanOpLt                            // <
	indexScanOpLte                           // <=
	indexScanOpAnd                           // &&
	indexScanOpOr                            // ||
	indexScanOpIsNull                        // IS NULL
	indexScanOpIsNotNull                     // IS NOT NULL
	indexScanOpSpatialEq                     // SpatialEq
	indexScanOpFulltextEq                    // FulltextEq
)

// swap returns the identity op for swapping a comparison's LHS and RHS
func (o indexScanOp) swap() indexScanOp {
	switch o {
	case indexScanOpGt:
		return indexScanOpLt
	case indexScanOpGte:
		return indexScanOpLte
	case indexScanOpLt:
		return indexScanOpGt
	case indexScanOpLte:
		return indexScanOpGte
	default:
		return o
	}
}

func newLeaf(id indexScanId, e sql.Expression, underlying string) (*iScanLeaf, bool) {
	var op indexScanOp
	var left sql.Expression
	var right sql.Expression
	switch e := e.(type) {
	case *expression.NullSafeEquals:
		op = indexScanOpNullSafeEq
		right = e.Right()
		left = e.Left()
	case *expression.Equals:
		op = indexScanOpEq
		right = e.Right()
		left = e.Left()
	case *expression.InTuple:
		op = indexScanOpInSet
		right = e.Right()
		left = e.Left()
	case *expression.HashInTuple:
		op = indexScanOpInSet
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
	case *expression.Not:
		switch e := e.Child.(type) {
		case *expression.IsNull:
			left = e.Child
			op = indexScanOpIsNotNull
		case *expression.Equals:
			left = e.Left()
			right = e.Right()
			op = indexScanOpNotEq
		case *expression.InTuple:
			op = indexScanOpNotInSet
			right = e.Right()
			left = e.Left()
		case *expression.HashInTuple:
			op = indexScanOpNotInSet
			right = e.Right()
			left = e.Left()
		default:
			return nil, false
		}
	case *spatial.Intersects, *spatial.Within, *spatial.STEquals:
		op = indexScanOpSpatialEq
		children := e.Children()
		left = children[0]
		right = children[1]
	case *expression.MatchAgainst:
		op = indexScanOpFulltextEq
		return &iScanLeaf{id: id, op: op, gf: e.Columns[0].(*expression.GetField), underlying: underlying, fulltextIndex: e.GetIndex().ID()}, true
	default:
		return nil, false
	}

	if _, ok := left.(*expression.GetField); !ok {
		left, right = right, left
		op = op.swap()
	}

	gf, ok := left.(*expression.GetField)
	if !ok {
		return nil, false
	}

	if op == indexScanOpIsNull || op == indexScanOpIsNotNull {
		return &iScanLeaf{id: id, gf: gf, op: op, underlying: underlying}, true
	}

	if !isEvaluable(right) {
		return nil, false
	}

	if op == indexScanOpInSet || op == indexScanOpNotInSet {
		tup := right.(expression.Tuple)
		var litSet []interface{}
		for _, lit := range tup {
			value, err := lit.Eval(nil, nil)
			if err != nil {
				return nil, false
			}
			litSet = append(litSet, value)
		}
		return &iScanLeaf{id: id, gf: gf, op: op, setValues: litSet, underlying: underlying}, true
	}

	value, err := right.Eval(nil, nil)
	if err != nil {
		return nil, false
	}

	return &iScanLeaf{id: id, gf: gf, op: op, litValue: value, underlying: underlying}, true
}

const dummyNotUniqueDistinct = .90
const dummyNotUniqueNull = .03

func uniformDistStatisticsForIndex(ctx *sql.Context, iat sql.IndexAddressableTable, idx sql.Index) (sql.Statistic, error) {
	var rowCount uint64
	var avgSize uint64
	if st, ok := iat.(sql.StatisticsTable); ok {
		var err error
		rowCount, _, err = st.RowCount(ctx)
		if err != nil {
			return nil, err

		}
		if rowCount > 0 {
			dataSize, err := st.DataLength(ctx)
			if err != nil {
				return nil, err
			}
			avgSize = dataSize / rowCount
		}
	}

	var dbName string
	if dbTable, ok := iat.(sql.Databaseable); ok {
		dbName = strings.ToLower(dbTable.Database())
	}
	tableName := strings.ToLower(iat.Name())

	var sch sql.Schema
	if pkt, ok := iat.(sql.PrimaryKeyTable); ok {
		sch = pkt.PrimaryKeySchema().Schema
	} else {
		sch = iat.Schema()
	}

	return newUniformDistStatistic(dbName, tableName, sch, idx, rowCount, avgSize)
}

func indexFds(tableName string, sch sql.Schema, idx sql.Index) (*sql.FuncDepSet, sql.ColSet, error) {
	var idxCols sql.ColSet
	pref := fmt.Sprintf("%s.", tableName)
	for _, col := range idx.ColumnExpressionTypes() {
		colName := strings.TrimPrefix(strings.ToLower(col.Expression), pref)
		i := sch.IndexOfColName(colName)
		if i < 0 {
			return nil, idxCols, fmt.Errorf("column not found on table during stats building: %s", colName)
		}
		idxCols.Add(sql.ColumnId(i + 1))
	}

	var all sql.ColSet
	var notNull sql.ColSet
	for i, col := range sch {
		all.Add(sql.ColumnId(i + 1))
		if !col.Nullable {
			notNull.Add(sql.ColumnId(i + 1))
		}
	}

	strict := true
	for i, hasNext := idxCols.Next(1); hasNext; i, hasNext = idxCols.Next(i + 1) {
		if !notNull.Contains(i) {
			strict = false
		}
	}

	var strictKeys []sql.ColSet
	var laxKeys []sql.ColSet
	if !idx.IsUnique() {
		// not an FD
	} else if strict {
		strictKeys = append(strictKeys, idxCols)
	} else {
		laxKeys = append(laxKeys, idxCols)
	}
	return sql.NewTablescanFDs(all, strictKeys, laxKeys, notNull), idxCols, nil
}

func newUniformDistStatistic(dbName, tableName string, sch sql.Schema, idx sql.Index, rowCount, avgSize uint64) (sql.Statistic, error) {
	tablePrefix := fmt.Sprintf("%s.", tableName)

	distinctCount := rowCount
	if !idx.IsUnique() {
		distinctCount = uint64(float64(distinctCount) * dummyNotUniqueDistinct)
	}

	nullCount := uint64(float64(distinctCount) * dummyNotUniqueNull)

	var cols []string
	var types []sql.Type
	for _, e := range idx.ColumnExpressionTypes() {
		cols = append(cols, strings.TrimPrefix(strings.ToLower(e.Expression), tablePrefix))
		types = append(types, e.Type)
	}

	var class sql.IndexClass
	switch {
	case idx.IsSpatial():
		class = sql.IndexClassSpatial
	case idx.IsFullText():
		class = sql.IndexClassFulltext
	default:
		class = sql.IndexClassDefault
	}

	qual := sql.NewStatQualifier(dbName, tableName, strings.ToLower(idx.ID()))
	stat := stats.NewStatistic(rowCount, distinctCount, nullCount, avgSize, time.Now(), qual, cols, types, nil, class)

	fds, idxCols, err := indexFds(tableName, sch, idx)
	if err != nil {
		return nil, err
	}
	ret := stat.WithFuncDeps(fds)
	ret = ret.WithColSet(idxCols)
	return ret, nil
}

func newConjCollector(s sql.Statistic, ordinals map[string]int) *conjCollector {
	return &conjCollector{
		stat:     s,
		ordinals: ordinals,
		eqVals:   make([]interface{}, len(ordinals)),
		nullable: make([]bool, len(ordinals)),
	}
}

// conjCollector is used to stack and track changes to
// an index histogram for a list of conjugate filters
type conjCollector struct {
	stat          sql.Statistic
	ordinals      map[string]int
	missingPrefix int
	constant      sql.FastIntSet
	eqVals        []interface{}
	nullable      []bool
	applied       sql.FastIntSet
	isFalse       bool
}

func (c *conjCollector) add(f *iScanLeaf) error {
	c.applied.Add(int(f.Id()))
	var err error
	switch f.Op() {
	case indexScanOpNullSafeEq:
		err = c.addEq(f.gf.Name(), f.litValue, true)
	case indexScanOpEq:
		err = c.addEq(f.gf.Name(), f.litValue, false)
	case indexScanOpInSet:
		// TODO cost UNION of equals
		err = c.addEq(f.gf.Name(), f.setValues[0], false)
	default:
		err = c.addIneq(f.Op(), f.gf.Name(), f.litValue)
	}
	return err
}

func (c *conjCollector) addEq(col string, val interface{}, nullSafe bool) error {
	// make constant
	ord := c.ordinals[col]
	if c.constant.Contains(ord) {
		if c.eqVals[ord] != val {
			// FALSE filter
			c.isFalse = true
			return nil
		}
		return nil
	}

	c.constant.Add(ord)
	c.eqVals[ord] = val
	c.nullable[ord] = nullSafe

	if ord == c.missingPrefix {
		// we are interested in the cases where the index prefix
		// key is extended
		if ord == len(c.eqVals)-1 {
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
		var err error
		c.stat, err = stats.PrefixKey(c.stat, c.eqVals[:ord+1], c.nullable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *conjCollector) addIneq(op indexScanOp, col string, val interface{}) error {
	ord := c.ordinals[col]
	if ord > 0 {
		return nil
	}
	err := c.cmpFirstCol(op, val)
	if err != nil {
		return err
	}
	return c.truncateMcvs(ord, op, val)
}

// cmpFirstCol checks whether we should try to range truncate the first
// column in the index
func (c *conjCollector) cmpFirstCol(op indexScanOp, val interface{}) error {
	// check if first col already constant
	// otherwise attempt to truncate histogram
	var err error
	if c.constant.Contains(1) {
		return nil
	}
	switch op {
	case indexScanOpNotEq:
		// todo notEq
		c.stat, err = stats.PrefixGt(c.stat, val)
	case indexScanOpGt:
		c.stat, err = stats.PrefixGt(c.stat, val)
	case indexScanOpGte:
		c.stat, err = stats.PrefixGte(c.stat, val)
	case indexScanOpLt:
		c.stat, err = stats.PrefixLt(c.stat, val)
	case indexScanOpLte:
		c.stat, err = stats.PrefixLte(c.stat, val)
	case indexScanOpIsNull:
		c.stat, err = stats.PrefixIsNull(c.stat)
	case indexScanOpIsNotNull:
		c.stat, err = stats.PrefixIsNotNull(c.stat)
	}
	return err
}

func (c *conjCollector) truncateMcvs(i int, op indexScanOp, val interface{}) error {
	var err error
	switch op {
	case indexScanOpGt:
		c.stat, err = stats.McvPrefixGt(c.stat, i, val)
	case indexScanOpGte:
		c.stat, err = stats.McvPrefixGte(c.stat, i, val)
	case indexScanOpLt:
		c.stat, err = stats.McvPrefixLt(c.stat, i, val)
	case indexScanOpLte:
		c.stat, err = stats.McvPrefixLte(c.stat, i, val)
	case indexScanOpIsNull:
		c.stat, err = stats.McvPrefixIsNull(c.stat, i, val)
	case indexScanOpIsNotNull:
		c.stat, err = stats.McvPrefixIsNotNull(c.stat, i, val)
	}
	return err
}
