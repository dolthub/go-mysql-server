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

package memo

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type GroupId uint16
type TableId uint16

type TableAndColumn struct {
	tableName  string
	columnName string
}

// Memo collects a forest of query plans structured by logical and
// physical equivalency. Logically equivalent plans, represented by
// an exprGroup, produce the same rows (possibly unordered) and schema.
// Physical plans are stored in a linked list within an expression group.
type Memo struct {
	cnt  uint16
	root *ExprGroup

	hints *joinHints

	c         Coster
	s         Carder
	statsProv sql.StatsProvider
	Ctx       *sql.Context
	scope     *plan.Scope
	scopeLen  int

	TableProps *tableProps
}

func NewMemo(ctx *sql.Context, stats sql.StatsProvider, s *plan.Scope, scopeLen int, cost Coster, card Carder) *Memo {
	return &Memo{
		Ctx:        ctx,
		c:          cost,
		s:          card,
		statsProv:  stats,
		scope:      s,
		scopeLen:   scopeLen,
		TableProps: newTableProps(),
		hints:      &joinHints{},
	}
}

func (m *Memo) Root() *ExprGroup {
	return m.root
}

// newExprGroup creates a new logical expression group to encapsulate the
// action of a SQL clause.
// TODO: this is supposed to deduplicate logically equivalent table scans
// and scalar expressions, replacing references with a pointer. Currently
// a hacky format to quickly support memoizing join trees.
func (m *Memo) NewExprGroup(rel exprType) *ExprGroup {
	m.cnt++
	id := GroupId(m.cnt)
	grp := newExprGroup(m, id, rel)

	if s, ok := rel.(SourceRel); ok {
		m.TableProps.addTable(s.Name(), id)
	}
	return grp
}

func (m *Memo) memoizeSourceRel(rel SourceRel) *ExprGroup {
	grp := m.NewExprGroup(rel)
	return grp
}

func (m *Memo) getTableId(table string) (GroupId, bool) {
	return m.TableProps.GetId(table)
}

func (m *Memo) MemoizeLeftJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []sql.Expression) *ExprGroup {
	newJoin := &LeftJoin{
		JoinBase: &JoinBase{
			relBase: &relBase{},
			Left:    left,
			Right:   right,
			Op:      op,
			Filter:  filter,
		},
	}
	// todo intern relExprs? add to appropriate group?
	if grp == nil {
		return m.NewExprGroup(newJoin)
	}
	newJoin.g = grp
	grp.Prepend(newJoin)
	return grp
}

func (m *Memo) MemoizeInnerJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []sql.Expression) *ExprGroup {
	newJoin := &InnerJoin{
		JoinBase: &JoinBase{
			relBase: &relBase{},
			Left:    left,
			Right:   right,
			Op:      op,
			Filter:  filter,
		},
	}
	// todo intern relExprs? add to appropriate group?
	if grp == nil {
		return m.NewExprGroup(newJoin)
	}
	newJoin.g = grp
	grp.Prepend(newJoin)
	return grp
}

func (m *Memo) MemoizeLookupJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []sql.Expression, lookup *Lookup) *ExprGroup {
	newJoin := &LookupJoin{
		JoinBase: &JoinBase{
			relBase: &relBase{},
			Left:    left,
			Right:   right,
			Op:      op,
			Filter:  filter,
		},
		Lookup: lookup,
	}
	newJoin.Lookup.Parent = newJoin.JoinBase

	if grp == nil {
		return m.NewExprGroup(newJoin)
	}
	newJoin.g = grp
	grp.Prepend(newJoin)
	return grp
}

func (m *Memo) MemoizeRangeHeapJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []sql.Expression, rangeHeap *RangeHeap) *ExprGroup {
	newJoin := &RangeHeapJoin{
		JoinBase: &JoinBase{
			relBase: &relBase{},
			Left:    left,
			Right:   right,
			Op:      op,
			Filter:  filter,
		},
		RangeHeap: rangeHeap,
	}
	newJoin.RangeHeap.Parent = newJoin.JoinBase

	if grp == nil {
		return m.NewExprGroup(newJoin)
	}
	newJoin.g = grp
	grp.Prepend(newJoin)
	return grp
}

func (m *Memo) MemoizeMergeJoin(grp, left, right *ExprGroup, lIdx, rIdx *IndexScan, op plan.JoinType, filter []sql.Expression, swapCmp bool) *ExprGroup {
	rel := &MergeJoin{
		JoinBase: &JoinBase{
			relBase: &relBase{},
			Op:      op,
			Filter:  filter,
			Left:    left,
			Right:   right,
		},
		InnerScan: lIdx,
		OuterScan: rIdx,
		SwapCmp:   swapCmp,
	}
	rel.InnerScan.Parent = rel.JoinBase
	rel.OuterScan.Parent = rel.JoinBase

	if grp == nil {
		return m.NewExprGroup(rel)
	}
	rel.g = grp
	grp.Prepend(rel)
	return grp
}

func (m *Memo) MemoizeProject(grp, child *ExprGroup, projections []sql.Expression) *ExprGroup {
	rel := &Project{
		relBase:     &relBase{},
		Child:       child,
		Projections: projections,
	}
	if grp == nil {
		return m.NewExprGroup(rel)
	}
	rel.g = grp
	grp.Prepend(rel)
	return grp
}

func (m *Memo) MemoizeFilter(grp, child *ExprGroup, filters []sql.Expression) *ExprGroup {
	rel := &Filter{
		relBase: &relBase{},
		Child:   child,
		Filters: filters,
	}
	if grp == nil {
		return m.NewExprGroup(rel)
	}
	rel.g = grp
	grp.Prepend(rel)
	return grp
}

func (m *Memo) MemoizeMax1Row(grp, child *ExprGroup) *ExprGroup {
	rel := &Max1Row{
		relBase: &relBase{},
		Child:   child,
	}
	if grp == nil {
		return m.NewExprGroup(rel)
	}
	rel.g = grp
	grp.Prepend(rel)
	return grp
}

// OptimizeRoot finds the implementation for the root expression
// that has the lowest cost.
func (m *Memo) OptimizeRoot() error {
	return m.optimizeMemoGroup(m.root)
}

// optimizeMemoGroup recursively builds the lowest cost plan for memo
// group expressions. We optimize expressions groups independently, walking
// the linked list of execution plans for a particular group only after
// optimizing all subgroups. All plans within a group by definition share
// the same subgroup dependencies. After finding the best implementation
// for a particular group, we fix the best plan for that group and recurse
// into its parents.
// TODO: we should not have to cost every plan, sometimes there is a provably
// best case implementation
func (m *Memo) optimizeMemoGroup(grp *ExprGroup) error {
	if grp.Done {
		return nil
	}
	var err error
	n := grp.First
	for n != nil {
		var cost float64
		for _, g := range n.Children() {
			err = m.optimizeMemoGroup(g)
			if err != nil {
				return err
			}
			cost += g.Cost
		}
		relCost, err := m.c.EstimateCost(m.Ctx, n, m.statsProv)
		if err != nil {
			return err
		}

		if grp.RelProps.Distinct.IsHash() {
			var dCost float64
			if sortedInputs(n) {
				n.SetDistinct(SortedDistinctOp)
			} else {
				n.SetDistinct(HashDistinctOp)
				dCost, err = m.c.EstimateCost(m.Ctx, &Distinct{Child: grp}, m.statsProv)
				if err != nil {
					return err
				}
			}
			relCost += dCost
		} else {
			n.SetDistinct(noDistinctOp)
		}

		n.SetCost(relCost)
		cost += relCost
		m.updateBest(grp, n, cost)
		n = n.Next()
	}

	grp.Done = true
	grp.RelProps.card, err = m.s.EstimateCard(m.Ctx, grp.Best, m.statsProv)
	if err != nil {
		return err
	}
	return nil
}

// updateBest chooses the best hinted plan or the best overall plan if the
// hint corresponds to  no valid plan. Ordering is applied as a global
// rather than a local property.
func (m *Memo) updateBest(grp *ExprGroup, n RelExpr, cost float64) {
	if m.hints != nil {
		if m.hints.satisfiedBy(n) {
			if !grp.HintOk {
				grp.Best = n
				grp.Cost = cost
				grp.HintOk = true
				return
			}
			grp.updateBest(n, cost)
		} else if grp.Best == nil || !grp.HintOk {
			grp.updateBest(n, cost)
		}
		return
	}
	grp.updateBest(n, cost)
}

func (m *Memo) BestRootPlan(ctx *sql.Context) (sql.Node, error) {
	b := NewExecBuilder()
	return buildBestJoinPlan(b, m.root, nil)
}

// buildBestJoinPlan converts group's lowest cost implementation into a
// tree node with a recursive DFS.
func buildBestJoinPlan(b *ExecBuilder, grp *ExprGroup, input sql.Schema) (sql.Node, error) {
	if !grp.Done {
		panic("expected expression group plans to be fixed")
	}
	n := grp.Best
	var err error
	children := make([]sql.Node, len(n.Children()))
	for i, g := range n.Children() {
		children[i], err = buildBestJoinPlan(b, g, input)
		if err != nil {
			return nil, err
		}
	}
	return b.buildRel(n, children...)
}

func getProjectColset(p *Project) sql.ColSet {
	var colset sql.ColSet
	for _, e := range p.Projections {
		transform.InspectExpr(e, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok && gf.Id() > 0 {
				colset.Add(gf.Id())
			}
			return false
		})
	}
	return colset
}

func (m *Memo) ApplyHint(hint Hint) {
	switch hint.Typ {
	case HintTypeJoinOrder:
		m.WithJoinOrder(hint.Args)
	case HintTypeJoinFixedOrder:
	case HintTypeInnerJoin, HintTypeMergeJoin, HintTypeLookupJoin, HintTypeHashJoin, HintTypeSemiJoin, HintTypeAntiJoin, HintTypeLeftOuterLookupJoin:
		m.WithJoinOp(hint.Typ, hint.Args[0], hint.Args[1])
	default:
	}
}

func (m *Memo) WithJoinOrder(tables []string) {
	// order maps groupId -> table dependencies
	order := make(map[GroupId]uint64)
	for i, t := range tables {
		id, ok := m.TableProps.GetId(t)
		if !ok {
			return
		}
		order[id] = uint64(i)
	}
	hint := newJoinOrderHint(order)
	hint.build(m.root)
	if hint.isValid() {
		m.hints.order = hint
	}
}

func (m *Memo) WithJoinOp(op HintType, left, right string) {
	lGrp, _ := m.TableProps.GetId(left)
	rGrp, _ := m.TableProps.GetId(right)
	hint := newjoinOpHint(op, lGrp, rGrp)
	if !hint.isValid() {
		return
	}
	m.hints.ops = append(m.hints.ops, hint)
}

func (m *Memo) String() string {
	exprs := make([]string, m.cnt)
	groups := make([]*ExprGroup, 0)
	if m.root != nil {
		r := m.root.First
		for r != nil {
			groups = append(groups, r.Group())
			groups = append(groups, r.Children()...)
			r = r.Next()
		}
	}
	for len(groups) > 0 {
		newGroups := make([]*ExprGroup, 0)
		for _, g := range groups {
			if exprs[int(TableIdForSource(g.Id))] != "" {
				continue
			}
			exprs[int(TableIdForSource(g.Id))] = g.String()
			newGroups = append(newGroups, g.children()...)
		}
		groups = newGroups
	}
	b := strings.Builder{}
	b.WriteString("memo:\n")
	beg := "├──"
	for i, g := range exprs {
		if i == len(exprs)-1 {
			beg = "└──"
		}
		b.WriteString(fmt.Sprintf("%s G%d: %s\n", beg, i+1, g))
	}
	return b.String()
}

type tableProps struct {
	grpToName map[GroupId]string
	nameToGrp map[string]GroupId
}

func newTableProps() *tableProps {
	return &tableProps{
		grpToName: make(map[GroupId]string),
		nameToGrp: make(map[string]GroupId),
	}
}

func (p *tableProps) addTable(n string, id GroupId) {
	p.grpToName[id] = n
	p.nameToGrp[n] = id
}

func (p *tableProps) GetTable(id GroupId) (string, bool) {
	n, ok := p.grpToName[id]
	return n, ok
}

func (p *tableProps) GetId(n string) (GroupId, bool) {
	id, ok := p.nameToGrp[strings.ToLower(n)]
	return id, ok
}

func (p *tableProps) getTableNames(f sql.FastIntSet) []string {
	var names []string
	for idx, ok := f.Next(0); ok; idx, ok = f.Next(idx + 1) {
		if ok {
			groupId := GroupId(idx + 1)
			table, ok := p.GetTable(groupId)
			if !ok {
				panic(fmt.Sprintf("table not found for group %d", groupId))
			}
			names = append(names, table)
		}
	}
	return names
}

// Coster types can estimate the CPU and memory cost of physical execution
// operators.
type Coster interface {
	// EstimateCost cost returns the incremental CPU and memory cost for an
	// operator, or an error. Cost is dependent on physical operator type,
	// and the cardinality of inputs.
	EstimateCost(*sql.Context, RelExpr, sql.StatsProvider) (float64, error)
}

// Carder types can estimate the cardinality (row count) of relational
// expressions.
type Carder interface {
	// EstimateCard returns the estimate row count outputs for a relational
	// expression. Cardinality is an expression group property.
	EstimateCard(*sql.Context, RelExpr, sql.StatsProvider) (float64, error)
}

// RelExpr wraps a sql.Node for use as a ExprGroup linked list node.
// TODO: we need relExprs for every sql.Node and sql.Expression
type RelExpr interface {
	fmt.Stringer
	exprType
	Next() RelExpr
	SetNext(RelExpr)
	SetCost(c float64)
	Cost() float64
	Distinct() distinctOp
	SetDistinct(distinctOp)
}

type relBase struct {
	// g is this relation's expression group
	g *ExprGroup
	// n is the next RelExpr in the ExprGroup linked list
	n RelExpr
	// c is this relation's cost while costing and plan reify are separate
	c float64
	// cnt is this relations output row count
	cnt float64
	// d indicates a RelExpr should be checked for distinctness
	d distinctOp
}

// relKey is a quick identifier for avoiding duplicate work on the same
// RelExpr.
// TODO: the key should be a formalized hash of 1) the operator type, and 2)
// hashes of the RelExpr and ScalarExpr children.
func relKey(r RelExpr) uint64 {
	key := int(r.Group().Id)
	i := 1<<16 - 1
	for _, c := range r.Children() {
		key += i * int(c.Id)
		i *= 1<<16 - 1
	}
	return uint64(key)
}

type distinctOp uint8

const (
	unknownDistinctOp distinctOp = iota
	noDistinctOp
	SortedDistinctOp
	HashDistinctOp
)

func (d distinctOp) IsHash() bool {
	return d == HashDistinctOp
}

func (r *relBase) Distinct() distinctOp {
	return r.d
}

func (r *relBase) SetDistinct(d distinctOp) {
	r.d = d
}

func (r *relBase) Group() *ExprGroup {
	return r.g
}

func (r *relBase) SetGroup(g *ExprGroup) {
	r.g = g
}

func (r *relBase) Next() RelExpr {
	return r.n
}

func (r *relBase) SetNext(rel RelExpr) {
	r.n = rel
}

func (r *relBase) SetCost(c float64) {
	r.c = c
}

func (r *relBase) Cost() float64 {
	return r.c
}

func TableIdForSource(id GroupId) sql.TableId {
	return sql.TableId(id - 1)
}

type exprType interface {
	Group() *ExprGroup
	Children() []*ExprGroup
	SetGroup(g *ExprGroup)
}

// SourceRel represents a data source, like a tableScan, subqueryAlias,
// or list of values.
type SourceRel interface {
	RelExpr
	// outputCols retuns the output schema of this data source.
	// TODO: this is more useful as a relExpr property, but we need
	// this to fix up expression indexes currently
	OutputCols() sql.Schema
	Name() string
	TableId() sql.TableId
	Indexes() []*Index
	SetIndexes(indexes []*Index)
	TableIdNode() plan.TableIdNode
}

type Index struct {
	// ordered list of index columns
	order []sql.ColumnId
	// unordered column set
	set sql.ColSet
	idx sql.Index
}

func (i *Index) Cols() []sql.ColumnId {
	return i.order
}

func (i *Index) ColSet() sql.ColSet {
	return i.set
}

func (i *Index) SqlIdx() sql.Index {
	return i.idx
}

type sourceBase struct {
	*relBase
	indexes []*Index
}

func (s *sourceBase) Indexes() []*Index {
	return s.indexes
}

func (s *sourceBase) SetIndexes(indexes []*Index) {
	s.indexes = indexes
}

// JoinRel represents a plan.JoinNode or plan.CrossJoin. See plan.JoinType
// for the full list.
type JoinRel interface {
	RelExpr
	JoinPrivate() *JoinBase
	Group() *ExprGroup
}

var _ JoinRel = (*AntiJoin)(nil)
var _ JoinRel = (*ConcatJoin)(nil)
var _ JoinRel = (*CrossJoin)(nil)
var _ JoinRel = (*LeftJoin)(nil)
var _ JoinRel = (*FullOuterJoin)(nil)
var _ JoinRel = (*HashJoin)(nil)
var _ JoinRel = (*InnerJoin)(nil)
var _ JoinRel = (*LookupJoin)(nil)
var _ JoinRel = (*SemiJoin)(nil)

type JoinBase struct {
	*relBase

	Op     plan.JoinType
	Filter []sql.Expression
	Left   *ExprGroup
	Right  *ExprGroup
}

func (r *JoinBase) Children() []*ExprGroup {
	return []*ExprGroup{r.Left, r.Right}
}

func (r *JoinBase) JoinPrivate() *JoinBase {
	return r
}

// Copy creates a JoinBase with the same underlying join expression.
// note: it is important to Copy the base node to avoid cyclical
// relExpr references in the ExprGroup linked list.
func (r *JoinBase) Copy() *JoinBase {
	return &JoinBase{
		relBase: &relBase{
			g: r.g,
			n: r.n,
			c: r.c,
		},
		Op:     r.Op,
		Filter: r.Filter,
		Left:   r.Left,
		Right:  r.Right,
	}
}

type Lookup struct {
	Index    *Index
	KeyExprs []sql.Expression
	Nullmask []bool

	Parent *JoinBase
}

type IndexScan struct {
	Idx    *Index
	Range  sql.Range
	Parent *JoinBase
}

// RangeHeap contains all the information necessary to construct a RangeHeap join.
// Because both sides of the join can be implemented either by an index or a sorted node,
// we require that exactly one of ValueIndex and ValueExpr is non-nil, and exactly one
// of MinIndex and MinExpr is non-nil. If the index is non-nil, we will use it to construct
// a plan.IndexedTableAccess. Otherwise we use the expression to construct a plan.Sort.
type RangeHeap struct {
	ValueIndex *IndexScan
	ValueExpr  sql.Expression

	MinIndex *IndexScan
	MinExpr  sql.Expression

	ValueCol                *expression.GetField
	MinColRef               *expression.GetField
	MaxColRef               *expression.GetField
	RangeClosedOnLowerBound bool
	RangeClosedOnUpperBound bool
	Parent                  *JoinBase
}
