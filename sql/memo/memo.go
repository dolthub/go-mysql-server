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

// Memo collects a forest of query plans structured by logical and
// physical equivalency. Logically equivalent plans, represented by
// an exprGroup, produce the same rows (possibly unordered) and schema.
// Physical plans are stored in a linked list within an expression group.
type Memo struct {
	cnt     uint16
	root    *ExprGroup
	exprs   map[uint64]*ExprGroup
	Columns map[string]sql.ColumnId
	hints   *joinHints

	c        Coster
	s        Carder
	statsRw  sql.StatsReadWriter
	Ctx      *sql.Context
	scope    *plan.Scope
	scopeLen int

	TableProps *tableProps
}

func NewMemo(ctx *sql.Context, stats sql.StatsReadWriter, s *plan.Scope, scopeLen int, cost Coster, card Carder) *Memo {
	return &Memo{
		Ctx:        ctx,
		c:          cost,
		s:          card,
		statsRw:    stats,
		scope:      s,
		scopeLen:   scopeLen,
		TableProps: newTableProps(),
		hints:      &joinHints{},
		Columns:    make(map[string]sql.ColumnId),
		exprs:      make(map[uint64]*ExprGroup),
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
	m.assignColumnIds(rel)
	return grp
}

// TODO we need to remove this as soon as name resolution refactor is in
func (m *Memo) assignColumnIds(rel SourceRel) {
	if rel.Name() == "" {
		m.Columns["1"] = sql.ColumnId(len(m.Columns) + 1)
	} else {
		for _, c := range rel.OutputCols() {
			var name string
			if c.Source != "" {
				name = fmt.Sprintf("%s.%s", strings.ToLower(c.Source), strings.ToLower(c.Name))
			} else {
				name = fmt.Sprintf("%s.%s", strings.ToLower(rel.Name()), strings.ToLower(c.Name))
			}
			m.Columns[name] = sql.ColumnId(len(m.Columns) + 1)
		}
	}
}

func (m *Memo) getTableId(table string) (GroupId, bool) {
	return m.TableProps.GetId(table)
}

func (m *Memo) getColumnId(table, name string) (sql.ColumnId, bool) {
	var tag string
	if table != "" {
		tag = fmt.Sprintf("%s.%s", strings.ToLower(table), strings.ToLower(name))
	} else {
		tag = name
	}
	id, ok := m.Columns[tag]
	return id, ok
}

func (m *Memo) PreexistingScalar(e ScalarExpr) *ExprGroup {
	hash := internExpr(e)
	group, _ := m.exprs[hash]
	return group
}

func (m *Memo) MemoizeScalar(e sql.Expression) *ExprGroup {
	var scalar *ExprGroup
	switch e := e.(type) {
	case expression.ArithmeticOp:
		scalar = m.memoizeArithmetic(e)
	case expression.Comparer:
		scalar = m.memoizeComparison(e)
	case *expression.Literal:
		scalar = m.memoizeLiteral(e)
	case *expression.GetField:
		scalar = m.MemoizeColRef(e)
	case *expression.IsNull:
		scalar = m.MemoizeIsNull(e.Child)
	case *expression.And:
		scalar = m.memoizeAnd(e)
	case *expression.Or:
		scalar = m.memoizeOr(e)
	case *expression.BindVar:
		scalar = m.memoizeBindvar(e)
	default:
		scalar = m.memoizeHidden(e)
	}
	hash := internExpr(scalar.Scalar)
	if hash != 0 {
		m.exprs[hash] = scalar
	}
	return scalar
}

func (m *Memo) memoizeAnd(e *expression.And) *ExprGroup {
	left := m.MemoizeScalar(e.Left)
	right := m.MemoizeScalar(e.Right)
	scalar := &And{scalarBase: &scalarBase{}, Left: left, Right: right}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	return grp
}

func (m *Memo) memoizeOr(e *expression.Or) *ExprGroup {
	left := m.MemoizeScalar(e.Left)
	right := m.MemoizeScalar(e.Right)
	scalar := &Or{scalarBase: &scalarBase{}, Left: left, Right: right}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	return grp
}

func (m *Memo) memoizeLiteral(lit *expression.Literal) *ExprGroup {
	scalar := &Literal{scalarBase: &scalarBase{}, Val: lit.Value(), Typ: lit.Type()}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	return grp
}

func (m *Memo) MemoizeColRef(e *expression.GetField) *ExprGroup {
	col, ok := m.getColumnId(e.Table(), e.Name())
	if !ok {
		panic("unreachable")
	}
	var table GroupId
	if e.Table() != "" {
		table, ok = m.getTableId(e.Table())
		if !ok {
			panic("unreachable")
		}
	}
	scalar := &ColRef{scalarBase: &scalarBase{}, Col: col, Table: table, Gf: e}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	// references table, col
	return grp
}

func (m *Memo) memoizeArithmetic(comp expression.ArithmeticOp) *ExprGroup {
	lGrp := m.MemoizeScalar(comp.LeftChild())
	rGrp := m.MemoizeScalar(comp.RightChild())
	var scalar ScalarExpr
	var op ArithType
	switch e := comp.(type) {
	case *expression.Arithmetic:
		switch e.Op {
		case "+":
			op = ArithTypePlus
		case "-":
			op = ArithTypeMinus
		case "*":
			op = ArithTypeMult
		default:
			panic(fmt.Sprintf("unsupported arithemtic type: %s", e.Op))
		}
	case *expression.Div:
		op = ArithTypeDiv
	case *expression.IntDiv:
		op = ArithTypeIntDiv
	case *expression.Mod:
		op = ArithTypeIntDiv
	default:
		panic(fmt.Sprintf("unsupported type: %T", e))
	}
	scalar = &Arithmetic{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp, Op: op}
	eGroup := m.PreexistingScalar(scalar)
	if eGroup == nil {
		eGroup = m.NewExprGroup(scalar)
	}
	return eGroup
}

func (m *Memo) memoizeComparison(comp expression.Comparer) *ExprGroup {
	lGrp := m.MemoizeScalar(comp.Left())
	rGrp := m.MemoizeScalar(comp.Right())
	var scalar ScalarExpr
	switch e := comp.(type) {
	case *expression.Equals:
		scalar = &Equal{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.NullSafeEquals:
		scalar = &NullSafeEq{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.GreaterThan:
		scalar = &Gt{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.GreaterThanOrEqual:
		scalar = &Geq{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.LessThan:
		scalar = &Lt{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.LessThanOrEqual:
		scalar = &Leq{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.Regexp:
		scalar = &Regexp{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	case *expression.InTuple:
		scalar = &InTuple{scalarBase: &scalarBase{}, Left: lGrp, Right: rGrp}
	default:
		panic(fmt.Sprintf("unsupported type: %T", e))
	}
	eGroup := m.PreexistingScalar(scalar)
	if eGroup == nil {
		eGroup = m.NewExprGroup(scalar)
	}
	return eGroup
}

func (m *Memo) MemoizeIsNull(child sql.Expression) *ExprGroup {
	childGrp := m.MemoizeScalar(child)

	scalar := &IsNull{scalarBase: &scalarBase{}, Child: childGrp}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	return grp
}

func (m *Memo) memoizeBindvar(e *expression.BindVar) *ExprGroup {
	scalar := &Bindvar{scalarBase: &scalarBase{}, Name: e.Name, Typ: e.Typ}
	grp := m.PreexistingScalar(scalar)
	if grp != nil {
		return grp
	}
	grp = m.NewExprGroup(scalar)
	// TODO scalar props
	return grp
}

func (m *Memo) memoizeHidden(e sql.Expression) *ExprGroup {
	var cols sql.ColSet
	var tables sql.FastIntSet
	transform.InspectExpr(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			colRef := m.MemoizeScalar(e).Scalar.(*ColRef)
			cols.Add(colRef.Col)
			tables.Add(int(TableIdForSource(colRef.Table)))
		default:
		}
		return false
	})
	scalar := &Hidden{scalarBase: &scalarBase{}, E: e, Cols: cols, Tables: tables}
	return m.NewExprGroup(scalar)
}

func (m *Memo) MemoizeLeftJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []ScalarExpr) *ExprGroup {
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

func (m *Memo) MemoizeInnerJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []ScalarExpr) *ExprGroup {
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

func (m *Memo) MemoizeLookupJoin(grp, left, right *ExprGroup, op plan.JoinType, filter []ScalarExpr, lookup *Lookup) *ExprGroup {
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

func (m *Memo) MemoizeProject(grp, child *ExprGroup, projections []*ExprGroup) *ExprGroup {
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

func (m *Memo) MemoizeFilter(grp, child *ExprGroup, filters []*ExprGroup) *ExprGroup {
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
		relCost, err := m.c.EstimateCost(m.Ctx, n, m.statsRw)
		if err != nil {
			return err
		}

		if grp.RelProps.Distinct.IsHash() {
			var dCost float64
			if sortedInputs(n) {
				n.SetDistinct(SortedDistinctOp)
			} else {
				n.SetDistinct(HashDistinctOp)
				dCost, err = m.c.EstimateCost(m.Ctx, &Distinct{Child: grp}, m.statsRw)
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
	grp.RelProps.card, err = m.s.EstimateCard(m.Ctx, grp.Best, m.statsRw)
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

func (m *Memo) BestRootPlan() (sql.Node, error) {
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
		input = append(input, g.RelProps.OutputCols()...)
	}
	return b.buildRel(n, input, children...)
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
	for _, e := range m.exprs {
		exprs[int(TableIdForSource(e.Id))] = e.String()
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

func (p *tableProps) getTable(id GroupId) (string, bool) {
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
			table, ok := p.getTable(groupId)
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
	EstimateCost(*sql.Context, RelExpr, sql.StatsReader) (float64, error)
}

// Carder types can estimate the cardinality (row count) of relational
// expressions.
type Carder interface {
	// EstimateCard returns the estimate row count outputs for a relational
	// expression. Cardinality is an expression group property.
	EstimateCard(*sql.Context, RelExpr, sql.StatsReader) (float64, error)
}

type unaryScalarExpr interface {
	child() ScalarExpr
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

func TableIdForSource(id GroupId) TableId {
	return TableId(id - 1)
}

type exprType interface {
	Group() *ExprGroup
	Children() []*ExprGroup
	SetGroup(g *ExprGroup)
}

// ScalarExpr is a sql.Expression equivalent. Both ScalarExpr
// and RelExpr are embedded in Memo as *ExprGroup. ScalarExpr
// will only have one implementation.
// todo: do we need scalar expressions in the memo? or could
// they be ref'd out
type ScalarExpr interface {
	fmt.Stringer
	exprType
	ExprId() ScalarExprId
}

type scalarBase struct {
	// g is this relation's expression group
	g *ExprGroup
}

func (r *scalarBase) Group() *ExprGroup {
	return r.g
}

func (r *scalarBase) SetGroup(g *ExprGroup) {
	r.g = g
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
	TableId() TableId
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
	Filter []ScalarExpr
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
	Source   string
	Index    sql.Index
	KeyExprs []ScalarExpr
	Nullmask []bool

	Parent *JoinBase
}

type IndexScan struct {
	Source string
	Idx    sql.Index

	Parent *JoinBase
}

// splitConjunction_memo breaks AND expressions into their left and right parts, recursively
func SplitConjunction(e ScalarExpr) []ScalarExpr {
	if e == nil {
		return nil
	}
	a, ok := e.(*And)
	if !ok {
		return []ScalarExpr{e}
	}

	return append(
		SplitConjunction(a.Left.Scalar),
		SplitConjunction(a.Right.Scalar)...,
	)
}

// splitDisjunction breaks OR expressions into their left and right parts, recursively
func SplitDisjunction(e *Or) []ScalarExpr {
	q := []ScalarExpr{e.Left.Scalar, e.Right.Scalar}
	var ret []ScalarExpr
	for len(q) > 0 {
		next := q[0]
		q = q[1:]
		nextOr, ok := next.(*Or)
		if !ok {
			ret = append(ret, next)
		} else {
			q = append(q, nextOr.Left.Scalar, nextOr.Right.Scalar)
		}
	}
	return ret
}

func ScalarToSqlCol(e *ExprGroup) *sql.Column {
	switch e := e.Scalar.(type) {
	case *ColRef:
		return &sql.Column{
			Name:     e.Gf.Name(),
			Source:   e.Gf.Table(),
			Type:     e.Gf.Type(),
			Nullable: e.Gf.IsNullable(),
		}
	case *Literal:
		return &sql.Column{
			Name: fmt.Sprintf("%v", e.Val),
			Type: e.Typ,
		}
	default:
		return nil
	}
}
