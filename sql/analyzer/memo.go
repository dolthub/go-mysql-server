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
	"strings"

	"github.com/dolthub/go-mysql-server/optgen/cmd/support"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

//go:generate go run ../../optgen/cmd/optgen/main.go -out memo.og.go -pkg analyzer memo memo.go

type GroupId uint16
type TableId uint16

// Memo collects a forest of query plans structured by logical and
// physical equivalency. Logically equivalent plans, represented by
// an exprGroup, produce the same rows (possibly unordered) and schema.
// Physical plans are stored in a linked list within an expression group.
type Memo struct {
	cnt  uint16
	root *exprGroup

	hints *joinHints

	c        Coster
	s        Carder
	statsRw  sql.StatsReadWriter
	ctx      *sql.Context
	scope    *Scope
	scopeLen int

	tableProps *tableProps
}

func NewMemo(ctx *sql.Context, stats sql.StatsReadWriter, s *Scope, cost Coster, card Carder) *Memo {
	return &Memo{
		ctx:        ctx,
		c:          cost,
		s:          card,
		statsRw:    stats,
		scope:      s,
		scopeLen:   len(s.Schema()),
		tableProps: newTableProps(),
		hints:      &joinHints{},
	}
}

// memoize creates a new logical expression group to encapsulate the
// action of a SQL clause.
// TODO: this is supposed to deduplicate logically equivalent table scans
// and scalar expressions, replacing references with a pointer. Currently
// a hacky format to quickly support memoizing join trees.
func (m *Memo) memoize(rel relExpr) *exprGroup {
	m.cnt++
	id := GroupId(m.cnt)
	grp := newExprGroup(m, id, rel)

	if s, ok := rel.(sourceRel); ok {
		m.tableProps.addTable(s.name(), id)
	}
	return grp
}

// optimizeRoot finds the implementation for the root expression
// that has the lowest cost.
func (m *Memo) optimizeRoot() error {
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
func (m *Memo) optimizeMemoGroup(grp *exprGroup) error {
	if grp.done {
		return nil
	}
	var err error
	n := grp.first
	for n != nil {
		var cost float64
		for _, g := range n.children() {
			err = m.optimizeMemoGroup(g)
			if err != nil {
				return err
			}
			cost += g.cost
		}
		relCost, err := m.c.EstimateCost(m.ctx, n, m.statsRw)
		if err != nil {
			return err
		}

		if grp.relProps.distinct.IsHash() {
			var dCost float64
			if sortedInputs(n) {
				n.setDistinct(sortedDistinctOp)
			} else {
				n.setDistinct(hashDistinctOp)
				dCost, err = m.c.EstimateCost(m.ctx, &distinct{child: grp}, m.statsRw)
				if err != nil {
					return err
				}
			}
			relCost += dCost
		} else {
			n.setDistinct(noDistinctOp)
		}

		n.setCost(relCost)
		cost += relCost
		m.updateBest(grp, n, cost)
		n = n.next()
	}

	grp.done = true
	grp.relProps.card, err = m.s.EstimateCard(m.ctx, grp.best, m.statsRw)
	if err != nil {
		return err
	}
	return nil
}

// updateBest chooses the best hinted plan or the best overall plan if the
// hint corresponds to  no valid plan. Ordering is applied as a global
// rather than a local property.
func (m *Memo) updateBest(grp *exprGroup, n relExpr, cost float64) {
	if m.hints != nil {
		if m.hints.satisfiedBy(n) {
			if !grp.hintOk {
				grp.best = n
				grp.cost = cost
				grp.hintOk = true
				return
			}
			grp.updateBest(n, cost)
		} else if grp.best == nil || !grp.hintOk {
			grp.updateBest(n, cost)
		}
		return
	}
	grp.updateBest(n, cost)
}

func (m *Memo) bestRootPlan() (sql.Node, error) {
	b := NewExecBuilder()
	return buildBestJoinPlan(b, m.root, nil)
}

// buildBestJoinPlan converts group's lowest cost implementation into a
// tree node with a recursive DFS.
func buildBestJoinPlan(b *ExecBuilder, grp *exprGroup, input sql.Schema) (sql.Node, error) {
	if !grp.done {
		panic("expected expression group plans to be fixed")
	}
	n := grp.best
	var err error
	children := make([]sql.Node, len(n.children()))
	for i, g := range n.children() {
		children[i], err = buildBestJoinPlan(b, g, input)
		if err != nil {
			return nil, err
		}
		input = append(input, g.relProps.OutputCols()...)
	}
	return b.buildRel(n, input, children...)
}

func (m *Memo) applyHint(hint Hint) {
	switch hint.Typ {
	case HintTypeJoinOrder:
		m.WithJoinOrder(hint.Args)
	case HintTypeJoinFixedOrder:
	case HintTypeInnerJoin, HintTypeMergeJoin, HintTypeLookupJoin, HintTypeHashJoin, HintTypeSemiJoin, HintTypeAntiJoin, HintTypeLeftOuterLookupJoin, HintTypeRightSemiLookupJoin:
		m.WithJoinOp(hint.Typ, hint.Args[0], hint.Args[1])
	default:
	}
}

func (m *Memo) WithJoinOrder(tables []string) {
	// order maps groupId -> table dependencies
	order := make(map[GroupId]uint64)
	for i, t := range tables {
		id, ok := m.tableProps.getId(t)
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
	lGrp, _ := m.tableProps.getId(left)
	rGrp, _ := m.tableProps.getId(right)
	hint := newjoinOpHint(op, lGrp, rGrp)
	if !hint.isValid() {
		return
	}
	m.hints.ops = append(m.hints.ops, hint)
}

func (m *Memo) String() string {
	exprs := make([]string, m.cnt)
	groups := make([]*exprGroup, 0)
	if m.root != nil {
		r := m.root.first
		for r != nil {
			groups = append(groups, r.group())
			groups = append(groups, r.children()...)
			r = r.next()
		}
	}
	for len(groups) > 0 {
		newGroups := make([]*exprGroup, 0)
		for _, g := range groups {
			if exprs[int(g.id)-1] != "" {
				continue
			}
			exprs[int(g.id)-1] = g.String()
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

// relProps are relational attributes shared by all plans in an expression
// group (see: exprGroup).
type relProps struct {
	grp *exprGroup

	outputCols   sql.Schema
	inputTables  sql.FastIntSet
	outputTables sql.FastIntSet

	card float64

	distinct distinctOp
	limit    sql.Expression
	filter   sql.Expression
}

func newRelProps(rel relExpr) *relProps {
	p := &relProps{
		grp: rel.group(),
	}
	if r, ok := rel.(sourceRel); ok {
		p.outputCols = r.outputCols()
	}
	p.populateOutputTables()
	p.populateInputTables()

	return p
}

// populateOutputTables initializes the bitmap indicating which tables'
// attributes are available outputs from the exprGroup
func (p *relProps) populateOutputTables() {
	switch n := p.grp.first.(type) {
	case sourceRel:
		p.outputTables = sql.NewFastIntSet(int(n.tableId()))
	case *antiJoin:
		p.outputTables = n.left.relProps.OutputTables()
	case *semiJoin:
		p.outputTables = n.left.relProps.OutputTables()
	case *distinct:
		p.outputTables = n.child.relProps.OutputTables()
	case *project:
		p.outputTables = n.child.relProps.OutputTables()
	case joinRel:
		p.outputTables = n.joinPrivate().left.relProps.OutputTables().Union(n.joinPrivate().right.relProps.OutputTables())
	default:
		panic(fmt.Sprintf("unhandled type: %T", n))
	}
}

// populateInputTables initializes the bitmap indicating which tables
// are input into this exprGroup. This is used to enforce join order
// hinting for semi joins.
func (p *relProps) populateInputTables() {
	switch n := p.grp.first.(type) {
	case sourceRel:
		p.inputTables = sql.NewFastIntSet(int(n.tableId()))
	case *distinct:
		p.inputTables = n.child.relProps.InputTables()
	case *project:
		p.inputTables = n.child.relProps.InputTables()
	case joinRel:
		p.inputTables = n.joinPrivate().left.relProps.InputTables().Union(n.joinPrivate().right.relProps.InputTables())
	default:
		panic(fmt.Sprintf("unhandled type: %T", n))
	}
}

func (p *relProps) populateOutputCols() {
	p.outputCols = p.outputColsForRel(p.grp.best)
}

func (p *relProps) outputColsForRel(r relExpr) sql.Schema {
	switch r := r.(type) {
	case *semiJoin:
		return r.left.relProps.OutputCols()
	case *antiJoin:
		return r.left.relProps.OutputCols()
	case *lookupJoin:
		if r.op.IsRightPartial() {
			return r.right.relProps.OutputCols()
		} else if r.op.IsPartial() {
			return r.left.relProps.OutputCols()
		} else {
			return append(r.joinPrivate().left.relProps.OutputCols(), r.joinPrivate().right.relProps.OutputCols()...)
		}
	case joinRel:
		return append(r.joinPrivate().left.relProps.OutputCols(), r.joinPrivate().right.relProps.OutputCols()...)
	case *distinct:
		return r.child.relProps.OutputCols()
	case *project:
		return r.outputCols()
	case sourceRel:
		return r.outputCols()
	default:
		panic("unknown type")
	}
	return nil
}

// OutputCols returns the output schema of a node
func (p *relProps) OutputCols() sql.Schema {
	if p.outputCols == nil {
		if p.grp.best == nil {
			return p.outputColsForRel(p.grp.first)
		}
		p.populateOutputCols()
	}
	return p.outputCols
}

// OutputTables returns a bitmap of tables in the output schema of this node.
func (p *relProps) OutputTables() sql.FastIntSet {
	return p.outputTables
}

// InputTables returns a bitmap of tables input into this node.
func (p *relProps) InputTables() sql.FastIntSet {
	return p.inputTables
}

// sortedInputs returns true if a relation's inputs are sorted on the
// full output schema. The OrderedDistinct operator can be used in this
// case.
func sortedInputs(rel relExpr) bool {
	switch r := rel.(type) {
	case *max1Row:
		return true
	case *project:
		if _, ok := r.child.best.(*max1Row); ok {
			return true
		}
		sortedOn := sortedColsForRel(r.child.best)
		childOutputs := r.outputCols()
		if len(sortedOn) < len(childOutputs) {
			return false
		}
		sorted := make(map[tableCol]struct{})
		for _, c := range sortedOn {
			sorted[tableCol{table: c.Source, col: c.Name}] = struct{}{}
		}
		for _, c := range childOutputs {
			if _, ok := sorted[tableCol{table: strings.ToLower(c.Source), col: strings.ToLower(c.Name)}]; !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func sortedColsForRel(rel relExpr) sql.Schema {
	switch r := rel.(type) {
	case *tableScan:
		tab, ok := r.table.Table.(sql.PrimaryKeyTable)
		if ok {
			ords := tab.PrimaryKeySchema().PkOrdinals
			var pks sql.Schema
			for _, i := range ords {
				pks = append(pks, tab.PrimaryKeySchema().Schema[i])
			}
			return pks
		}
	case *mergeJoin:
		var ret sql.Schema
		for _, e := range r.innerScan.idx.Expressions() {
			// TODO columns can have "." characters, this will miss cases
			parts := strings.Split(e, ".")
			var name string
			if len(parts) == 2 {
				name = parts[1]
			} else {
				return nil
			}
			ret = append(ret, &sql.Column{
				Name:     strings.ToLower(name),
				Source:   strings.ToLower(r.innerScan.idx.Table()),
				Nullable: true},
			)
		}
		return ret
	case joinRel:
		return sortedColsForRel(r.joinPrivate().left.best)
	case *project:
		// TODO remove projections from sortedColsForRel(n.child.best)
		return nil
	case *tableAlias:
		rt, ok := r.table.Child.(*plan.ResolvedTable)
		if !ok {
			return nil
		}
		tab, ok := rt.Table.(sql.PrimaryKeyTable)
		if ok {
			ords := tab.PrimaryKeySchema().PkOrdinals
			var pks sql.Schema
			for _, i := range ords {
				col := tab.PrimaryKeySchema().Schema[i].Copy()
				col.Source = r.name()
				pks = append(pks, col)
			}
			return pks
		}
	default:
	}
	return nil
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

func (p *tableProps) getId(n string) (GroupId, bool) {
	id, ok := p.nameToGrp[n]
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

// exprGroup is a linked list of plans that return the same result set
// defined by row count and schema.
type exprGroup struct {
	m         *Memo
	_children []*exprGroup
	relProps  *relProps
	first     relExpr
	best      relExpr

	id GroupId

	cost   float64
	done   bool
	hintOk bool
}

func newExprGroup(m *Memo, id GroupId, rel relExpr) *exprGroup {
	// bit of circularity: |grp| references |ref|, |rel| references |grp|,
	// and |relProps| references |rel| and |grp| info.
	grp := &exprGroup{
		m:     m,
		id:    id,
		first: rel,
	}
	rel.setGroup(grp)
	grp.relProps = newRelProps(rel)
	return grp
}

// prepend adds a new plan to an expression group at the beginning of
// the list, to avoid recursive exploration steps (like adding indexed joins).
func (e *exprGroup) prepend(rel relExpr) {
	first := e.first
	e.first = rel
	rel.setNext(first)
}

// children returns a unioned list of child exprGroup for
// every logical plan in this group.
func (e *exprGroup) children() []*exprGroup {
	n := e.first
	children := make([]*exprGroup, 0)
	for n != nil {
		children = append(children, n.children()...)
		n = n.next()
	}
	return children
}

// updateBest updates a group's best to the given expression or a hinted
// operator if the hinted plan is not found. Join operator is applied as
// a local rather than global property.
func (e *exprGroup) updateBest(n relExpr, grpCost float64) {
	if e.best == nil || grpCost <= e.cost {
		e.best = n
		e.cost = grpCost
	}
}

func (e *exprGroup) finalize(node sql.Node, input sql.Schema) (sql.Node, error) {
	props := e.relProps
	var result = node
	if props.filter != nil {
		sch := append(input, node.Schema()...)
		filter, _, err := FixFieldIndexes(e.m.scope, nil, sch, props.filter)
		if err != nil {
			return nil, err
		}
		result = plan.NewFilter(filter, result)
	}
	if props.limit != nil {
		result = plan.NewLimit(props.limit, result)
	}
	return result, nil
}

func (e *exprGroup) String() string {
	b := strings.Builder{}
	n := e.first
	sep := ""
	for n != nil {
		b.WriteString(sep)
		b.WriteString(fmt.Sprintf("(%s", formatRelExpr(n)))
		if e.best != nil {
			b.WriteString(fmt.Sprintf(" %.1f", n.cost()))

			childCost := 0.0
			for _, c := range n.children() {
				childCost += c.cost
			}
			if e.cost == n.cost()+childCost {
				b.WriteString(")*")
			} else {
				b.WriteString(")")
			}
		} else {
			b.WriteString(")")
		}
		sep = " "
		n = n.next()
	}
	return b.String()
}

// Coster types can estimate the CPU and memory cost of physical execution
// operators.
type Coster interface {
	// EstimateCost cost returns the incremental CPU and memory cost for an
	// operator, or an error. Cost is dependent on physical operator type,
	// and the cardinality of inputs.
	EstimateCost(*sql.Context, relExpr, sql.StatsReader) (float64, error)
}

// Carder types can estimate the cardinality (row count) of relational
// expressions.
type Carder interface {
	// EstimateCard returns the estimate row count outputs for a relational
	// expression. Cardinality is an expression group property.
	EstimateCard(*sql.Context, relExpr, sql.StatsReader) (float64, error)
}

// relExpr wraps a sql.Node for use as a exprGroup linked list node.
// TODO: we need relExprs for every sql.Node and sql.Expression
type relExpr interface {
	fmt.Stringer
	group() *exprGroup
	next() relExpr
	setNext(relExpr)
	children() []*exprGroup
	setGroup(g *exprGroup)
	setCost(c float64)
	cost() float64
	distinct() distinctOp
	setDistinct(distinctOp)
}

type relBase struct {
	// g is this relation's expression group
	g *exprGroup
	// n is the next relExpr in the exprGroup linked list
	n relExpr
	// c is this relation's cost while costing and plan reify are separate
	c float64
	// cnt is this relations output row count
	cnt float64
	// d indicates a relExpr should be checked for distinctness
	d distinctOp
}

// relKEy is a quick identifier for avoiding duplicate work on the same
// relExpr.
// TODO: the key should be a formalized hash of 1) the operator type, and 2)
// hashes of the relExpr and scalarExpr children.
func relKey(r relExpr) uint64 {
	key := int(r.group().id)
	i := 1<<16 - 1
	for _, c := range r.children() {
		key += i * int(c.id)
		i *= 1<<16 - 1
	}
	return uint64(key)
}

type distinctOp uint8

const (
	unknownDistinctOp distinctOp = iota
	noDistinctOp
	sortedDistinctOp
	hashDistinctOp
)

func (d distinctOp) IsHash() bool {
	return d == hashDistinctOp
}

func (r *relBase) distinct() distinctOp {
	return r.d
}

func (r *relBase) setDistinct(d distinctOp) {
	r.d = d
}

func (r *relBase) group() *exprGroup {
	return r.g
}

func (r *relBase) setGroup(g *exprGroup) {
	r.g = g
}

func (r *relBase) next() relExpr {
	return r.n
}

func (r *relBase) setNext(rel relExpr) {
	r.n = rel
}

func (r *relBase) setCost(c float64) {
	r.c = c
}

func (r *relBase) cost() float64 {
	return r.c
}

func tableIdForSource(id GroupId) TableId {
	return TableId(id - 1)
}

// sourceRel represents a data source, like a tableScan, subqueryAlias,
// or list of values.
type sourceRel interface {
	relExpr
	// outputCols retuns the output schema of this data source.
	// TODO: this is more useful as a relExpr property, but we need
	// this to fix up expression indexes currently
	outputCols() sql.Schema
	name() string
	tableId() TableId
}

// joinRel represents a plan.JoinNode or plan.CrossJoin. See plan.JoinType
// for the full list.
type joinRel interface {
	relExpr
	joinPrivate() *joinBase
	group() *exprGroup
}

var _ joinRel = (*antiJoin)(nil)
var _ joinRel = (*concatJoin)(nil)
var _ joinRel = (*crossJoin)(nil)
var _ joinRel = (*leftJoin)(nil)
var _ joinRel = (*fullOuterJoin)(nil)
var _ joinRel = (*hashJoin)(nil)
var _ joinRel = (*innerJoin)(nil)
var _ joinRel = (*lookupJoin)(nil)
var _ joinRel = (*semiJoin)(nil)

type joinBase struct {
	*relBase

	op     plan.JoinType
	filter []sql.Expression
	left   *exprGroup
	right  *exprGroup
}

func (r *joinBase) children() []*exprGroup {
	return []*exprGroup{r.left, r.right}
}

func (r *joinBase) joinPrivate() *joinBase {
	return r
}

// copy creates a joinBase with the same underlying join expression.
// note: it is important to copy the base node to avoid cyclical
// relExpr references in the exprGroup linked list.
func (r *joinBase) copy() *joinBase {
	return &joinBase{
		relBase: &relBase{
			g: r.g,
			n: r.n,
			c: r.c,
		},
		op:     r.op,
		filter: r.filter,
		left:   r.left,
		right:  r.right,
	}
}

type lookup struct {
	source   string
	index    sql.Index
	keyExprs []sql.Expression
	nullmask []bool

	parent *joinBase
}

type indexScan struct {
	source string
	idx    sql.Index

	parent *joinBase
}

var ExprDefs support.GenDefs = []support.MemoDef{ // alphabetically sorted
	{
		Name:   "crossJoin",
		IsJoin: true,
	},
	{
		Name:   "innerJoin",
		IsJoin: true,
	},
	{
		Name:   "leftJoin",
		IsJoin: true,
	},
	{
		Name:   "semiJoin",
		IsJoin: true,
	},
	{
		Name:   "antiJoin",
		IsJoin: true,
	},
	{
		Name:   "lookupJoin",
		IsJoin: true,
		Attrs: [][2]string{
			{"lookup", "*lookup"},
		},
	},
	{
		Name:   "concatJoin",
		IsJoin: true,
		Attrs: [][2]string{
			{"concat", "[]*lookup"},
		},
	},
	{
		Name:   "hashJoin",
		IsJoin: true,
		Attrs: [][2]string{
			{"innerAttrs", "[]sql.Expression"},
			{"outerAttrs", "[]sql.Expression"},
		},
	},
	{
		Name:   "mergeJoin",
		IsJoin: true,
		Attrs: [][2]string{
			{"innerScan", "*indexScan"},
			{"outerScan", "*indexScan"},
		},
	},
	{
		Name:   "fullOuterJoin",
		IsJoin: true,
	},
	{
		Name:       "tableScan",
		SourceType: "*plan.ResolvedTable",
	},
	{
		Name:       "values",
		SourceType: "*plan.ValueDerivedTable",
	},
	{
		Name:       "tableAlias",
		SourceType: "*plan.TableAlias",
	},
	{
		Name:       "recursiveTable",
		SourceType: "*plan.RecursiveTable",
	},
	{
		Name:       "recursiveCte",
		SourceType: "*plan.RecursiveCte",
	},
	{
		Name:       "subqueryAlias",
		SourceType: "*plan.SubqueryAlias",
	},
	{
		Name:       "max1Row",
		SourceType: "sql.NameableNode",
	},
	{
		Name:       "tableFunc",
		SourceType: "sql.TableFunction",
	},
	{
		Name:       "emptyTable",
		SourceType: "*plan.EmptyTable",
	},
	{
		Name:    "project",
		IsUnary: true,
		Attrs: [][2]string{
			{"projections", "[]sql.Expression"},
		},
	},
	{
		Name:     "distinct",
		IsUnary:  true,
		SkipExec: true,
	},
}
