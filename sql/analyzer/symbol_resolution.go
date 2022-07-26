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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"gopkg.in/src-d/go-errors.v1"
	"strings"
)

type buildScope struct {
	cols   map[tableCol]struct{}
	parent *buildScope
	b      *builder

	// stars and unqualifiedStars indicate buildScope dependency overrides
	stars           map[string]struct{}
	unqualifiedStar bool

	qualified   map[tableCol]struct{}
	unqualified map[tableCol]struct{}
	aliased     map[tableCol]struct{}
}

func (s *buildScope) push() *buildScope {
	r := s.b.newScope()
	r.parent = s
	return r
}

func (s *buildScope) format() string {
	b := strings.Builder{}
	var sep string
	for i := range s.cols {
		b.WriteString(fmt.Sprintf("%s%s", s.cols[i], sep))
		sep = ", "
	}
	return b.String()
}

func (s *buildScope) addCols(cols ...tableCol) {
	for _, c := range cols {
		s.cols[c] = struct{}{}
	}
}

func (s *buildScope) appendScopeCols(other *buildScope) {
	for i := range other.cols {
		s.cols[i] = struct{}{}
	}
	for t := range other.stars {
		s.stars[t] = struct{}{}
	}
	s.unqualifiedStar = s.unqualifiedStar || other.unqualifiedStar
}

func (s *buildScope) hasCol(col tableCol) bool {
	_, ok := s.cols[col]
	return ok
}

func (s *buildScope) addColsFromAlias(outer *buildScope, table, alias string, columns []string) {
	_, starred := outer.stars[alias]
	for i := range columns {
		baseCol := tableCol{table: table, col: columns[i]}
		aliasCol := tableCol{table: alias, col: columns[i]}
		if starred || outer.hasCol(aliasCol) {
			// if the outer scope requests an aliased column
			// a table lower in the tree must provide the source
			s.addCols(baseCol)
		}
	}
	for t := range outer.stars {
		if t == alias {
			s.stars[table] = struct{}{}
		}
	}
	s.unqualifiedStar = s.unqualifiedStar || outer.unqualifiedStar
}

// addColsFromNode adds all of a node's column dependencies to this buildScope
func (s *buildScope) addColsFromNode(n sql.Node) {
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return
	}
	for _, e := range ne.Expressions() {
		transform.InspectExpr(e, func(e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.Alias:
				switch e := e.Child.(type) {
				case *expression.GetField:
					s.addCols(tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
				case *expression.UnresolvedColumn:
					s.addCols(tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
				default:
				}
			case *expression.GetField:
				s.addCols(tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
			case *expression.UnresolvedColumn:
				s.addCols(tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
			case *expression.Star:
				if len(e.Table) > 0 {
					s.stars[strings.ToLower(e.Table)] = struct{}{}
				} else {
					s.unqualifiedStar = true
				}
			default:
			}
			return false
		})
	}
}

//qualifyCols recursively updates
func (s *buildScope) qualifyCols(cols ...tableCol) {

}

type builder struct {
	ctx        *sql.Context
	root       sql.Node
	tableCols  map[sql.RelId][]tableCol
	id         sql.RelId
	inSubquery string
}

func newBuilder(ctx *sql.Context, root sql.Node) *builder {
	return &builder{
		ctx:       ctx,
		root:      root,
		tableCols: make(map[sql.RelId][]tableCol),
	}
}

func compareTableCol(i, j tableCol) int {
	if i.table < j.table {
		return -1
	} else if i.table > j.table {
		return 1
	} else if i.col < j.col {
		return -1
	} else if i.col > j.col {
		return 1
	}
	return 0
}

// mergeTableCols combines two sets of []tableCol, carefully maintaining
// ordering to keep the projection outputs deterministic.
func mergeTableCols(x, y []tableCol) []tableCol {
	if len(x) == 0 {
		return y
	} else if len(y) == 0 {
		return x
	}
	newCols := make([]tableCol, len(x)+len(y))
	var i, j, k int
	for i < len(x) && j < len(y) {
		cmp := compareTableCol(x[i], y[j])
		if cmp < 0 {
			newCols[k] = x[i]
			i++
		} else if cmp > 0 {
			newCols[k] = y[j]
			j++
		} else {
			newCols[k] = x[i]
			i++
			j++
		}
		k++
	}
	for i < len(x) {
		newCols[k] = x[i]
		i++
		k++
	}
	for j < len(y) {
		newCols[k] = y[j]
		j++
		k++
	}
	return newCols[:k]
}

// addTableCols combines the buildScope dependencies, maintaining schema sort ordering
func (b *builder) addTableCols(id sql.RelId, other []tableCol) {
	curr, _ := b.tableCols[id]
	b.tableCols[id] = mergeTableCols(curr, other)
}

func (b *builder) delTableCols(id sql.RelId) {
	delete(b.tableCols, id)
}

func (b *builder) buildTable(n sql.Node, inScope *buildScope) *buildScope {
	table := getTable(n)
	t, ok := table.(sql.ProjectedTable)
	outScope := inScope.push()
	outScope.appendScopeCols(inScope)

	if !ok {
		return outScope
	}

	if t.Name() == sql.DualTableName {
		return outScope
	}

	_, selectStar := outScope.stars[t.Name()]
	if outScope.unqualifiedStar {
		selectStar = true
	}

	cols := make([]tableCol, 0)
	source := strings.ToLower(t.Name())
	for _, col := range t.Schema() {
		c := tableCol{table: source, col: strings.ToLower(col.Name)}
		if selectStar || inScope.hasCol(c) {
			cols = append(cols, c)
		}
	}
	if len(cols) > 0 {
		b.addTableCols(n.(sql.RelationalNode).RelationalId(), cols)
		//outScope.qualifyCols(cols...)
	}
	return outScope
}

func (b *builder) buildValues(n sql.Node, inScope *buildScope) *buildScope {
	// ex: select x from (values row(1,2), row(3,4)) as t(x,y)
	//todo push cols into value nodes
	return inScope
}

func (b *builder) buildTableAlias(n *plan.TableAlias, inScope *buildScope) *buildScope {
	tab := n.Child.(sql.Table)
	outScope := inScope.push()
	cols := make([]string, len(tab.Schema()))
	for i, c := range tab.Schema() {
		cols[i] = c.Name
	}

	b.id++
	n = n.WithRelationalId(b.id).(*plan.TableAlias)

	outScope.addColsFromAlias(inScope, tab.Name(), n.Name(), cols)
	return outScope
}

func (b *builder) buildNaturalJoin(n sql.Node, inScope *buildScope) *buildScope {
	//todo: natural joins will project a columns with same name from each table,
	// but only recorded once in the root-project
	return inScope
}

func (b *builder) buildJoin(n sql.Node, inScope *buildScope) *buildScope {
	outScope := b.buildGenericNode(n, inScope)

	// both relations need the same outer scope
	innerScope := outScope.push()
	outerScope := outScope.push()
	innerScope.appendScopeCols(inScope)
	outerScope.appendScopeCols(inScope)
	_ = b.walk(n.(sql.BinaryNode).Left(), innerScope)
	_ = b.walk(n.(sql.BinaryNode).Right(), outerScope)

	return outScope
}

func (b *builder) buildUnion(n *plan.Union, inScope *buildScope) *buildScope {
	leftScope := inScope.push()
	rightScope := inScope.push()
	outScope := b.walk(n.Left(), leftScope)
	rOut := b.walk(n.Right(), rightScope)
	outScope.appendScopeCols(rOut)
	return outScope
}

//todo(max): We don't do a good job differentiating between subquery and node scopes.
// There should be two levels of nesting? PG appears to just call the
// whole optimizer recursively instead of passing dependencies between subqueries.
func (b *builder) buildSubqueryAlias(n *plan.SubqueryAlias, inScope *buildScope) *buildScope {
	// ex: select x from (select x,y from a)
	// subquery is a row source, it should accept projections?
	subqueryScope := inScope.push()
	oldSq := b.inSubquery
	b.inSubquery = n.Name()
	_ = b.walk(n.Child, subqueryScope)
	b.inSubquery = oldSq
	return inScope
}

func (b *builder) buildSubqueryExpr(n *plan.Subquery, inScope *buildScope) *buildScope {
	// ex: select b.y in (select a.x from a where a.x = b.x)
	// ex: select b.y in (select c.w from (select a.* from a) as c where c.x = b.x)
	outScope := b.walk(n.Query, inScope)
	return outScope
}

func (b *builder) buildCte(n *plan.Subquery, inScope *buildScope) *buildScope {
	return inScope
}

func (b *builder) buildGroupBy(n *plan.GroupBy, inScope *buildScope) *buildScope {
	return inScope
}

func (b *builder) buildWindow(n *plan.Window, inScope *buildScope) *buildScope {
	return inScope
}

func (b *builder) buildGenericNode(n sql.Node, inScope *buildScope) *buildScope {
	inScope.addColsFromNode(n)
	return inScope
}

func (b *builder) newScope() *buildScope {
	return &buildScope{
		b:     b,
		cols:  make(map[tableCol]struct{}),
		stars: make(map[string]struct{}),
	}
}

// pushdownProjections tags pushdown targets, top-down collects node dependencies, and then bottom-up
// rebuilds the tree projecting node dependencies.
func pushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, s *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if s != nil {
		return n, transform.SameTree, nil
	}
	b := newBuilder(ctx, n)
	err := b.assignRelIds()
	if err != nil {
		if errors.Is(err, ErrAlreadyPushedProjections) {
			return n, transform.SameTree, nil
		} else if errors.Is(err, ErrProjectWithSubqueryFailed) {
			return n, transform.SameTree, nil
		} else if errors.Is(err, ErrProjectIntoDML) {
			return n, transform.SameTree, nil
		} else if errors.Is(err, ErrProjectIntoNaturalJoinFailed) {
			return n, transform.SameTree, nil
		}
		return nil, transform.SameTree, err
	}
	emptyScope := b.newScope()
	b.walk(b.root, emptyScope)
	return b.finish()
}

var ErrAlreadyPushedProjections = errors.NewKind("already pushed projections")
var ErrProjectWithSubqueryFailed = errors.NewKind("project with subquery expression failed")
var ErrProjectIntoDML = errors.NewKind("project into DML expression failed")
var ErrProjectIntoNaturalJoinFailed = errors.NewKind("project into natural join failed")

// assignRelIds is a simple way to connect top-down info collection
// with bottom-up rebuilding of the tree.
//todo(max): This is an adaptor to account for how we can't build a tree top-down.
// If qualifying columns, deps, projections, filters, etc was the first step
// post-parsing, the split logic here would be unnecessary.
func (b *builder) assignRelIds() error {
	n, same, err := transform.NodeWithOpaque(b.root, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if ne, ok := n.(sql.Expressioner); ok {
			//todo(max): support for subquery expressions
			for _, e := range ne.Expressions() {
				foundSq := transform.InspectExpr(e, func(e sql.Expression) bool {
					_, ok := e.(*plan.Subquery)
					return ok
				})
				if foundSq {
					return nil, transform.SameTree, ErrProjectWithSubqueryFailed.New()
				}
			}
		}

		switch n := n.(type) {
		case *plan.DeleteFrom, *plan.InsertInto, *plan.Update:
			//todo(max): support for DML
			return nil, transform.SameTree, ErrProjectIntoDML.New()
		case *plan.NaturalJoin:
			//todo(max): support for natural join
			return nil, transform.SameTree, ErrProjectIntoNaturalJoinFailed.New()
		case sql.RelationalNode:
			if n.RelationalId() > 0 {
				return nil, transform.SameTree, ErrAlreadyPushedProjections.New()
			}
			b.id++
			return n.WithRelationalId(b.id), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
	if err != nil {
		return err
	}
	if same {
		return nil
	}
	b.root = n
	return nil
}

// finish rebuilds a tree, pushing dependencies into table nodes that we
// collected in the walk step
func (b *builder) finish() (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(b.root, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			cols, ok := b.tableCols[n.RelationalId()]
			if !ok {
				return n, transform.SameTree, nil
			}
			projections := make([]string, len(cols))
			for i := range cols {
				projections[i] = cols[i].col
			}
			tab := getTable(n)
			ptab, ok := tab.(sql.ProjectedTable)
			if !ok {
				return n, transform.SameTree, nil
			}

			ret, err := n.WithTable(ptab.WithProjections(projections))
			if err != nil {
				return nil, transform.SameTree, err
			}
			dec := plan.NewDecoratedNode(fmt.Sprintf("Projected table access on %v", projections), ret)
			return dec, transform.NewTree, nil
		case *plan.Values:
			return n, transform.SameTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// findSubqueryExpr searches for a *plan.Subquery in a single node,
// returning the subquery or nil
func findSubqueryExpr(n sql.Node) *plan.Subquery {
	var sq *plan.Subquery
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return nil
	}
	for _, e := range ne.Expressions() {
		found := transform.InspectExpr(e, func(e sql.Expression) bool {
			if e, ok := e.(*plan.Subquery); ok {
				sq = e
				return true
			}
			return false
		})
		if found {
			return sq
		}
	}
	return nil
}

// walk is a top-down traversal of the plan tree that builds a linked-list
// of buildScope node buildScope dependencies. Dependencies flow
// downwards linearly from the tree root to plan.ResolvedTable leaves, and
// in the future other sql.RelationalNode types. This is broad enough to
// absorb several roles:
//   1) This can qualify expressions if upon finding a buildScope row source,
//      we traverse upwards in the linked list bookkeeping columns between
//      three buckets: i) unqualified, ii) qualified, iii) alias. Conflicts
//      between multiple column sources would provide ambiguous column errors.
//   2) Type checking -- when all of a node's columns are qualified, we can
//      verify that node's expression type semantics.
//   3) Filter pushdown - filters can move to their referenced relational
//      nodes. In the case of multi-relation filters, they would move to the
//      top-level join node that includes all referenced relationas.
//   4) Column functional dependencies?
//   3) Builder methods can be associated with specific optimization rules
//      that only apply to specific nodes.
func (b *builder) walk(n sql.Node, inScope *buildScope) *buildScope {
	var outScope = inScope
	transform.Inspect(n, func(n sql.Node) bool {
		inScope = outScope

		if sqe := findSubqueryExpr(n); sqe != nil {
			inScope = b.buildSubqueryExpr(sqe, inScope)
		}

		switch n := n.(type) {
		case *plan.ResolvedTable, *plan.IndexedTableAccess:
			outScope = b.buildTable(n, inScope)
		case *plan.Values:
			outScope = b.buildValues(n, inScope)
		case *plan.TableAlias:
			outScope = b.buildTableAlias(n, inScope)
		case *plan.Union:
			outScope = b.buildUnion(n, inScope)
			return false
		case *plan.SubqueryAlias:
			outScope = b.buildSubqueryAlias(n, inScope)
		case plan.JoinNode, *plan.CrossJoin:
			outScope = b.buildJoin(n, inScope)
		case *plan.NaturalJoin:
			outScope = b.buildNaturalJoin(n, inScope)
		case *plan.GroupBy:
			outScope = b.buildGenericNode(n, inScope)
		case *plan.Filter, *plan.Project, *plan.Sort, *plan.Window:
			outScope = b.buildGenericNode(n, inScope)
		}
		return true
	})
	return outScope
}
