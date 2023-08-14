package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// scope tracks relational dependencies necessary to type check expressions,
// resolve name definitions, and build relational nodes.
type scope struct {
	b      *Builder
	parent *scope
	ast    ast.SQLNode
	node   sql.Node

	subquery bool

	// cols are definitions provided by this scope
	cols []scopeColumn
	// extraCols are auxillary output columns required
	// for sorting or grouping
	extraCols []scopeColumn
	// redirectCol is used for natural join right-table
	// attributes that redirect to the left table intersection
	redirectCol map[string]scopeColumn
	// tables are the list of table definitions in this scope
	tables map[string]tableId
	// ctes are common table expressions defined in this scope
	// TODO these should be case-sensitive
	ctes map[string]*scope
	// groupBy collects aggregation functions and inputs
	groupBy *groupBy
	// windowFuncs is a list of window functions in the current scope
	windowFuncs []scopeColumn
	windowDefs  map[string]*sql.WindowDefinition
	// exprs collects unique expression ids for reference
	exprs map[string]columnId
	proc  *procCtx
}

func (s *scope) resolveColumn(table, col string, checkParent bool) (scopeColumn, bool) {
	// procedure params take precedence
	if table == "" && checkParent && s.procActive() {
		col, ok := s.proc.GetVar(col)
		if ok {
			return col, true
		}
	}
	var found scopeColumn
	var foundCand bool
	for _, c := range s.cols {
		if strings.EqualFold(c.col, col) && (c.table == table || table == "") {
			if foundCand {
				if !s.b.TriggerCtx().Call && len(s.b.TriggerCtx().UnresolvedTables) > 0 {
					c, ok := s.triggerCol(table, col)
					if ok {
						return c, true
					}
				}
				if c.table == OnDupValuesPrefix {
					return found, true
				} else if found.table == OnDupValuesPrefix {
					return c, true
				}
				err := sql.ErrAmbiguousColumnName.New(col, []string{c.table, found.table})
				if c.table == "" {
					err = sql.ErrAmbiguousColumnOrAliasName.New(c.col)
				}
				s.handleErr(err)
			}
			found = c
			foundCand = true
		}
	}
	if foundCand {
		return found, true
	}
	if c, ok := s.redirectCol[fmt.Sprintf("%s.%s", table, col)]; ok {
		return c, true
	}

	if s.groupBy != nil {
		if c, ok := s.groupBy.outScope.resolveColumn(table, col, false); ok {
			return c, true
		}
	}

	if !s.b.TriggerCtx().Call && len(s.b.TriggerCtx().UnresolvedTables) > 0 {
		c, ok := s.triggerCol(table, col)
		if ok {
			return c, true
		}
	}

	if !checkParent || s.parent == nil {
		return scopeColumn{}, false
	}

	c, foundCand := s.parent.resolveColumn(table, col, true)
	if !foundCand {
		return scopeColumn{}, false
	}

	return c, true
}

func (s *scope) hasTable(table string) bool {
	_, ok := s.tables[strings.ToLower(table)]
	if ok {
		return true
	}
	if s.parent != nil {
		return s.parent.hasTable(table)
	}
	return false
}

// triggerCol is used to hallucinate a new column during trigger DDL
// when we fail a resolveColumn.
func (s *scope) triggerCol(table, col string) (scopeColumn, bool) {
	// hallucinate tablecol
	for _, t := range s.b.TriggerCtx().UnresolvedTables {
		if strings.EqualFold(t, table) {
			col := scopeColumn{table: t, col: col}
			id := s.newColumn(col)
			col.id = id
			return col, true
		}
	}
	if table == "" {
		col := scopeColumn{col: col}
		id := s.newColumn(col)
		col.id = id
		return col, true
	}
	return scopeColumn{}, false
}

// getExpr returns a columnId if the given expression has
// been built.
func (s *scope) getExpr(name string, checkCte bool) (columnId, bool) {
	n := strings.ToLower(name)
	id, ok := s.exprs[n]
	if !ok && s.groupBy != nil {
		id, ok = s.groupBy.outScope.getExpr(n, checkCte)
	}
	if !ok && checkCte && s.ctes != nil {
		for _, cte := range s.ctes {
			id, ok = cte.getExpr(n, false)
			if ok {
				break
			}
		}
	}
	// TODO: possibly want to look in parent scopes
	if !ok && s.parent != nil {
		return s.parent.getExpr(name, checkCte)
	}
	return id, ok
}

func (s *scope) procActive() bool {
	return s.proc != nil
}

func (s *scope) initProc() {
	s.proc = &procCtx{
		s:          s,
		conditions: make(map[string]*plan.DeclareCondition),
		cursors:    make(map[string]struct{}),
		vars:       make(map[string]scopeColumn),
		labels:     make(map[string]bool),
	}
}

// initGroupBy creates a container scope for aggregation
// functions and function inputs.
func (s *scope) initGroupBy() {
	s.groupBy = &groupBy{outScope: s.replace()}
}

// setTableAlias updates column definitions in this scope to
// appear sourced from a new table name.
func (s *scope) setTableAlias(t string) {
	t = strings.ToLower(t)
	var oldTable string
	for i := range s.cols {
		beforeColStr := s.cols[i].String()
		if oldTable == "" {
			oldTable = s.cols[i].table
		}
		s.cols[i].table = t
		id, ok := s.getExpr(beforeColStr, true)
		if ok {
			// todo better way to do projections
			delete(s.exprs, beforeColStr)
		}
		s.exprs[strings.ToLower(s.cols[i].String())] = id
	}
	id, ok := s.tables[oldTable]
	if !ok {
		return
	}
	delete(s.tables, oldTable)
	if s.tables == nil {
		s.tables = make(map[string]tableId)
	}
	s.tables[t] = id
}

// setColAlias updates the column name definitions for this scope
// to the names in the input list.
func (s *scope) setColAlias(cols []string) {
	if len(cols) != len(s.cols) {
		err := sql.ErrColumnCountMismatch.New()
		s.b.handleErr(err)
	}
	ids := make([]columnId, len(cols))
	for i := range s.cols {
		beforeColStr := s.cols[i].String()
		id, ok := s.getExpr(beforeColStr, true)
		if ok {
			// todo better way to do projections
			delete(s.exprs, beforeColStr)
		}
		ids[i] = id
		delete(s.exprs, beforeColStr)
	}
	for i := range s.cols {
		name := strings.ToLower(cols[i])
		s.cols[i].col = name
		s.exprs[s.cols[i].String()] = ids[i]
	}
}

// push creates a new scope referencing the current scope as a
// parent. Variables in the new scope will have name visibility
// into this scope.
func (s *scope) push() *scope {
	new := &scope{
		b:      s.b,
		parent: s,
	}
	if s.procActive() {
		new.initProc()
	}
	return new
}

// replace creates a new scope with the same parent definition
// visibility as the current scope. Useful for groupby and subqueries
// that have more complex naming hierarchy.
func (s *scope) replace() *scope {
	if s == nil {
		return &scope{}
	}
	return &scope{
		b:      s.b,
		parent: s.parent,
	}
}

// copy produces an identical scope with copied references.
func (s *scope) copy() *scope {
	if s == nil {
		return nil
	}

	ret := *s
	if ret.node != nil {
		ret.node, _ = DeepCopyNode(s.node)
	}
	if s.tables != nil {
		ret.tables = make(map[string]tableId, len(s.tables))
		for k, v := range s.tables {
			ret.tables[k] = v
		}
	}
	if s.ctes != nil {
		ret.ctes = make(map[string]*scope, len(s.ctes))
		for k, v := range s.ctes {
			ret.ctes[k] = v
		}
	}
	if s.exprs != nil {
		ret.exprs = make(map[string]columnId, len(s.exprs))
		for k, v := range s.exprs {
			ret.exprs[k] = v
		}
	}
	if s.groupBy != nil {
		gbCopy := *s.groupBy
		ret.groupBy = &gbCopy
	}
	if s.cols != nil {
		ret.cols = make([]scopeColumn, len(s.cols))
		copy(ret.cols, s.cols)
	}

	return &ret
}

// DeepCopyNode copies a sql.Node.
func DeepCopyNode(node sql.Node) (sql.Node, error) {
	n, _, err := transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		e, err := transform.Clone(e)
		return e, transform.NewTree, err
	})
	return n, err
}

// addCte adds a cte definition to this scope for table resolution.
func (s *scope) addCte(name string, cteScope *scope) {
	if s.ctes == nil {
		s.ctes = make(map[string]*scope)
	}
	s.ctes[name] = cteScope
	s.addTable(name)
}

// getCte attempts to resolve a table name as a cte definition.
func (s *scope) getCte(name string) *scope {
	checkScope := s
	for checkScope != nil {
		if checkScope.ctes != nil {
			cte, ok := checkScope.ctes[strings.ToLower(name)]
			if ok {
				return cte
			}
		}
		checkScope = checkScope.parent
	}
	return nil
}

// redirect overwrites a definition with an alias rewrite,
// without preventing us from resolving the original column.
// This is used for resolving natural join projections.
func (s *scope) redirect(from, to scopeColumn) {
	if s.redirectCol == nil {
		s.redirectCol = make(map[string]scopeColumn)
	}
	s.redirectCol[from.String()] = to
}

// addColumn interns and saves the given column to this scope.
// todo: new IR should absorb interning and use bitmaps for
// column identity
func (s *scope) addColumn(col scopeColumn) {
	s.cols = append(s.cols, col)
	if s.exprs == nil {
		s.exprs = make(map[string]columnId)
	}
	if col.table != "" {
		s.exprs[fmt.Sprintf("%s.%s", strings.ToLower(col.table), strings.ToLower(col.col))] = col.id
	} else {
		s.exprs[strings.ToLower(col.col)] = col.id
	}
	return
}

// newColumn adds the column to the current scope and assigns a
// new columnId for referencing. newColumn builds a new expression
// reference, whereas addColumn only adds a preexisting expression
// definition to a given scope.
func (s *scope) newColumn(col scopeColumn) columnId {
	s.b.colId++
	col.id = s.b.colId
	s.addColumn(col)
	s.addTable(col.table)
	return col.id
}

// addTable records adds a table name defined in this scope
func (s *scope) addTable(name string) {
	if name == "" {
		return
	}
	if s.tables == nil {
		s.tables = make(map[string]tableId)
	}
	if _, ok := s.tables[name]; !ok {
		s.b.tabId++
		s.tables[name] = s.b.tabId
	}
}

// addExtraColumn marks an auxiliary column used in an
// aggregation, sorting, or having clause.
func (s *scope) addExtraColumn(col scopeColumn) {
	s.extraCols = append(s.extraCols, col)
}

func (s *scope) addColumns(cols []scopeColumn) {
	s.cols = append(s.cols, cols...)
}

// appendColumnsFromScope merges column definitions for
// multi-relational expressions.
func (s *scope) appendColumnsFromScope(src *scope) {
	s.cols = append(s.cols, src.cols...)
	if len(src.exprs) > 0 && s.exprs == nil {
		s.exprs = make(map[string]columnId)
	}
	for k, v := range src.exprs {
		s.exprs[k] = v
	}
	if len(src.redirectCol) > 0 && s.redirectCol == nil {
		s.redirectCol = make(map[string]scopeColumn)
	}
	for k, v := range src.redirectCol {
		s.redirectCol[k] = v
	}
	if len(src.tables) > 0 && s.tables == nil {
		s.tables = make(map[string]tableId)
	}
	for k, v := range src.tables {
		s.tables[k] = v
	}
	// these become pass-through columns in the new scope.
	for i := len(src.cols); i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

func (s *scope) handleErr(err error) {
	panic(parseErr{err})
}

// tableId and columnId are temporary ways to track expression
// and name uniqueness.
// todo: the plan format should track these
type tableId uint16
type columnId uint16

type scopeColumn struct {
	db          string
	table       string
	col         string
	originalCol string
	id          columnId
	typ         sql.Type
	scalar      sql.Expression
	nullable    bool
	descending  bool
}

// empty returns true if a scopeColumn is the null value
func (c scopeColumn) empty() bool {
	return c.id == 0
}

// scalarGf returns a getField reference to this column's expression.
func (c scopeColumn) scalarGf() sql.Expression {
	if c.scalar != nil {
		if p, ok := c.scalar.(*expression.ProcedureParam); ok {
			return p
		}
	}
	if c.originalCol != "" {
		return expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.originalCol, c.nullable)
	}
	return expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
}

func (c scopeColumn) String() string {
	if c.table == "" {
		return c.col
	} else {
		return fmt.Sprintf("%s.%s", c.table, c.col)
	}
}
