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
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func validateUniqueTableNames(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// getTableAliases will error if any table name / alias is repeated
	_, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, err
}

// deferredColumn is a wrapper on UnresolvedColumn used to defer the resolution of the column because it may require
// some work done by other analyzer phases.
type deferredColumn struct {
	*expression.UnresolvedColumn
}

func (dc *deferredColumn) DebugString() string {
	return fmt.Sprintf("deferred(%s)", dc.UnresolvedColumn.String())
}

// IsNullable implements the Expression interface.
func (*deferredColumn) IsNullable() bool {
	return true
}

// Children implements the Expression interface.
func (*deferredColumn) Children() []sql.Expression { return nil }

// WithChildren implements the Expression interface.
func (dc *deferredColumn) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(dc, len(children), 0)
	}
	return dc, nil
}

type tableCol struct {
	table string
	col   string
}

func newTableCol(table, col string) tableCol {
	return tableCol{
		table: strings.ToLower(table),
		col:   strings.ToLower(col),
	}
}

var _ sql.Tableable = tableCol{}
var _ sql.Nameable = tableCol{}

func (tc tableCol) Table() string {
	return tc.table
}

func (tc tableCol) Name() string {
	return tc.col
}

func (tc tableCol) String() string {
	if tc.table != "" {
		return fmt.Sprintf("%s.%s", tc.table, tc.col)
	} else {
		return tc.col
	}
}

type indexedCol struct {
	*sql.Column
	index int
}

// column is the common interface that groups UnresolvedColumn and deferredColumn.
type column interface {
	sql.Nameable
	sql.Tableable
	sql.Expression
}

// scopeLevelSymbols tracks available table and column name symbols at a specific scope level for a query. Each nested
// subquery in a statement represents an additional scope level.
type scopeLevelSymbols struct {
	availableColumns   map[string][]string
	availableAliases   map[string][]*expression.Alias
	availableTables    map[string]string
	availableTableCols map[tableCol]struct{}
}

func newScopeLevelSymbols() *scopeLevelSymbols {
	return &scopeLevelSymbols{
		availableColumns:   make(map[string][]string),
		availableAliases:   make(map[string][]*expression.Alias),
		availableTables:    make(map[string]string),
		availableTableCols: make(map[tableCol]struct{}),
	}
}

// availableNames tracks available table and column name symbols at each scope level for a query, where level 0
// is the top-level, outermost scope and each additional level is one layer of query scope inward.
type availableNames map[int]*scopeLevelSymbols

// indexColumn adds a column with the given table and column name at the given scope level
func (a availableNames) indexColumn(table, col string, scopeLevel int) {
	col = strings.ToLower(col)
	_, ok := a[scopeLevel]
	if !ok {
		a[scopeLevel] = newScopeLevelSymbols()
	}
	tableLower := strings.ToLower(table)
	if !stringContains(a[scopeLevel].availableColumns[col], tableLower) {
		a[scopeLevel].availableColumns[col] = append(a[scopeLevel].availableColumns[col], tableLower)
		a[scopeLevel].availableTableCols[tableCol{table: tableLower, col: col}] = struct{}{}
	}
}

// levels returns a sorted list of the scope levels for these available name symbols, starting with the most specific,
// or innermost, level and ending with the outermost level (i.e. the top level query).
func (a availableNames) levels() []int {
	levels := make([]int, len(a))
	i := 0
	for l := range a {
		levels[i] = l
		i++
	}
	sort.Sort(sort.Reverse(sort.IntSlice(levels)))
	return levels
}

// indexAlias adds an alias name to track at the specified scope level
func (a availableNames) indexAlias(e *expression.Alias, scopeLevel int) {
	name := strings.ToLower(e.Name())
	_, ok := a[scopeLevel]
	if !ok {
		a[scopeLevel] = newScopeLevelSymbols()
	}
	_, ok = a[scopeLevel].availableAliases[name]
	if !ok {
		a[scopeLevel].availableAliases[name] = make([]*expression.Alias, 0)
	}
	a[scopeLevel].availableAliases[name] = append(a[scopeLevel].availableAliases[name], e)
}

// indexTable adds a table with the given name at the specified scope level
func (a availableNames) indexTable(alias, name string, scopeLevel int) {
	alias = strings.ToLower(alias)
	_, ok := a[scopeLevel]
	if !ok {
		a[scopeLevel] = newScopeLevelSymbols()
	}
	a[scopeLevel].availableTables[alias] = strings.ToLower(name)
}

func (a availableNames) tablesAtLevel(scopeLevel int) map[string]string {
	return a[scopeLevel].availableTables
}

func (a availableNames) allTables() []string {
	var allTables []string
	for _, symbols := range a {
		for name, table := range symbols.availableTables {
			allTables = append(allTables, name, table)
		}
	}
	return dedupStrings(allTables)
}

// aliasesAndTablesForColumnAtLevel returns a slice of strings indicating how many distinct alias definitions are available
// for the specified column name, as well as a slice of strings indicating which distinct tables are available with that
// column name.
func (a availableNames) aliasesAndTablesForColumnAtLevel(column string, scopeLevel int) ([]string, []string) {
	tableNames := a[scopeLevel].availableColumns[column]
	aliasesFound := make([]string, 0, len(tableNames))
	tablesFound := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		if tableName == "" {
			// Regardless of the number of aliases defined with a specific alias name, availableColumns
			// currently tracks a single empty string to represent them, so check in another datastructure
			// to see how many alias definitions actually used this name.
			for range a[scopeLevel].availableAliases[column] {
				aliasesFound = append(aliasesFound, tableName)
			}
		} else {
			tablesFound = append(tablesFound, tableName)
		}
	}

	return aliasesFound, tablesFound
}

func (a availableNames) hasTableCol(tc tableCol) bool {
	for scopeLevel := range a {
		_, ok := a[scopeLevel].availableTableCols[tc]
		if ok {
			return true
		}
	}
	return false
}

func dedupStrings(in []string) []string {
	var seen = make(map[string]struct{})
	var result []string
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}
	return result
}

// findOnDupUpdateLeftExprs gathers all the left expressions for statements in InsertInto.OnDupExprs
// onDupExprs are always set with a column on the left
func findOnDupUpdateLeftExprs(onDupExprs []sql.Expression) map[*expression.UnresolvedColumn]bool {
	onDupUpdateLeftExprs := map[*expression.UnresolvedColumn]bool{}
	for _, e := range onDupExprs {
		if sf, ok := e.(*expression.SetField); ok {
			if uc, ok := sf.Left.(*expression.UnresolvedColumn); ok {
				onDupUpdateLeftExprs[uc] = true
			}
		}
	}
	return onDupUpdateLeftExprs
}

// qualifyColumns assigns a table to any column expressions that don't have one already
func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	originalNode := n

	// Calculate the available symbols BEFORE we get into a transform function, since symbols need to be calculated
	// on the full scope; otherwise transform looks at sub-nodes and calculates a partial view of what is available.
	symbols := getAvailableNamesByScope(n, scope)

	var onDupUpdateSymbols availableNames
	var onDupUpdateLeftExprs map[*expression.UnresolvedColumn]bool
	if in, ok := n.(*plan.InsertInto); ok && len(in.OnDupExprs) > 0 {
		inNoSrc := plan.NewInsertInto(
			in.Database(),
			in.Destination,
			nil,
			in.IsReplace,
			in.ColumnNames,
			in.OnDupExprs,
			in.Ignore,
		)
		onDupUpdateSymbols = getAvailableNamesByScope(inNoSrc, scope)
		onDupUpdateLeftExprs = findOnDupUpdateLeftExprs(in.OnDupExprs)
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if _, ok := n.(sql.Expressioner); !ok || n.Resolved() {
			return n, transform.SameTree, nil
		}

		// Don't qualify unresolved JSON tables, wait for joins
		if jt, ok := n.(*plan.JSONTable); ok {
			if !jt.Resolved() {
				return n, transform.SameTree, nil
			}
		}

		// Updates need to have check constraints qualified since multiple tables could be involved
		sameCheckConstraints := true
		if nn, ok := n.(*plan.Update); ok {
			newChecks, err := qualifyCheckConstraints(nn)
			if err != nil {
				return n, transform.SameTree, err
			}
			nn.Checks = newChecks
			n = nn
			sameCheckConstraints = false
		}

		// Before we can qualify references in a GroupBy node, we need to see if any aliases
		// were defined and then used in grouping expressions
		sameGroupBy := true
		if groupBy, ok := n.(*plan.GroupBy); ok {
			newGroupBy, identity, err := identifyGroupingAliasReferences(groupBy)
			if err != nil {
				return originalNode, transform.SameTree, err
			}
			if identity == transform.NewTree {
				n = newGroupBy
				sameGroupBy = false
			}
		}

		newNode, sameNode, err := transform.OneNodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			evalSymbols := symbols
			uc, isCol := e.(*expression.UnresolvedColumn)
			if in, ok := n.(*plan.InsertInto); ok && len(in.OnDupExprs) > 0 && isCol && onDupUpdateLeftExprs[uc] {
				evalSymbols = onDupUpdateSymbols
			}
			return qualifyExpression(e, n, evalSymbols)
		})
		if err != nil {
			return originalNode, transform.SameTree, err
		}

		if sameCheckConstraints && sameGroupBy && sameNode == transform.SameTree {
			return newNode, transform.SameTree, nil
		}
		return newNode, transform.NewTree, nil
	})
}

// identifyGroupingAliasReferences finds any aliases defined in the projected expressions of
// the specified GroupBy node, looks for references to those aliases in the grouping expressions
// of the same GroupBy node, and transforms them to an AliasReference expression. This is
// necessary because when qualifying columns, we can't currently distinguish between projection
// expressions and grouping expressions because GroupBy combines both in its Expresions() func,
// so we special case GroupBy here to identify aliases used in the grouping expressions.
func identifyGroupingAliasReferences(groupBy *plan.GroupBy) (*plan.GroupBy, transform.TreeIdentity, error) {
	projectedAliases := aliasesDefinedInNode(groupBy)

	// Temporarily remove projection expressions so we can transform only the grouping expressions
	groupByWithOnlyGroupingExprs := plan.NewGroupBy(nil, groupBy.GroupByExprs, groupBy.Child)
	newNode, identity, err := transform.OneNodeExpressions(groupByWithOnlyGroupingExprs, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		uc, ok := e.(*expression.UnresolvedColumn)
		if !ok || uc.Table() != "" {
			return e, transform.SameTree, nil
		}

		if stringContains(projectedAliases, strings.ToLower(uc.Name())) {
			return expression.NewAliasReference(uc.Name()), transform.NewTree, nil
		}

		return e, transform.SameTree, nil
	})
	if identity == transform.NewTree && err == nil {
		groupBy = plan.NewGroupBy(groupBy.SelectedExprs, newNode.(*plan.GroupBy).GroupByExprs, groupBy.Child)
	}
	return groupBy, identity, err
}

// qualifyCheckConstraints returns a new set of CheckConstraints created by taking the specified Update node's checks
// and qualifying them to the table involved in the update, including honoring any table aliases specified.
func qualifyCheckConstraints(update *plan.Update) (sql.CheckConstraints, error) {
	checks := update.Checks
	table, alias := getResolvedTableAndAlias(update.Child)

	newExprs := make([]sql.Expression, len(checks))
	for i, checkExpr := range checks.ToExpressions() {
		newExpr, _, err := transform.Expr(checkExpr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.UnresolvedColumn:
				if e.Table() == "" {
					tableName := table.Name()
					if alias != "" {
						tableName = alias
					}
					return expression.NewUnresolvedQualifiedColumn(tableName, e.Name()), transform.NewTree, nil
				}
			default:
				// nothing else needed for other types
			}
			return e, transform.SameTree, nil
		})
		if err != nil {
			return nil, err
		}
		newExprs[i] = newExpr
	}

	return checks.FromExpressions(newExprs)
}

// getAvailableNamesByScope searches the node |n|, the current query being analyzed, as well as any nodes from outer
// scope levels contained in |scope| in order to calculate the available columns, tables, and aliases available to
// the current scope.
func getAvailableNamesByScope(n sql.Node, scope *plan.Scope) availableNames {
	symbols := make(availableNames)

	scopeNodes := make([]sql.Node, 0, 1+len(scope.InnerToOuter()))
	scopeNodes = append(scopeNodes, n)
	scopeNodes = append(scopeNodes, scope.InnerToOuter()...)
	currentScopeLevel := len(scopeNodes)

	// Examine all columns, from the innermost scope (this node) outward.
	children := n.Children()

	// find all ResolvedTables in InsertInto.Source visible when resolving columns iff there are on duplicate key updates
	if in, ok := n.(*plan.InsertInto); ok && in.Source != nil && len(in.OnDupExprs) > 0 {
		aliasedTables := make(map[sql.Node]bool)
		transform.Inspect(in.Source, func(n sql.Node) bool {
			if tblAlias, ok := n.(*plan.TableAlias); ok && tblAlias.Resolved() {
				children = append(children, tblAlias)
				aliasedTables[tblAlias.Child] = true
			}
			return true
		})
		transform.Inspect(in.Source, func(n sql.Node) bool {
			if resTbl, ok := n.(*plan.ResolvedTable); ok && !aliasedTables[resTbl] {
				children = append(children, resTbl)
			}
			return true
		})
		transform.Inspect(in.Source, func(n sql.Node) bool {
			if subAlias, ok := n.(*plan.SubqueryAlias); ok && !aliasedTables[subAlias] {
				children = append(children, subAlias)
			}
			return true
		})
	}

	getColumnsInNodes(children, symbols, currentScopeLevel-1)
	for _, currentScopeNode := range scopeNodes {
		getColumnsInNodes([]sql.Node{currentScopeNode}, symbols, currentScopeLevel-1)
		currentScopeLevel--
	}

	// Get table names in all outer scopes and nodes. Inner scoped names will overwrite those from the outer scope.
	// note: we terminate the symbols for this level after finding the first column source
	for scopeLevel, n := range scopeNodes {
		transform.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.SubqueryAlias, *plan.ResolvedTable, *plan.ValueDerivedTable, *plan.RecursiveTable, *plan.RecursiveCte, *plan.IndexedTableAccess, *plan.JSONTable:
				name := strings.ToLower(n.(sql.Nameable).Name())
				symbols.indexTable(name, name, scopeLevel)
				return false
			case *plan.TableAlias:
				switch t := n.Child.(type) {
				case *plan.ResolvedTable, *plan.UnresolvedTable, *plan.SubqueryAlias,
					*plan.RecursiveTable, *plan.IndexedTableAccess:
					name := strings.ToLower(t.(sql.Nameable).Name())
					alias := strings.ToLower(n.Name())
					symbols.indexTable(alias, name, scopeLevel)
				}
				return false
			case sql.Projector:
				// projected aliases overwrite lower namespaces, but importantly,
				// we do not terminate symbol generation.
				for _, e := range n.ProjectedExprs() {
					if a, ok := e.(*expression.Alias); ok {
						symbols.indexAlias(a, scopeLevel)
					}
				}
			}
			return true
		})
	}

	return symbols
}

// qualifyExpression examines the specified expression |e|, coming from the specified node |node|, and uses the |availableNames|
// symbol map to identify the table or expression alias an unqualified column reference should map to. The updated,
// qualified expression is returned along with the transform identity, or any error encountered.
func qualifyExpression(e sql.Expression, node sql.Node, symbols availableNames) (sql.Expression, transform.TreeIdentity, error) {
	switch col := e.(type) {
	case column:
		if col.Resolved() {
			return col, transform.SameTree, nil
		}

		// Skip qualification if an expression has already been identified as an alias reference
		if _, ok := col.(*expression.AliasReference); ok {
			return col, transform.SameTree, nil
		}

		// Skip this step for variables
		if strings.HasPrefix(col.Name(), "@") || strings.HasPrefix(col.Table(), "@") {
			return col, transform.SameTree, nil
		}

		// if there are no tables or columns anywhere in the query, just give up and let another part of the analyzer throw
		// an analysis error. (for some queries, like SHOW statements, this is expected and not an error)
		if len(symbols) == 0 {
			return col, transform.SameTree, nil
		}

		canAccessAliasAtCurrentScope := true
		if _, ok := node.(*plan.Filter); ok {
			// Expression aliases from the same scope are NOT allowed in where/filter clauses
			canAccessAliasAtCurrentScope = false
		}

		// If this column is already qualified, make sure the table name is known
		if col.Table() != "" {
			if validateQualifiedColumn(col, symbols) {
				return col, transform.SameTree, nil
			} else {
				similar := similartext.Find(symbols.allTables(), col.Table())
				return nil, transform.SameTree, sql.ErrTableNotFound.New(col.Table() + similar)
			}
		}

		// Look in all the scopes (from inner to outer), to identify the column. Stop as soon as we have a scope with
		// exactly 1 match for the column name. If there is ambiguity in available column names, that's an error.
		name := strings.ToLower(col.Name())
		for _, scopeLevel := range symbols.levels() {
			aliasesFound, tablesFound := symbols.aliasesAndTablesForColumnAtLevel(name, scopeLevel)
			switch len(aliasesFound) + len(tablesFound) {
			case 0:
				// This column could be in an outer scope, keep going
				continue
			case 1:
				if len(aliasesFound) > 0 {
					// This indicates we found a match with an alias definition
					if canAccessAliasAtCurrentScope || scopeLevel != symbols.levels()[0] {
						return expression.NewAliasReference(col.Name()), transform.NewTree, nil
					}
				} else {
					return expression.NewUnresolvedQualifiedColumn(
						tablesFound[0],
						col.Name(),
					), transform.NewTree, nil
				}
			default:
				switch node.(type) {
				case *plan.Sort, *plan.Having:
					// For order by and having clauses... prefer an alias over a column when there is ambiguity
					if len(aliasesFound) == 0 {
						if len(tablesFound) == 1 {
							return expression.NewUnresolvedQualifiedColumn(tablesFound[0], col.Name()), transform.NewTree, nil
						} else if len(tablesFound) > 1 {
							return col, transform.SameTree, sql.ErrAmbiguousColumnOrAliasName.New(col.Name())
						}
					} else if len(aliasesFound) == 1 {
						return expression.NewAliasReference(col.Name()), transform.NewTree, nil
					} else if len(aliasesFound) > 1 {
						return col, transform.SameTree, sql.ErrAmbiguousColumnOrAliasName.New(col.Name())
					}
				default:
					// otherwise, prefer the table column...
					if len(tablesFound) == 1 {
						return expression.NewUnresolvedQualifiedColumn(tablesFound[0], col.Name()), transform.NewTree, nil
					} else if len(aliasesFound) > 0 {
						// MySQL allows ambiguity with multiple alias names in some situations, so identify this as an
						// alias reference and resolve the exact alias definition later
						return expression.NewAliasReference(col.Name()), transform.NewTree, nil
					}
				}

				return nil, transform.SameTree, sql.ErrAmbiguousColumnName.New(col.Name(), strings.Join(tablesFound, ", "))
			}
		}

		if !canAccessAliasAtCurrentScope {
			// return a deferredColumn if we still can't find a column and know this couldn't be an alias reference
			return &deferredColumn{expression.NewUnresolvedQualifiedColumn(col.Table(), col.Name())}, transform.NewTree, nil
		}

		// If there are no tables that have any column with the column name let's just return it as it is. This may be an
		// alias, so we'll wait for the reorder of the projection to resolve it.
		return col, transform.SameTree, nil
	case *expression.Star:
		// Make sure that any qualified stars reference known tables
		if col.Table != "" {
			tableFound := false
			for level := range symbols {
				tables := symbols.tablesAtLevel(level)
				if _, ok := tables[strings.ToLower(col.Table)]; ok {
					tableFound = true
					break
				}
			}
			if !tableFound {
				return nil, transform.SameTree, sql.ErrTableNotFound.New(col.Table)
			}
		}
		return col, transform.SameTree, nil
	default:
		// If any other kind of expression has a star, just replace it
		// with an unqualified star because it cannot be expanded.
		return transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if _, ok := e.(*expression.Star); ok {
				return expression.NewStar(), transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		})
	}
}

// validateQualifiedColumn returns true if the table name of the specified column is a valid table name symbol, meaning
// it is available in some scope of the current statement. If a valid table name symbol can't be found, false is returned.
func validateQualifiedColumn(col column, symbols availableNames) bool {
	for scopeLevel := range symbols {
		tables := symbols.tablesAtLevel(scopeLevel)
		if _, ok := tables[strings.ToLower(col.Table())]; ok {
			return true
		}
	}

	if symbols.hasTableCol(tableCol{table: strings.ToLower(col.Table()), col: strings.ToLower(col.Name())}) {
		return true
	}

	return false
}

func getColumnsInNodes(nodes []sql.Node, names availableNames, scopeLevel int) {
	indexExpressions := func(exprs []sql.Expression) {
		for _, e := range exprs {
			switch e := e.(type) {
			case *expression.Alias:
				// Mark this as an available column; we'll record the Alias information later
				names.indexColumn("", e.Name(), scopeLevel)
			case *expression.GetField:
				names.indexColumn(e.Table(), e.Name(), scopeLevel)
			}
		}
	}

	for _, node := range nodes {
		switch n := node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.RecursiveTable, *plan.JSONTable:
			for _, col := range n.Schema() {
				names.indexColumn(col.Source, col.Name, scopeLevel)
			}
		case sql.Projector:
			indexExpressions(n.ProjectedExprs())
			getColumnsInNodes(node.Children(), names, scopeLevel)
		default:
			getColumnsInNodes(n.Children(), names, scopeLevel)
		}
	}
}

var errGlobalVariablesNotSupported = errors.NewKind("can't resolve global variable, %s was requested")

// resolveJSONTables is a helper function that resolves JSONTables in join as they have special visibility into the left side of the join
// This function should return a *plan.JSONTable when there's no error
func resolveJSONTables(ctx *sql.Context, a *Analyzer, scope *plan.Scope, left sql.Node, jt *plan.JSONTable) (sql.Node, transform.TreeIdentity, error) {
	if jt.Resolved() {
		return jt, transform.SameTree, nil
	}

	// wrap left in project node to get scope.Schema to work correctly
	proj := plan.NewProject([]sql.Expression{}, left)
	rightScope := scope.NewScope(proj)
	// json table has visibility into columns on left of joins
	columns, err := indexColumns(ctx, a, jt, rightScope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	newJt, same, err := transform.OneNodeExprsWithNode(jt, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		uc, ok := e.(column)
		if !ok || e.Resolved() {
			return e, transform.SameTree, nil
		}
		return resolveColumnExpression(a, n, uc, columns)
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	if same {
		return jt, transform.SameTree, nil
	}
	return newJt, transform.NewTree, nil
}

func resolveJSONTablesInJoin(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		var jtNew sql.Node
		var jtSame transform.TreeIdentity
		var jtErr error
		switch j := n.(type) {
		case *plan.JoinNode:
			switch {
			case j.Op.IsNatural() || j.Op.IsInner():
				if jt, ok := j.Right().(*plan.JSONTable); ok {
					jtNew, jtSame, jtErr = resolveJSONTables(ctx, a, scope, j.Left(), jt)
				}
			default:
				return n, transform.SameTree, nil
			}
		default:
			return n, transform.SameTree, nil
		}

		if jtErr != nil {
			return nil, transform.SameTree, jtErr
		}

		if jtNew == nil || jtSame {
			return n, transform.SameTree, nil
		}

		newN, err := n.WithChildren(n.Children()[0], jtNew)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if _, ok := newN.(sql.Expressioner); !ok {
			return newN, transform.NewTree, nil
		}

		columns, err := indexColumns(ctx, a, newN, scope)
		if err != nil {
			return nil, transform.SameTree, err
		}

		ret, _, err := transform.OneNodeExprsWithNode(newN, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, transform.SameTree, nil
			}

			return resolveColumnExpression(a, newN, uc, columns)
		})
		if err != nil {
			return nil, transform.SameTree, nil
		}
		return ret, transform.NewTree, nil
	})
}

// resolveColumns replaces UnresolvedColumn expressions with GetField expressions for the appropriate numbered field in
// the expression's child node.
func resolveColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_columns")
	defer span.End()

	n, same1, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		if _, ok := n.(sql.Expressioner); !ok {
			return n, transform.SameTree, nil
		}

		// We need to use the schema, so all children must be resolved.
		// TODO: also enforce the equivalent constraint for outer scopes. More complicated, because the outer scope can't
		//  be Resolved() owing to a child expression (the one being evaluated) not being resolved yet.
		for _, c := range n.Children() {
			if !c.Resolved() {
				return n, transform.SameTree, nil
			}
		}

		columns, err := indexColumns(ctx, a, n, scope)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return transform.OneNodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, transform.SameTree, nil
			}

			return resolveColumnExpression(a, n, uc, columns)
		})
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	n, same2, err := resolveJSONTablesInJoin(ctx, a, n, scope, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, same1 && same2, nil
}

// indexColumns returns a map of column identifiers to their index in the node's schema. Columns from outer scopes are
// included as well, with lower indexes (prepended to node schema) but lower precedence (overwritten by inner nodes in
// map)
func indexColumns(_ *sql.Context, _ *Analyzer, n sql.Node, scope *plan.Scope) (map[tableCol]indexedCol, error) {
	var columns = make(map[tableCol]indexedCol)
	var idx int

	indexColumn := func(col *sql.Column) {
		columns[tableCol{
			table: strings.ToLower(col.Source),
			col:   strings.ToLower(col.Name),
		}] = indexedCol{col, idx}
		idx++
	}

	indexSchema := func(n sql.Schema) {
		for _, col := range n {
			indexColumn(col)
		}
	}

	var indexColumnExpr func(e sql.Expression)
	indexColumnExpr = func(e sql.Expression) {
		switch e := e.(type) {
		case *expression.Alias:
			// Aliases get indexed twice with the same index number: once with the aliased name and once with the
			// underlying name
			indexColumn(transform.ExpressionToColumn(e))
			idx--
			indexColumnExpr(e.Child)
		default:
			indexColumn(transform.ExpressionToColumn(e))
		}
	}

	indexChildNode := func(n sql.Node) {
		switch n := n.(type) {
		case sql.Projector:
			for _, e := range n.ProjectedExprs() {
				indexColumnExpr(e)
			}
		case *plan.Values:
			// values nodes don't have a schema to index like other nodes that provide columns
		default:
			indexSchema(n.Schema())
		}
	}

	if scope.OuterRelUnresolved() {
		// the columns in this relation will be mis-indexed, skip
		// until outer rel is resolved
		return nil, nil
	}

	// Index the columns in the outer scope, outer to inner. This means inner scope columns will overwrite the outer
	// ones of the same name. This matches the MySQL scope precedence rules.
	indexSchema(scope.Schema())

	// For the innermost scope (the node being evaluated), look at the schemas of the children instead of this node
	// itself. Skip this for DDL nodes that handle indexing separately.
	shouldIndexChildNode := true
	switch n.(type) {
	case *plan.AddColumn, *plan.ModifyColumn:
		shouldIndexChildNode = false
	case *plan.RecursiveCte, *plan.Union:
		shouldIndexChildNode = false
	}

	if shouldIndexChildNode {
		for _, child := range n.Children() {
			indexChildNode(child)
		}
	}

	// For certain DDL nodes, we have to do more work
	indexSchemaForDefaults := func(column *sql.Column, order *sql.ColumnOrder, sch sql.Schema) {
		tblSch := make(sql.Schema, len(sch))
		copy(tblSch, sch)
		if order == nil {
			tblSch = append(tblSch, column)
		} else if order.First {
			tblSch = append(sql.Schema{column}, tblSch...)
		} else { // must be After
			index := 1
			afterColumn := strings.ToLower(order.AfterColumn)
			for _, col := range tblSch {
				if strings.ToLower(col.Name) == afterColumn {
					break
				}
				index++
			}
			if index <= len(tblSch) {
				tblSch = append(tblSch, nil)
				copy(tblSch[index+1:], tblSch[index:])
				tblSch[index] = column
			}
		}
		for _, col := range tblSch {
			columns[tableCol{
				table: "",
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			columns[tableCol{
				table: strings.ToLower(col.Source),
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			idx++
		}
	}

	switch node := n.(type) {
	case *plan.CreateTable: // For this node in particular, the columns will only come into existence after the analyzer step, so we forge them here.
		for _, col := range node.CreateSchema.Schema {
			columns[tableCol{
				table: "",
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			columns[tableCol{
				table: strings.ToLower(col.Source),
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			idx++
		}
	case *plan.AddColumn: // Add/Modify need to have the full column set in order to resolve a default expression.
		tbl := node.Table
		indexSchemaForDefaults(node.Column(), node.Order(), tbl.Schema())
	case *plan.ModifyColumn:
		tbl := node.Table
		indexSchemaForDefaults(node.NewColumn(), node.Order(), tbl.Schema())
	case *plan.RecursiveCte, *plan.Union:
		// opaque nodes have derived schemas
		// TODO also subquery aliases?
		indexChildNode(node.(sql.BinaryNode).Left())
	case *plan.InsertInto:
		// should index columns in InsertInto.Source
		aliasedTables := make(map[sql.Node]bool)
		transform.Inspect(node.Source, func(n sql.Node) bool {
			// need to reset idx for each table found, as this function assumes only 1 table
			if tblAlias, ok := n.(*plan.TableAlias); ok && tblAlias.Resolved() {
				idx = 0
				indexSchema(tblAlias.Schema())
				aliasedTables[tblAlias.Child] = true
			}
			return true
		})
		transform.Inspect(node.Source, func(n sql.Node) bool {
			if resTbl, ok := n.(*plan.ResolvedTable); ok && !aliasedTables[resTbl] {
				idx = 0
				indexSchema(resTbl.Schema())
			}
			return true
		})
		transform.Inspect(node.Source, func(n sql.Node) bool {
			if resTbl, ok := n.(*plan.SubqueryAlias); ok && resTbl.Resolved() && !aliasedTables[resTbl] {
				idx = 0
				indexSchema(resTbl.Schema())
			}
			return true
		})
	}

	return columns, nil
}

func resolveColumnExpression(a *Analyzer, n sql.Node, e column, columns map[tableCol]indexedCol) (sql.Expression, transform.TreeIdentity, error) {
	name := strings.ToLower(e.Name())
	table := strings.ToLower(e.Table())
	col, ok := columns[tableCol{table, name}]
	if !ok {
		switch uc := e.(type) {
		case *expression.UnresolvedColumn:
			// Defer the resolution of the column to give the analyzer more
			// time to resolve other parts so this can be resolved.
			a.Log("deferring resolution of column %s", e)
			return &deferredColumn{uc}, transform.NewTree, nil
		default:
			if table != "" {
				return nil, transform.SameTree, sql.ErrTableColumnNotFound.New(e.Table(), e.Name())
			}

			// This means the expression is either a non-existent column or an alias defined in the same projection.
			// Check for the latter first.
			// NOTE: For GroupBy nodes, the projected expressions and grouping expressions are both returned from
			//       Expressions(), so at this point in the code, we can't tell if we are looking at a projected
			//       expression or a grouping expression here, and the alias resolution rules are different for each.
			//       We handle this with special casing for GroupBy that transforms identified aliases in grouping
			//       expressions into AliasReferences, so here we assume that this is a projection expression.
			//       Being able to differentiate between grouping and projection expressions here could help
			//       clean up this logic.
			aliasesInNode := aliasesDefinedInNode(n)
			if stringContains(aliasesInNode, name) {
				return nil, transform.SameTree, sql.ErrMisusedAlias.New(name)
			}

			return nil, transform.SameTree, sql.ErrColumnNotFound.New(e.Name())
		}
	}

	a.Log("column %s resolved to GetFieldWithTable: idx %d, typ %s, table %s, name %s, nullable %t",
		e, col.index, col.Type, col.Source, col.Name, col.Nullable)
	return expression.NewGetFieldWithTable(
		col.index,
		col.Type,
		col.Source,
		col.Name,
		col.Nullable,
	), transform.NewTree, nil
}

// pushdownGroupByAliases reorders the aggregation in a groupby so aliases defined in it can be resolved in the grouping
// of the groupby. To do so, all aliases are pushed down to a projection node under the group by.
func pushdownGroupByAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if n.Resolved() {
		return n, transform.SameTree, nil
	}

	// replacedAliases is a map of original expression string to alias that has been pushed down below the GroupBy in
	// the new projection node.
	replacedAliases := make(map[string]string)
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// For any Expressioner node above the GroupBy, we need to apply the same alias replacement as we did in the
		// GroupBy itself.
		ex, ok := n.(sql.Expressioner)
		if ok && len(replacedAliases) > 0 {
			newExprs, same := replaceExpressionsWithAliases(ex.Expressions(), replacedAliases)
			if !same {
				n, err := ex.WithExpressions(newExprs...)
				return n, transform.NewTree, err
			}
		}

		g, ok := n.(*plan.GroupBy)
		if n.Resolved() || !ok || len(g.GroupByExprs) == 0 {
			return n, transform.SameTree, nil
		}

		// The reason we have two sets of columns, one for grouping and
		// one for aggregate is because an alias can redefine a column name
		// of the child schema. In the grouping, if that column is referenced
		// it refers to the alias, and not the one in the child. However,
		// in the aggregate, aliases in that same aggregate cannot be used,
		// so it refers to the column in the child node.
		var groupingColumns = make(map[string]column)
		for _, g := range g.GroupByExprs {
			for _, n := range findAllColumns(g) {
				groupingColumns[strings.ToLower(n.Name())] = n
			}
		}

		var selectedColumns = make(map[string]column)
		for _, agg := range g.SelectedExprs {
			// This alias is going to be pushed down, so don't bother gathering
			// its requirements.
			if alias, ok := agg.(*expression.Alias); ok {
				if _, ok := groupingColumns[strings.ToLower(alias.Name())]; ok {
					continue
				}
			}

			for _, n := range findAllColumns(agg) {
				selectedColumns[strings.ToLower(n.Name())] = n
			}
		}

		var newSelectedExprs []sql.Expression
		var projection []sql.Expression
		// Aliases will keep the aliases that have been pushed down and their
		// index in the new aggregate.
		var aliases = make(map[string]int)

		var needsReorder bool
		for _, expr := range g.SelectedExprs {
			alias, ok := expr.(*expression.Alias)
			// Note that aliases of aggregations cannot be used in the grouping
			// because the grouping is needed before computing the aggregation.
			if !ok || containsAggregation(alias) {
				newSelectedExprs = append(newSelectedExprs, expr)
				continue
			}

			name := strings.ToLower(alias.Name())
			// Only if the alias is required in the grouping set needsReorder
			// to true. If it's not required, there's no need for a reorder if
			// no other alias is required.
			_, ok = groupingColumns[name]
			if ok && groupingColumns[name].Table() == "" {
				aliases[name] = len(newSelectedExprs)
				needsReorder = true
				delete(groupingColumns, name)

				projection = append(projection, expr)
				replacedAliases[alias.Child.String()] = alias.Name()
				newSelectedExprs = append(newSelectedExprs, expression.NewAliasReference(alias.Name()))
			} else {
				newSelectedExprs = append(newSelectedExprs, expr)
			}
		}

		if !needsReorder {
			return n, transform.SameTree, nil
		}

		// Any replacements of aliases in the select expression must be mirrored in the group by, replacing any aliased
		// expressions with a reference to that alias. This is so that we can directly compare the group by an select
		// expressions for validation, which requires us to know that (table.column as col) and (table.column) are the
		// same expressions. So if we replace one, replace both.
		// TODO: this is pretty fragile and relies on string matching, need a better solution
		newGroupBys, _ := replaceExpressionsWithAliases(g.GroupByExprs, replacedAliases)

		// Instead of iterating columns directly, we want them sorted so the
		// executions of the rule are consistent.
		var missingCols = make([]column, 0, len(selectedColumns)+len(groupingColumns))
		for _, col := range selectedColumns {
			missingCols = append(missingCols, col)
		}
		for _, col := range groupingColumns {
			missingCols = append(missingCols, col)
		}

		sort.SliceStable(missingCols, func(i, j int) bool {
			return missingCols[i].Name() < missingCols[j].Name()
		})

		var renames = make(map[string]string)
		// All columns required by expressions in both grouping and aggregation
		// must also be projected in the new projection node or they will not
		// be able to resolve.
		for _, col := range missingCols {
			name := col.Name()
			// If an alias has been pushed down with the same name as a missing
			// column, there will be a conflict of names. We must find an unique name
			// for the missing column.
			if _, ok := aliases[name]; ok {
				for i := 1; ; i++ {
					name = fmt.Sprintf("%s_%02d", col.Name(), i)
					if _, ok := selectedColumns[name]; !ok {
						break
					} else if _, ok := groupingColumns[name]; !ok {
						break
					}
				}
			}

			if name == col.Name() {
				projection = append(projection, col)
			} else {
				renames[col.Name()] = name
				projection = append(projection, expression.NewAlias(name, col))
			}
		}

		// If there is any name conflict between columns we need to rename every
		// usage inside the aggregate.
		if len(renames) > 0 {
			for i, expr := range newSelectedExprs {
				var err error
				newSelectedExprs[i], _, err = transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					col, ok := e.(*expression.UnresolvedColumn)
					if ok {
						// We need to make sure we don't rename the reference to the
						// pushed down alias.
						if to, ok := renames[col.Name()]; ok && aliases[col.Name()] != i {
							return expression.NewUnresolvedColumn(to), transform.NewTree, nil
						}
					}

					return e, transform.SameTree, nil
				})
				if err != nil {
					return nil, transform.SameTree, err
				}
			}
		}

		return plan.NewGroupBy(
			newSelectedExprs, newGroupBys,
			plan.NewProject(projection, g.Child),
		), transform.NewTree, nil
	})
}

// replaceExpressionsWithAliases replaces any expressions in the slice given that match the map of aliases given with
// their alias expression or alias name. This is necessary when pushing aliases down the tree, since we introduce a
// projection node that effectively erases the original columns of a table.
func replaceExpressionsWithAliases(exprs []sql.Expression, replacedAliasesByExpression map[string]string) ([]sql.Expression, transform.TreeIdentity) {
	replacedAliasesByName := make(map[string]struct{})
	for _, aliasName := range replacedAliasesByExpression {
		replacedAliasesByName[aliasName] = struct{}{}
	}

	var newExprs []sql.Expression
	var expr sql.Expression
	for i := range exprs {
		expr = exprs[i]
		if alias, ok := replacedAliasesByExpression[expr.String()]; ok {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = expression.NewAliasReference(alias)
		} else if uc, ok := expr.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
			if _, ok := replacedAliasesByName[uc.Name()]; ok {
				if newExprs == nil {
					newExprs = make([]sql.Expression, len(exprs))
					copy(newExprs, exprs)
				}
				newExprs[i] = expression.NewAliasReference(uc.Name())
			}
		}
	}
	if len(newExprs) > 0 {
		return newExprs, transform.NewTree
	}
	return exprs, transform.SameTree
}

func findAllColumns(e sql.Expression) []column {
	var cols []column
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e.(type) {
		case *expression.UnresolvedColumn, *expression.AliasReference:
			cols = append(cols, e.(column))
		default:
		}
		return true
	})
	return cols
}
