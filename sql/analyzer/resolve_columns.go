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

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func validateUniqueTableNames(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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

type indexedCol struct {
	*sql.Column
	index int
}

// column is the common interface that groups UnresolvedColumn and deferredColumn and AliasReference
type column interface {
	sql.Nameable
	sql.Tableable
	sql.Expression
}

// nestingLevelSymbols tracks available table and column name symbols at a nesting level for a query. Each nested
// subquery represents an additional nesting level.
type nestingLevelSymbols struct {
	availableColumns   map[string][]string
	availableAliases   map[string]*expression.Alias
	availableTables    map[string]string
	availableTableCols map[tableCol]struct{}
	lastRel            string
}

func newNestingLevelSymbols() *nestingLevelSymbols {
	return &nestingLevelSymbols{
		availableColumns:   make(map[string][]string),
		availableAliases:   make(map[string]*expression.Alias),
		availableTables:    make(map[string]string),
		availableTableCols: make(map[tableCol]struct{}),
	}
}

// availableNames tracks available table and column name symbols at each nesting level for a query, where level 0
// is the node being analyzed, and each additional level is one layer of query scope outward.
type availableNames map[int]*nestingLevelSymbols

// debugString returns a string representation of this availableNames instance.
func (a availableNames) debugString() string {
	if a == nil {
		return ""
	}

	highestLevel := -1
	lowestLevel := -1
	for level := range a {
		if level > highestLevel || highestLevel == -1 {
			highestLevel = level
		}
		if level < lowestLevel || lowestLevel == -1 {
			lowestLevel = level
		}
	}

	perLevelResult := make([]string, highestLevel-lowestLevel+1)
	for level, symbols := range a {
		perLevelResult[level-lowestLevel] =
			fmt.Sprintf("  Aliases: (%s) \n", strings.Join(keys(symbols.availableAliases), ", ")) +
				fmt.Sprintf("  Tables: (%s)\n", strings.Join(keys(symbols.availableTables), ", ")) +
				fmt.Sprintf("  Columns: (%s)\n", strings.Join(keys(symbols.availableColumns), ", "))
	}

	result := ""

	if lowestLevel > -1 && highestLevel > -1 {
		for i := lowestLevel; i <= highestLevel; i++ {
			result = fmt.Sprintf("%s\nLevel %d\n%s", result, i, perLevelResult[i-lowestLevel])
		}
	}

	return result
}

// keys returns a slice containing the keys in the given map.
func keys[K comparable, V any](m map[K]V) []K {
	if m == nil {
		return []K{}
	}
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// indexColumn adds a column with the given table and column name at the given nesting level
func (a availableNames) indexColumn(table, col string, nestingLevel int) {
	col = strings.ToLower(col)
	_, ok := a[nestingLevel]
	if !ok {
		a[nestingLevel] = newNestingLevelSymbols()
	}
	tableLower := strings.ToLower(table)
	if !stringContains(a[nestingLevel].availableColumns[col], tableLower) {
		a[nestingLevel].availableColumns[col] = append(a[nestingLevel].availableColumns[col], tableLower)
		a[nestingLevel].availableTableCols[tableCol{table: tableLower, col: col}] = struct{}{}
	}
}

// levels returns a sorted list of nesting scopes
func (a availableNames) levels() []int {
	levels := make([]int, len(a))
	i := 0
	for l := range a {
		levels[i] = l
		i++
	}
	sort.Ints(levels)
	return levels
}

// indexAlias adds an alias name to the nesting level
func (a availableNames) indexAlias(e *expression.Alias, nestingLevel int) {
	name := strings.ToLower(e.Name())
	_, ok := a[nestingLevel]
	if !ok {
		a[nestingLevel] = newNestingLevelSymbols()
	}
	_, ok = a[nestingLevel].availableAliases[name]
	if !ok {
		a[nestingLevel].availableAliases[name] = e
	}
}

// conflictingAlias returns true if there is an alias in a lower buildScope with
// the same name. Columns with the same name as an alias in a higher buildScope
// must be qualified.
func (a availableNames) conflictingAlias(name string, nestingLevel int) bool {
	for i := 0; i < nestingLevel-1; i++ {
		if _, ok := a[i].availableAliases[name]; ok {
			return true
		}
	}
	return false
}

// indexTable adds a table with the given name at the given nesting level
func (a availableNames) indexTable(alias, name string, nestingLevel int) {
	alias = strings.ToLower(alias)
	_, ok := a[nestingLevel]
	if !ok {
		a[nestingLevel] = newNestingLevelSymbols()
	}
	a[nestingLevel].availableTables[alias] = strings.ToLower(name)
}

// nesting levels returns all levels present, from inner to outer
func (a availableNames) nestingLevels() []int {
	levels := make([]int, len(a))
	for level := range a {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	return levels
}

func (a availableNames) tablesAtLevel(level int) map[string]string {
	return a[level].availableTables
}

func (a availableNames) allTables() []string {
	var allTables []string
	for _, level := range a {
		for name, table := range level.availableTables {
			allTables = append(allTables, name, table)
		}
	}
	return dedupStrings(allTables)
}

func (a availableNames) tablesForColumnAtLevel(column string, level int) []string {
	return a[level].availableColumns[column]
}

func (a availableNames) hasTableCol(tc tableCol) bool {
	for i := range a {
		_, ok := a[i].availableTableCols[tc]
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

// qualifyColumns assigns a table to any column expressions that don't have one already
func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var nestingLevel int
	symbols := make(availableNames)
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if _, ok := n.(sql.Expressioner); !ok || n.Resolved() {
			return n, transform.SameTree, nil
		}
		if _, ok := n.(*plan.RecursiveCte); ok {
			return n, transform.SameTree, nil
		}

		symbols = getNodeAvailableNames(n, scope, symbols, nestingLevel)
		nestingLevel++

		return transform.OneNodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			return qualifyExpression(e, symbols)
		})
	})
}

// getNodeAvailableSymbols returns the set of table and column names accessible to the node given and using the buildScope
// given. Table aliases overwrite table names: the original name is not considered accessible once aliased.
// The value of the map is the same as the key, just used for existence checks.
func getNodeAvailableNames(n sql.Node, scope *Scope, names availableNames, nestingLevel int) availableNames {
	// Examine all columns, from the innermost scope (this one) outward.
	getColumnsInNodes(n.Children(), names, nestingLevel)
	for i, n := range scope.InnerToOuter() {
		// For the inner scope, we want all available columns in child nodes. For the outer scope, we are interested in
		// available columns in the sibling node
		getColumnsInNodes(n.Children(), names, i+1)
	}

	// Get table names in all outer scopes and nodes. Inner scoped names will overwrite those from the outer scope.
	// note: we terminate the symbols for this level after finding the first column source
	for i, n := range append(append(([]sql.Node)(nil), n), scope.InnerToOuter()...) {
		transform.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.SubqueryAlias, *plan.ResolvedTable, *plan.ValueDerivedTable, *plan.RecursiveCte, *information_schema.ColumnsTable, *plan.IndexedTableAccess:
				name := strings.ToLower(n.(sql.Nameable).Name())
				names.indexTable(name, name, i)
				return false
			case *plan.TableAlias:
				switch t := n.Child.(type) {
				case *plan.ResolvedTable, *plan.UnresolvedTable, *plan.SubqueryAlias,
					*plan.RecursiveTable, *information_schema.ColumnsTable, *plan.IndexedTableAccess:
					name := strings.ToLower(t.(sql.Nameable).Name())
					alias := strings.ToLower(n.Name())
					names.indexTable(alias, name, i)
				}
				return false
			case *plan.GroupBy:
				// groupby aliases can overwrite lower namespaces, but importantly,
				// we do not terminate symbol generation.
				for _, e := range n.SelectedExprs {
					if a, ok := e.(*expression.Alias); ok {
						names.indexAlias(a, nestingLevel)
					}
				}
			case *plan.Project:
				// project aliases can overwrite lower namespaces, but importantly,
				// we do not terminate symbol generation.
				for _, e := range n.Projections {
					if a, ok := e.(*expression.Alias); ok {
						names.indexAlias(a, nestingLevel)
					}
				}
			}
			return true
		})
	}

	return names
}

func qualifyExpression(e sql.Expression, symbols availableNames) (sql.Expression, transform.TreeIdentity, error) {
	switch col := e.(type) {
	case column:
		if col.Resolved() {
			return col, transform.SameTree, nil
		}

		// AliasReferences do not need to be qualified to a table; they are already fully qualified
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

		// TODO: more tests for error conditions

		// If this column is already qualified, make sure the table name is known
		if col.Table() != "" {
			// TODO: method for this
			tableFound := false
			for level := range symbols {
				tables := symbols.tablesAtLevel(level)
				if _, ok := tables[strings.ToLower(col.Table())]; ok {
					tableFound = true
					break
				}
			}

			if !tableFound {
				if symbols.hasTableCol(tableCol{table: strings.ToLower(col.Table()), col: strings.ToLower(col.Name())}) {
					return col, transform.SameTree, nil
				}
				similar := similartext.Find(symbols.allTables(), col.Table())
				return nil, transform.SameTree, sql.ErrTableNotFound.New(col.Table() + similar)
			}

			return col, transform.SameTree, nil
		}

		// Look in all the scope, inner to outer, to identify the column. Stop as soon as we have a scope with exactly 1
		// match for the column name. If any scope has ambiguity in available column names, that's an error.
		name := strings.ToLower(col.Name())
		levels := symbols.levels()
		if symbols.conflictingAlias(name, len(symbols)) {
			// A higher scope produces an alias with this name.
			// We override the outer scope and qualify this column
			// only if the current scope provides a definition.
			levels = levels[len(symbols)-1:]
		}
		for _, level := range levels {
			tablesForColumn := symbols.tablesForColumnAtLevel(name, level)

			// If the table exists but it's not available for this node it
			// means some work is still needed, so just return the column
			// and let it be resolved in the next pass.
			// TODO:
			// if !stringContains(tablesForColumn, table) {
			// 	return col, nil
			// }

			switch len(tablesForColumn) {
			case 0:
				// This column could be in an outer scope, keep going
				continue
			case 1:
				if tablesForColumn[0] == "" {
					return col, transform.SameTree, nil
				}
				return expression.NewUnresolvedQualifiedColumn(
					tablesForColumn[0],
					col.Name(),
				), transform.NewTree, nil
			default:
				if len(symbols[level].lastRel) > 0 {
					return expression.NewUnresolvedQualifiedColumn(
						symbols[level].lastRel,
						col.Name(),
					), transform.NewTree, nil
				}
				return nil, transform.SameTree, sql.ErrAmbiguousColumnName.New(col.Name(), strings.Join(tablesForColumn, ", "))
			}
		}

		// If there are no tables that have any column with the column name let's just return it as it is. This may be an
		// alias, so we'll wait for the reorder of the projection to resolve it.
		return col, transform.SameTree, nil
	case *expression.Star:
		// Make sure that any qualified stars reference known tables
		if col.Table != "" {
			//nestingLevels := symbols.nestingLevels()
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

func getColumnsInNodes(nodes []sql.Node, names availableNames, nestingLevel int) {
	indexExpressions := func(exprs []sql.Expression) {
		for _, e := range exprs {
			switch e := e.(type) {
			case *expression.Alias:
				names.indexColumn("", e.Name(), nestingLevel)
			case *expression.GetField:
				names.indexColumn(e.Table(), e.Name(), nestingLevel)
			case *expression.UnresolvedColumn:
				names.indexColumn(e.Table(), e.Name(), nestingLevel)
			}
		}
	}

	for _, node := range nodes {
		switch n := node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.RecursiveTable, *information_schema.ColumnsTable:
			for _, col := range n.Schema() {
				names.indexColumn(col.Source, col.Name, nestingLevel)
			}
		case *plan.Project:
			indexExpressions(n.Projections)
		case *plan.GroupBy:
			indexExpressions(n.SelectedExprs)
		case *plan.Window:
			indexExpressions(n.SelectExprs)
		default:
			getColumnsInNodes(n.Children(), names, nestingLevel)
		}
	}
}

var errGlobalVariablesNotSupported = errors.NewKind("can't resolve global variable, %s was requested")

const (
	sessionTable  = "@@" + sqlparser.SessionStr
	sessionPrefix = sqlparser.SessionStr + "."
	globalPrefix  = sqlparser.GlobalStr + "."
)

// resolveColumns replaces UnresolvedColumn expressions with GetField expressions for the appropriate numbered field in
// the expression's child node.
func resolveColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_columns")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
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
}

// indexColumns returns a map of column identifiers to their index in the node's schema. Columns from outer scopes are
// included as well, with lower indexes (prepended to node schema) but lower precedence (overwritten by inner nodes in
// map)
func indexColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (map[tableCol]indexedCol, error) {
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
		case *plan.Project:
			for _, e := range n.Projections {
				indexColumnExpr(e)
			}
		case *plan.GroupBy:
			for _, e := range n.SelectedExprs {
				indexColumnExpr(e)
			}
		case *plan.Window:
			for _, e := range n.SelectExprs {
				indexColumnExpr(e)
			}
		case *plan.Values:
			// values nodes don't have a schema to index like other nodes that provide columns
		default:
			indexSchema(n.Schema())
		}
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

func identifyGroupByAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if n.Resolved() {
		return n, transform.SameTree, nil
	}

	var nestingLevel int
	symbols := make(availableNames)

	// TODO: What is nesting level? Is that different than scope?
	getNodeAvailableNames(n, scope, symbols, 0)
	a.Log(fmt.Sprintf("Identified symbols (nesting level: %d): '%s'", nestingLevel, symbols.debugString()))

	// replacedAliases is a map of original expression string to alias that will need to be pushed down below the GroupBy in
	// the new projection node later in the analyzer.
	replacedAliases := make(map[string]string) // TODO: rename: identifiedAliases? // TODO: rewrite all this code?
	var err error
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if _, ok := n.(sql.Expressioner); !ok || n.Resolved() {
			return n, transform.SameTree, nil
		}
		if _, ok := n.(*plan.RecursiveCte); ok {
			return n, transform.SameTree, nil
		}

		symbols = getNodeAvailableNames(n, scope, symbols, nestingLevel)
		nestingLevel++

		// For any Expressioner node above the GroupBy, we need to apply the same alias replacement as we did in the
		// GroupBy itself.
		ex, ok := n.(sql.Expressioner)
		if ok && len(symbols) > 0 {
			newExprs, same := replaceExpressionsWithAliasReferences(ex.Expressions(), replacedAliases)
			//newExprs, same := replaceExpressionsWithAliasReferences2(ex.Expressions(), symbols)
			if !same {
				n, err = ex.WithExpressions(newExprs...)
				return n, transform.NewTree, err
			}
		}

		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		hav, ok := n.(*plan.Having)
		if ok {
			if hav.Cond != nil {
				transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					switch c := e.(type) {
					case *expression.UnresolvedColumn:
						for _, value := range replacedAliases {
							// TODO: This doesn't take into account aliases from outer scopes that could be visible...
							//       Feels like this needs to be part of identifying aliases and later
							//       resolving (or qualifying?) them.
							if c.Name() == value {
								return expression.NewAliasReference(value), transform.NewTree, nil
							}
						}
					}
					return e, transform.SameTree, nil
				})
			}
		}

		g, ok := n.(*plan.GroupBy)
		if !ok || len(g.GroupByExprs) == 0 {
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

		for _, expr := range g.SelectedExprs {
			alias, ok := expr.(*expression.Alias)
			// Note that aliases of aggregations cannot be used in the grouping
			// because the grouping is needed before computing the aggregation.
			if !ok || containsAggregation(alias) {
				continue
			}

			name := strings.ToLower(alias.Name())
			// Only if the alias is required in the grouping set needsReorder
			// to true. If it's not required, there's no need for a reorder if
			// no other alias is required.
			_, ok = groupingColumns[name]
			if ok && groupingColumns[name].Table() == "" {
				delete(groupingColumns, name)

				replacedAliases[alias.Child.String()] = alias.Name()
			}
		}

		var newExprs []sql.Expression
		for i, expr := range g.GroupByExprs {
			newExpr, isSame, err := transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if uc, ok := e.(*expression.UnresolvedColumn); ok {
					// If the "column"(named reference?)
					if uc.Table() != "" {
						return e, transform.SameTree, nil
					}

					// TODO: Need to understand nestingLevels better...
					//       For now, just search all levels, but this isn't correct
					for _, level := range symbols.levels() {
						if _, ok := symbols[level].availableAliases[strings.ToLower(uc.Name())]; ok {
							// If the unresolved column is not qualified with a table and there exists an available alias with
							// the same name, then it must be an alias reference because that's higher precedence than a column name.
							return expression.NewAliasReference(uc.Name()), transform.NewTree, nil
						}
					}
				}

				return e, transform.SameTree, nil
			})
			if err != nil {
				return n, transform.SameTree, err
			}
			// TODO: what is a "named reference"? It can be a column in a table, a column from a projection, a variable, or an alias
			if !isSame {
				if newExprs == nil {
					newExprs = make([]sql.Expression, len(g.GroupByExprs))
					copy(newExprs, g.GroupByExprs)
				}
				newExprs[i] = newExpr
			}
		}

		if newExprs == nil {
			return n, transform.SameTree, nil
		} else {
			g.GroupByExprs = newExprs
			return g, transform.NewTree, nil
		}
	})
}

// pushdownGroupByAliases reorders the aggregation in a groupby so aliases defined in it can be resolved in the grouping
// of the groupby. To do so, all aliases are pushed down to a projection node under the group by.
func pushdownGroupByAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if n.Resolved() {
		return n, transform.SameTree, nil
	}

	// replacedAliases is a map of original expression string to alias that has been pushed down below the GroupBy in
	// the new projection node.
	replacedAliases := make(map[string]string)
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// TODO: Don't we still need to do this?
		// For any unresolved alias references above the GroupBy, we need to apply the same alias replacement as we did in the
		// GroupBy itself.
		//ex, ok := n.(sql.Expressioner)
		//if ok && len(replacedAliases) > 0 {
		//	newExprs, same := replaceExpressionsWithAliases(ex.Expressions(), replacedAliases)
		//	if !same {
		//		n, err = ex.WithExpressions(newExprs...)
		//		return n, transform.NewTree, err
		//	}
		//}

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
		newGroupBys, _ = updateQualifiedColumns(g.GroupByExprs, replacedAliases)

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

func updateQualifiedColumns(exprs []sql.Expression, replacedAliases map[string]string) ([]sql.Expression, transform.TreeIdentity) {
	// TODO: Don't we need to transform on the expressions to processes them recursively and not just iterate? YES!
	//       This works for the replacement done by pushdownGroupByAliases, but... it needs to be done recursively
	//       for all nested expressions in order to identify aliases.
	var newExprs []sql.Expression
	for i := range exprs {
		switch c := exprs[i].(type) {
		case column:
			// If a column expression was previously qualified against a table, we need to unqualify it,
			// because it is now coming from a projection that we are inserting above the raw table source.
			// TODO: Is this good enough or should we compare to the expected table name? (seems good enuf)
			// Ugh... this won't work in the case where a column was already qualified as part of the original
			// statement. We can't unqualify that and change it to an alias or unresolved column. We need to identify
			// the aliases better up front...

			shouldUnqualify := false
			for _, value := range replacedAliases {
				if value == c.Name() {
					shouldUnqualify = true
				}
			}
			if c.Table() != "" && shouldUnqualify {
				if newExprs == nil {
					newExprs = make([]sql.Expression, len(exprs))
					copy(newExprs, exprs)
				}
				newExprs[i] = expression.NewUnresolvedColumn(c.Name())
			}
		}
	}
	if len(newExprs) > 0 {
		return newExprs, transform.NewTree
	}
	return exprs, transform.SameTree
}

// replaceExpressionsWithAliases replaces any expressions in the slice given that match the map of aliases given with
// their alias expression. This is necessary when pushing aliases down the tree, since we introduce a projection node
// that effectively erases the original columns of a table.
func replaceExpressionsWithAliases(exprs []sql.Expression, replacedAliases map[string]string) ([]sql.Expression, transform.TreeIdentity) {
	var newExprs []sql.Expression
	var expr sql.Expression
	for i := range exprs {
		expr = exprs[i]
		if alias, ok := replacedAliases[expr.String()]; ok {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = expression.NewUnresolvedColumn(alias)
		}
	}
	if len(newExprs) > 0 {
		return newExprs, transform.NewTree
	}
	return exprs, transform.SameTree
}

func replaceExpressionsWithAliasReferences2(exprs []sql.Expression, symbols availableNames) ([]sql.Expression, transform.TreeIdentity) {
	var newExprs []sql.Expression
	// TODO: Does this need to be a recursive transform and not just an iteration?
	for i := range exprs {
		switch e := exprs[i].(type) {
		case *expression.AliasReference:
			fmt.Println("Found AliasReference")
		case *expression.UnresolvedColumn:
			// If an unknown column does not have a table name, then check if it's an alias
			if e.Table() == "" {
				foundAlias := true // TODO: for now... let's assume everything without a table name is an alias?

				name := strings.ToLower(e.Name())
				levels := symbols.levels()

				// TODO: Huh?
				if symbols.conflictingAlias(name, len(symbols)) {
					// A higher scope produces an alias with this name.
					// We override the outer scope and qualify this column
					// only if the current scope provides a definition.
					levels = levels[len(symbols)-1:]
				}

				for _, level := range levels {
					tablesForColumn := symbols.tablesForColumnAtLevel(name, level)
					fmt.Println(tablesForColumn)
				}

				if foundAlias {
					if newExprs == nil {
						newExprs = make([]sql.Expression, len(exprs))
						copy(newExprs, exprs)
					}
					newExprs[i] = expression.NewAliasReference(e.Name())
				}
			}
		default:
			fmt.Printf("Found non-UnresolvedColumn: %s (%T)", e.String(), e)
		}
	}
	if len(newExprs) > 0 {
		return newExprs, transform.NewTree
	}
	return exprs, transform.SameTree
}

func replaceExpressionsWithAliasReferences(exprs []sql.Expression, replacedAliases map[string]string) ([]sql.Expression, transform.TreeIdentity) {
	var newExprs []sql.Expression
	var expr sql.Expression
	for i := range exprs {
		expr = exprs[i]
		if alias, ok := replacedAliases[expr.String()]; ok {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			// TODO: This is always guaranteed to be an alias reference, right?
			newExprs[i] = expression.NewAliasReference(alias)
		}
	}
	if len(newExprs) > 0 {
		return newExprs, transform.NewTree
	}
	return exprs, transform.SameTree
}

// TODO: UnresolvedColumn and AliasReferences need a better interface type to unify them.
//
//	"NamedReference" perhaps?
func findAllColumns(e sql.Expression) []column {
	var cols []column
	sql.Inspect(e, func(e sql.Expression) bool {
		uc, ok := e.(*expression.UnresolvedColumn)
		if ok {
			cols = append(cols, uc)
		}
		return true
	})
	sql.Inspect(e, func(e sql.Expression) bool {
		ar, ok := e.(*expression.AliasReference)
		if ok {
			cols = append(cols, ar)
		}
		return true
	})

	return cols
}
