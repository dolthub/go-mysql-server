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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func checkUniqueTableNames(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// getTableAliases will error if any table name / alias is repeated
	_, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	return n, err
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
func (dc *deferredColumn) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
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

// column is the common interface that groups UnresolvedColumn and deferredColumn.
type column interface {
	sql.Nameable
	sql.Tableable
	sql.Expression
}

// nestingLevelSymbols tracks available table and column name symbols at a nesting level for a query. Each nested
// subquery represents an additional nesting level.
type nestingLevelSymbols struct {
	availableColumns map[string][]string
	availableTables  map[string]string
}

func newNestingLevelSymbols() nestingLevelSymbols {
	return nestingLevelSymbols{
		availableColumns: make(map[string][]string),
		availableTables:  make(map[string]string),
	}
}

// availableNames tracks available table and column name symbols at each nesting level for a query, where level 0
// is the node being analyzed, and each additional level is one layer of query scope outward.
type availableNames map[int]nestingLevelSymbols

// indexColumn adds a column with the given table and column name at the given nesting level
func (a availableNames) indexColumn(table, col string, nestingLevel int) {
	col = strings.ToLower(col)
	_, ok := a[nestingLevel]
	if !ok {
		a[nestingLevel] = newNestingLevelSymbols()
	}
	if !stringContains(a[nestingLevel].availableColumns[col], strings.ToLower(table)) {
		a[nestingLevel].availableColumns[col] = append(a[nestingLevel].availableColumns[col], strings.ToLower(table))
	}
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
func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if _, ok := n.(sql.Expressioner); !ok || n.Resolved() {
			return n, nil
		}

		symbols := getNodeAvailableNames(n, scope)

		return plan.TransformExpressions(ctx, n, func(e sql.Expression) (sql.Expression, error) {
			return qualifyExpression(ctx, e, symbols)
		})
	})
}

// getNodeAvailableSymbols returns the set of table and column names accessible to the node given and using the scope
// given. Table aliases overwrite table names: the original name is not considered accessible once aliased.
// The value of the map is the same as the key, just used for existence checks.
func getNodeAvailableNames(n sql.Node, scope *Scope) availableNames {
	names := make(availableNames)

	// Examine all columns, from the innermost scope (this one) outward.
	getColumnsInNodes(n.Children(), names, 0)
	for i, n := range scope.InnerToOuter() {
		// For the inner scope, we want all available columns in child nodes. For the outer scope, we are interested in
		// available columns in the sibling node
		getColumnsInNodes(n.Children(), names, i+1)
	}

	// Get table names in all outer scopes and nodes. Inner scoped names will overwrite those from the outer scope.
	for i, n := range append(append(([]sql.Node)(nil), n), scope.InnerToOuter()...) {
		plan.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.SubqueryAlias, *plan.ResolvedTable, *plan.ValueDerivedTable:
				name := strings.ToLower(n.(sql.Nameable).Name())
				names.indexTable(name, name, i)
				return false
			case *plan.TableAlias:
				switch t := n.Child.(type) {
				case *plan.ResolvedTable, *plan.UnresolvedTable, *plan.SubqueryAlias:
					name := strings.ToLower(t.(sql.Nameable).Name())
					alias := strings.ToLower(n.Name())
					names.indexTable(alias, name, i)
				}
				return false
			}

			return true
		})
	}

	return names
}

func qualifyExpression(ctx *sql.Context, e sql.Expression, symbols availableNames) (sql.Expression, error) {
	switch col := e.(type) {
	case column:
		if col.Resolved() {
			return col, nil
		}

		// Skip this step for variables
		if strings.HasPrefix(col.Name(), "@") || strings.HasPrefix(col.Table(), "@") {
			return col, nil
		}

		nestingLevels := symbols.nestingLevels()

		// if there are no tables or columns anywhere in the query, just give up and let another part of the analyzer throw
		// an analysis error. (for some queries, like SHOW statements, this is expected and not an error)
		if len(nestingLevels) == 0 {
			return col, nil
		}

		// TODO: more tests for error conditions

		// If this column is already qualified, make sure the table name is known
		if col.Table() != "" {
			// TODO: method for this
			tableFound := false
			for _, level := range nestingLevels {
				tables := symbols.tablesAtLevel(level)
				if _, ok := tables[strings.ToLower(col.Table())]; ok {
					tableFound = true
					break
				}
			}

			if !tableFound {
				similar := similartext.Find(symbols.allTables(), col.Table())
				return nil, sql.ErrTableNotFound.New(col.Table() + similar)
			}

			return col, nil
		}

		// Look in all the scope, inner to outer, to identify the column. Stop as soon as we have a scope with exactly 1
		// match for the column name. If any scope has ambiguity in available column names, that's an error.
		for _, level := range nestingLevels {
			name := strings.ToLower(col.Name())
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
				return expression.NewUnresolvedQualifiedColumn(
					tablesForColumn[0],
					col.Name(),
				), nil
			default:
				return nil, sql.ErrAmbiguousColumnName.New(col.Name(), strings.Join(tablesForColumn, ", "))
			}
		}

		// If there are no tables that have any column with the column name let's just return it as it is. This may be an
		// alias, so we'll wait for the reorder of the projection to resolve it.
		return col, nil
	case *expression.Star:
		// Make sure that any qualified stars reference known tables
		if col.Table != "" {
			nestingLevels := symbols.nestingLevels()
			tableFound := false
			for _, level := range nestingLevels {
				tables := symbols.tablesAtLevel(level)
				if _, ok := tables[strings.ToLower(col.Table)]; ok {
					tableFound = true
					break
				}
			}
			if !tableFound {
				return nil, sql.ErrTableNotFound.New(col.Table)
			}
		}
		return col, nil
	default:
		// If any other kind of expression has a star, just replace it
		// with an unqualified star because it cannot be expanded.
		return expression.TransformUp(ctx, e, func(e sql.Expression) (sql.Expression, error) {
			if _, ok := e.(*expression.Star); ok {
				return expression.NewStar(), nil
			}
			return e, nil
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
		case *plan.TableAlias, *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable:
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
// the expression's child node. Also handles replacing session variables (treated as columns) with their values.
func resolveColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_columns")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		if _, ok := n.(sql.Expressioner); !ok {
			return n, nil
		}

		// We need to use the schema, so all children must be resolved.
		// TODO: also enforce the equivalent constraint for outer scopes. More complicated, because the outer scope can't
		//  be Resolved() owing to a child expression (the one being evaluated) not being resolved yet.
		for _, c := range n.Children() {
			if !c.Resolved() {
				return n, nil
			}
		}

		columns, err := indexColumns(ctx, a, n, scope)
		if err != nil {
			return nil, err
		}

		return plan.TransformExpressionsWithNode(ctx, n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, nil
			}

			expr, ok, err := resolveSystemOrUserVariable(ctx, a, uc)
			if err != nil {
				return nil, err
			}
			if ok {
				return expr, nil
			}

			return resolveColumnExpression(ctx, a, n, uc, columns)
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
			indexColumn(expression.ExpressionToColumn(e))
			idx--
			indexColumnExpr(e.Child)
		default:
			indexColumn(expression.ExpressionToColumn(e))
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
	// itself.
	for _, child := range n.Children() {
		indexChildNode(child)
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
		for _, col := range node.Schema() {
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
		if tbl, ok, _ := node.Database().GetTableInsensitive(ctx, node.TableName()); ok {
			indexSchemaForDefaults(node.Column(), node.Order(), tbl.Schema())
		}
	case *plan.ModifyColumn:
		if tbl, ok, _ := node.Database().GetTableInsensitive(ctx, node.TableName()); ok {
			colIdx := tbl.Schema().IndexOf(node.Column(), node.TableName())
			if colIdx < 0 {
				return nil, sql.ErrTableColumnNotFound.New(node.TableName(), node.Column())
			}

			var newSch sql.Schema
			newSch = append(newSch, tbl.Schema()[:colIdx]...)
			newSch = append(newSch, tbl.Schema()[colIdx+1:]...)
			indexSchemaForDefaults(node.NewColumn(), node.Order(), newSch)
		}
	}

	return columns, nil
}

func resolveSystemOrUserVariable(ctx *sql.Context, a *Analyzer, col column) (sql.Expression, bool, error) {
	var varName string
	var scope sqlparser.SetScope
	var err error
	if col.Table() != "" {
		varName, scope, err = sqlparser.VarScope(col.Table(), col.Name())
		if err != nil {
			return nil, false, err
		}
	} else {
		varName, scope, err = sqlparser.VarScope(col.Name())
		if err != nil {
			return nil, false, err
		}
	}
	switch scope {
	case sqlparser.SetScope_None:
		return nil, false, nil
	case sqlparser.SetScope_Global:
		_, _, ok := sql.SystemVariables.GetGlobal(varName)
		if !ok {
			return nil, false, sql.ErrUnknownSystemVariable.New(varName)
		}
		a.Log("resolved column %s to global system variable", col)
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Global), true, nil
	case sqlparser.SetScope_Persist:
		return nil, false, sql.ErrUnsupportedFeature.New("PERSIST")
	case sqlparser.SetScope_PersistOnly:
		return nil, false, sql.ErrUnsupportedFeature.New("PERSIST_ONLY")
	case sqlparser.SetScope_Session:
		_, err = ctx.GetSessionVariable(ctx, varName)
		if err != nil {
			return nil, false, err
		}
		a.Log("resolved column %s to session system variable", col)
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Session), true, nil
	case sqlparser.SetScope_User:
		a.Log("resolved column %s to user variable", col)
		return expression.NewUserVar(varName), true, nil
	default: // shouldn't happen
		return nil, false, fmt.Errorf("unknown set scope %v", scope)
	}
}

func resolveColumnExpression(ctx *sql.Context, a *Analyzer, n sql.Node, e column, columns map[tableCol]indexedCol) (sql.Expression, error) {
	name := strings.ToLower(e.Name())
	table := strings.ToLower(e.Table())
	col, ok := columns[tableCol{table, name}]
	if !ok {
		switch uc := e.(type) {
		case *expression.UnresolvedColumn:
			// Defer the resolution of the column to give the analyzer more
			// time to resolve other parts so this can be resolved.
			a.Log("deferring resolution of column %s", e)
			return &deferredColumn{uc}, nil
		default:
			if table != "" {
				return nil, sql.ErrTableColumnNotFound.New(e.Table(), e.Name())
			}

			// This means the expression is either a non-existent column or an alias defined in the same projection.
			// Check for the latter first.
			aliasesInNode := aliasesDefinedInNode(n)
			if stringContains(aliasesInNode, name) {
				return nil, sql.ErrMisusedAlias.New(name)
			}

			return nil, sql.ErrColumnNotFound.New(e.Name())
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
	), nil
}

// pushdownGroupByAliases reorders the aggregation in a groupby so aliases defined in it can be resolved in the grouping
// of the groupby. To do so, all aliases are pushed down to a projection node under the group by.
func pushdownGroupByAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if n.Resolved() {
		return n, nil
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		g, ok := n.(*plan.GroupBy)
		if n.Resolved() || !ok || len(g.GroupByExprs) == 0 {
			return n, nil
		}

		// The reason we have two sets of columns, one for grouping and
		// one for aggregate is because an alias can redefine a column name
		// of the child schema. In the grouping, if that column is referenced
		// it refers to the alias, and not the one in the child. However,
		// in the aggregate, aliases in that same aggregate cannot be used,
		// so it refers to the column in the child node.
		var groupingColumns = make(map[string]struct{})
		for _, g := range g.GroupByExprs {
			for _, n := range findAllColumns(g) {
				groupingColumns[strings.ToLower(n)] = struct{}{}
			}
		}

		var selectedColumns = make(map[string]struct{})
		for _, agg := range g.SelectedExprs {
			// This alias is going to be pushed down, so don't bother gathering
			// its requirements.
			if alias, ok := agg.(*expression.Alias); ok {
				if _, ok := groupingColumns[strings.ToLower(alias.Name())]; ok {
					continue
				}
			}

			for _, n := range findAllColumns(agg) {
				selectedColumns[strings.ToLower(n)] = struct{}{}
			}
		}

		var newSelectedExprs []sql.Expression
		replacements := make(map[string]string)
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
			if ok {
				aliases[name] = len(newSelectedExprs)
				needsReorder = true
				delete(groupingColumns, name)

				projection = append(projection, expr)
				replacements[alias.Child.String()] = alias.Name()
				newSelectedExprs = append(newSelectedExprs, expression.NewUnresolvedColumn(alias.Name()))
			} else {
				newSelectedExprs = append(newSelectedExprs, expr)
			}
		}

		if !needsReorder {
			return n, nil
		}

		// Any replacements of aliases in the select expression must be mirrored in the group by, replacing any aliased
		// expressions with a reference to that alias. This is so that we can directly compare the group by an select
		// expressions for validation, which requires us to know that (table.column as col) and (table.column) are the
		// same expressions. So if we replace one, replace both.
		var newGroupBys []sql.Expression
		for _, expr := range g.GroupByExprs {
			if alias, ok := replacements[expr.String()]; ok {
				newGroupBys = append(newGroupBys, expression.NewUnresolvedColumn(alias))
			} else {
				newGroupBys = append(newGroupBys, expr)
			}
		}

		// Instead of iterating columns directly, we want them sorted so the
		// executions of the rule are consistent.
		var missingCols = make([]string, 0, len(selectedColumns)+len(groupingColumns))
		for col := range selectedColumns {
			missingCols = append(missingCols, col)
		}
		for col := range groupingColumns {
			missingCols = append(missingCols, col)
		}
		sort.Strings(missingCols)

		var renames = make(map[string]string)
		// All columns required by expressions in both grouping and aggregation
		// must also be projected in the new projection node or they will not
		// be able to resolve.
		for _, col := range missingCols {
			name := col
			// If an alias has been pushed down with the same name as a missing
			// column, there will be a conflict of names. We must find an unique name
			// for the missing column.
			if _, ok := aliases[col]; ok {
				for i := 1; ; i++ {
					name = fmt.Sprintf("%s_%02d", col, i)
					if !stringContains(missingCols, name) {
						break
					}
				}
			}

			if name == col {
				projection = append(projection, expression.NewUnresolvedColumn(col))
			} else {
				renames[col] = name
				projection = append(projection, expression.NewAlias(name, expression.NewUnresolvedColumn(col)))
			}
		}

		// If there is any name conflict between columns we need to rename every
		// usage inside the aggregate.
		if len(renames) > 0 {
			for i, expr := range newSelectedExprs {
				var err error
				newSelectedExprs[i], err = expression.TransformUp(ctx, expr, func(e sql.Expression) (sql.Expression, error) {
					col, ok := e.(*expression.UnresolvedColumn)
					if ok {
						// We need to make sure we don't rename the reference to the
						// pushed down alias.
						if to, ok := renames[col.Name()]; ok && aliases[col.Name()] != i {
							return expression.NewUnresolvedColumn(to), nil
						}
					}

					return e, nil
				})
				if err != nil {
					return nil, err
				}
			}
		}

		return plan.NewGroupBy(
			newSelectedExprs, newGroupBys,
			plan.NewProject(projection, g.Child),
		), nil
	})
}

func findAllColumns(e sql.Expression) []string {
	var cols []string
	sql.Inspect(e, func(e sql.Expression) bool {
		col, ok := e.(*expression.UnresolvedColumn)
		if ok {
			cols = append(cols, col.Name())
		}
		return true
	})
	return cols
}
