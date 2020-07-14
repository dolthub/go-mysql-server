package analyzer

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/go-mysql-server/internal/similartext"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func checkAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("check_aliases")
	defer span.Finish()

	tableAliases, err := getTableAliases(n)
	if err != nil {
		return nil, err
	}

	tableNames := getTableNames(n)
	for _, tableName := range tableNames {
		if _, ok := tableAliases[strings.ToLower(tableName)]; ok {
			return nil, sql.ErrDuplicateAliasOrTable.New(tableName)
		}
	}

	plan.Inspect(n, func(node sql.Node) bool {
		p, ok := node.(*plan.Project)
		if !ok {
			return true
		}

		aliases := lookForAliasDeclarations(p)
		for alias := range aliases {
			if isAliasUsed(p, alias) {
				err = ErrMisusedAlias.New(alias)
			}
		}

		return true
	})

	return n, err
}

func lookForAliasDeclarations(node sql.Expressioner) map[string]struct{} {
	var (
		aliases = map[string]struct{}{}
		in      = struct{}{}
	)

	for _, e := range node.Expressions() {
		sql.Inspect(e, func(expr sql.Expression) bool {
			if alias, ok := expr.(*expression.Alias); ok {
				aliases[alias.Name()] = in
			}

			return true
		})
	}

	return aliases
}

func isAliasUsed(node sql.Expressioner, alias string) bool {
	var found bool
	for _, e := range node.Expressions() {
		sql.Inspect(e, func(expr sql.Expression) bool {
			if a, ok := expr.(*expression.Alias); ok {
				if a.Name() == alias {
					return false
				}

				return true
			}

			if n, ok := expr.(sql.Nameable); ok && n.Name() == alias {
				found = true
				return false
			}

			return true
		})

		if found {
			break
		}
	}

	return found
}

// deferredColumn is a wrapper on UnresolvedColumn used only to defer the
// resolution of the column because it may require some work done by
// other analyzer phases.
type deferredColumn struct {
	*expression.UnresolvedColumn
}

// IsNullable implements the Expression interface.
func (deferredColumn) IsNullable() bool {
	return true
}

// Children implements the Expression interface.
func (deferredColumn) Children() []sql.Expression { return nil }

// WithChildren implements the Expression interface.
func (e deferredColumn) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 0)
	}
	return e, nil
}

type tableCol struct {
	table string
	col   string
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

type availableColumns map[string]map[int][]string
type availableTables map[string]string

// qualifyColumns assigns a table to any column expressions that don't have one already
func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if _, ok := n.(sql.Expressioner); !ok || n.Resolved() {
			return n, nil
		}

		columns := getNodeAvailableColumns(n, scope)
		tables := getTableNamesInNode(n, scope)

		return plan.TransformExpressions(n, func(e sql.Expression) (sql.Expression, error) {
			return qualifyExpression(e, columns, tables)
		})
	})
}

func qualifyExpression(e sql.Expression, columns availableColumns, tables availableTables) (sql.Expression, error) {
	switch col := e.(type) {
	case column:
		// Skip this step for global and session variables
		if isGlobalOrSessionColumn(col) {
			return col, nil
		}

		// If this column is already qualified, make sure the table name is known
		if col.Table() != "" {
			if _, ok := tables[strings.ToLower(col.Table())]; !ok {
				return nil, sql.ErrTableNotFound.New(col.Table())
			}
			return col, nil
		}

		name, table := strings.ToLower(col.Name()), strings.ToLower(col.Table())

		nestingLevels := columns.nestingLevels(name)

		// If there are no tables found for this column in any scope, we'll never enter the loop below. Assume that this
		// column will be resolved later.
		if len(nestingLevels) == 0 {
			return col, nil
		}

		for _, level := range nestingLevels {
			tablesForColumn := columns.tablesAtLevel(name, level)
			if table != "" {
				table, ok := tables[table]
				if !ok {
					if len(tables) == 0 {
						return nil, sql.ErrTableNotFound.New(col.Table())
					}

					similar := similartext.FindFromMap(tables, col.Table())
					return nil, sql.ErrTableNotFound.New(col.Table() + similar)
				}

				// If the table exists but it's not available for this node it
				// means some work is still needed, so just return the column
				// and let it be resolved in the next pass.
				if !stringContains(tablesForColumn, table) {
					return col, nil
				}

				return expression.NewUnresolvedQualifiedColumn(table, col.Name()), nil
			}

			switch len(tablesForColumn) {
			case 0:
				// If there are no tables that have any column with the column
				// name let's just return it as it is. This may be an alias, so
				// we'll wait for the reorder of the projection.
				return col, nil
			case 1:
				return expression.NewUnresolvedQualifiedColumn(
					tablesForColumn[0],
					col.Name(),
				), nil
			default:
				return nil, ErrAmbiguousColumnName.New(col.Name(), strings.Join(tablesForColumn, ", "))
			}
		}

		return nil, ErrInAnalysis.New("Should have made a decision already")
	case *expression.Star:
		if col.Table != "" {
			if _, ok := tables[strings.ToLower(col.Table)]; !ok {
				return nil, sql.ErrTableNotFound.New(col.Table)
			}
		}
		return col, nil
	default:
		// If any other kind of expression has a star, just replace it
		// with an unqualified star because it cannot be expanded.
		return expression.TransformUp(e, func(e sql.Expression) (sql.Expression, error) {
			if _, ok := e.(*expression.Star); ok {
				return expression.NewStar(), nil
			}
			return e, nil
		})
	}
}

func getNodeAvailableColumns(node sql.Node, scope *Scope) availableColumns {
	var columns = make(availableColumns)
	// Examine all columns, from the innermost scope (this one) outward.
	getColumnsInNodes(node.Children(), columns, 0)
	for i, n := range scope.Nodes() {
		// For the inner scope, we want all available columns in child nodes. For the outer scope, we are interested in
		// available columns in the sibling node
		getColumnsInNodes([]sql.Node{n}, columns, i+1)
	}
	return columns
}

// indexColumn adds a column with the given table and column name at the given nesting level
func (a availableColumns) indexColumn(table, col string, nestingLevel int) {
	col = strings.ToLower(col)
	_, ok := a[col]
	if !ok {
		a[col] = make(map[int][]string)
	}
	if !stringContains(a[col][nestingLevel], strings.ToLower(table)) {
		a[col][nestingLevel] = append(a[col][nestingLevel], strings.ToLower(table))
	}
}

func (a availableColumns) nestingLevels(column string) []int {
	var nestingLevels []int
	for s := range a[column] {
		nestingLevels = append(nestingLevels, s)
	}
	return nestingLevels
}

func (a availableColumns) tablesAtLevel(column string, nestingLevel int) []string {
	if tablesAtLevel, ok :=  a[column]; ok {
		return tablesAtLevel[nestingLevel]
	}
	return nil
}

func getColumnsInNodes(nodes []sql.Node, columns availableColumns, nestingLevel int) {
	indexExpressions := func(exprs []sql.Expression) {
		for _, e := range exprs {
			switch e := e.(type) {
			case *expression.Alias:
				columns.indexColumn("", e.Name(), nestingLevel)
			case *expression.GetField:
				columns.indexColumn(e.Table(), e.Name(), nestingLevel)
			case *expression.UnresolvedColumn:
				columns.indexColumn(e.Table(), e.Name(), nestingLevel)
			}
		}
	}

	for _, node := range nodes {
		switch n := node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable, *plan.SubqueryAlias:
			for _, col := range n.Schema() {
				columns.indexColumn(col.Source, col.Name, nestingLevel)
			}
		case *plan.Project:
			indexExpressions(n.Projections)
		case *plan.GroupBy:
			indexExpressions(n.SelectedExprs)
		default:
			getColumnsInNodes(n.Children(), columns, nestingLevel)
		}
	}
}

// getNodeAvailableTables returns the set of table names and table aliases in the node given, keyed by their
// lower-cased names. Table aliases overwrite table names: the original name is not considered accessible once aliased.
// The value of the map is the same as the key, just used for existence checks.
func getTableNamesInNode(node sql.Node, scope *Scope) availableTables {
	tables := make(map[string]string)

	// Get table names in all outer scopes and nodes. Inner scoped names will overwrite those from the outer scope.
	for _, n := range append(append(([]sql.Node)(nil), node), scope.Nodes()...) {
		plan.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.SubqueryAlias, *plan.ResolvedTable:
				name := strings.ToLower(n.(sql.Nameable).Name())
				tables[name] = name
				return false
			case *plan.TableAlias:
				switch t := n.Child.(type) {
				case *plan.ResolvedTable, *plan.UnresolvedTable, *plan.SubqueryAlias:
					name := strings.ToLower(t.(sql.Nameable).Name())
					alias := strings.ToLower(n.Name())
					tables[alias] = name
				}
				return false
			}

			return true
		})
	}
	return tables
}

// GetTableNames returns the names of all tables in the node given. Aliases aren't considered.
func getTableNames(n sql.Node) []string {
	names := make([]string, 0)
	plan.Inspect(n, func(node sql.Node) bool {
		switch x := node.(type) {
		case *plan.UnresolvedTable:
			names = append(names, x.Name())
		case *plan.ResolvedTable:
			names = append(names, x.Name())
		}

		return true
	})

	return names
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
		for _, c := range n.Children() {
			if !c.Resolved() {
				return n, nil
			}
		}

		columns := findChildIndexedColumns(n)
		return plan.TransformExpressions(n, func(e sql.Expression) (sql.Expression, error) {
			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, nil
			}

			if isGlobalOrSessionColumn(uc) {
				return resolveGlobalOrSessionColumn(ctx, a, uc)
			}

			return resolveColumnExpression(ctx, a, uc, columns)
		})
	})
}

func findChildIndexedColumns(n sql.Node) map[tableCol]indexedCol {
	var idx int
	var columns = make(map[tableCol]indexedCol)

	for _, child := range n.Children() {
		childSch := child.Schema()
		for _, col := range childSch {
			columns[tableCol{
				table: strings.ToLower(col.Source),
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			idx++
		}
	}

	return columns
}

func resolveGlobalOrSessionColumn(ctx *sql.Context, a *Analyzer, col column) (sql.Expression, error) {
	if col.Table() != "" && strings.ToLower(col.Table()) != sessionTable {
		return nil, errGlobalVariablesNotSupported.New(col)
	}

	name := strings.TrimLeft(col.Name(), "@")
	name = strings.TrimPrefix(strings.TrimPrefix(name, globalPrefix), sessionPrefix)
	typ, value := ctx.Get(name)

	a.Log("resolved column %s to session field %s (type %s)", col, value, typ)
	return expression.NewGetSessionField(name, typ, value), nil
}

func resolveColumnExpression(ctx *sql.Context, a *Analyzer, e column, columns map[tableCol]indexedCol) (sql.Expression, error) {
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
				return nil, ErrColumnTableNotFound.New(e.Table(), e.Name())
			}

			return nil, ErrColumnNotFound.New(e.Name())
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

		var aggregateColumns = make(map[string]struct{})
		for _, agg := range g.SelectedExprs {
			// This alias is going to be pushed down, so don't bother gathering
			// its requirements.
			if alias, ok := agg.(*expression.Alias); ok {
				if _, ok := groupingColumns[strings.ToLower(alias.Name())]; ok {
					continue
				}
			}

			for _, n := range findAllColumns(agg) {
				aggregateColumns[strings.ToLower(n)] = struct{}{}
			}
		}

		var newAggregate []sql.Expression
		var projection []sql.Expression
		// Aliases will keep the aliases that have been pushed down and their
		// index in the new aggregate.
		var aliases = make(map[string]int)

		var needsReorder bool
		for _, a := range g.SelectedExprs {
			alias, ok := a.(*expression.Alias)
			// Note that aliases of aggregations cannot be used in the grouping
			// because the grouping is needed before computing the aggregation.
			if !ok || containsAggregation(alias) {
				newAggregate = append(newAggregate, a)
				continue
			}

			name := strings.ToLower(alias.Name())
			// Only if the alias is required in the grouping set needsReorder
			// to true. If it's not required, there's no need for a reorder if
			// no other alias is required.
			_, ok = groupingColumns[name]
			if ok {
				aliases[name] = len(newAggregate)
				needsReorder = true
				delete(groupingColumns, name)

				projection = append(projection, a)
				newAggregate = append(newAggregate, expression.NewUnresolvedColumn(alias.Name()))
			} else {
				newAggregate = append(newAggregate, a)
			}
		}

		if !needsReorder {
			return n, nil
		}

		// Instead of iterating columns directly, we want them sorted so the
		// executions of the rule are consistent.
		var missingCols = make([]string, 0, len(aggregateColumns)+len(groupingColumns))
		for col := range aggregateColumns {
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
			for i, expr := range newAggregate {
				var err error
				newAggregate[i], err = expression.TransformUp(expr, func(e sql.Expression) (sql.Expression, error) {
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
			newAggregate, g.GroupByExprs,
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

func isGlobalOrSessionColumn(col column) bool {
	return strings.HasPrefix(col.Name(), "@@") || strings.HasPrefix(col.Table(), "@@")
}
