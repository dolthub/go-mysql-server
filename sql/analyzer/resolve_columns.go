package analyzer

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/internal/similartext"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
)

func checkAliases(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("check_aliases")
	defer span.Finish()

	a.Log("check aliases")

	var err error
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
		expression.Inspect(e, func(expr sql.Expression) bool {
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
		expression.Inspect(e, func(expr sql.Expression) bool {
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

func (e deferredColumn) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(e)
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

func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		exp, ok := n.(sql.Expressioner)
		if !ok || n.Resolved() {
			return n, nil
		}

		columns := getNodeAvailableColumns(n)
		tables := getNodeAvailableTables(n)

		return exp.TransformExpressions(func(e sql.Expression) (sql.Expression, error) {
			return qualifyExpression(e, columns, tables)
		})
	})
}

func qualifyExpression(
	e sql.Expression,
	columns map[string][]string,
	tables map[string]string,
) (sql.Expression, error) {
	switch col := e.(type) {
	case column:
		// Skip this step for global and session variables
		if isGlobalOrSessionColumn(col) {
			return col, nil
		}

		name, table := strings.ToLower(col.Name()), strings.ToLower(col.Table())
		availableTables := dedupStrings(columns[name])
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
			if !stringContains(availableTables, table) {
				return col, nil
			}

			return expression.NewUnresolvedQualifiedColumn(table, col.Name()), nil
		}

		switch len(availableTables) {
		case 0:
			// If there are no tables that have any column with the column
			// name let's just return it as it is. This may be an alias, so
			// we'll wait for the reorder of the projection.
			return col, nil
		case 1:
			return expression.NewUnresolvedQualifiedColumn(
				availableTables[0],
				col.Name(),
			), nil
		default:
			return nil, ErrAmbiguousColumnName.New(col.Name(), strings.Join(availableTables, ", "))
		}
	case *expression.Star:
		if col.Table != "" {
			if real, ok := tables[strings.ToLower(col.Table)]; ok {
				col = expression.NewQualifiedStar(real)
			}

			if _, ok := tables[strings.ToLower(col.Table)]; !ok {
				return nil, sql.ErrTableNotFound.New(col.Table)
			}
		}
		return col, nil
	default:
		// If any other kind of expression has a star, just replace it
		// with an unqualified star because it cannot be expanded.
		return e.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			if _, ok := e.(*expression.Star); ok {
				return expression.NewStar(), nil
			}
			return e, nil
		})
	}
}

func getNodeAvailableColumns(n sql.Node) map[string][]string {
	var columns = make(map[string][]string)
	getColumnsInNodes(n.Children(), columns)
	return columns
}

func getColumnsInNodes(nodes []sql.Node, columns map[string][]string) {
	indexCol := func(table, col string) {
		col = strings.ToLower(col)
		columns[col] = append(columns[col], strings.ToLower(table))
	}

	indexExpressions := func(exprs []sql.Expression) {
		for _, e := range exprs {
			switch e := e.(type) {
			case *expression.Alias:
				indexCol("", e.Name())
			case *expression.GetField:
				indexCol(e.Table(), e.Name())
			case *expression.UnresolvedColumn:
				indexCol(e.Table(), e.Name())
			}
		}
	}

	for _, node := range nodes {
		switch n := node.(type) {
		case *plan.ResolvedTable, *plan.SubqueryAlias:
			for _, col := range n.Schema() {
				indexCol(col.Source, col.Name)
			}
		case *plan.Project:
			indexExpressions(n.Projections)
		case *plan.GroupBy:
			indexExpressions(n.Aggregate)
		default:
			getColumnsInNodes(n.Children(), columns)
		}
	}
}

func getNodeAvailableTables(n sql.Node) map[string]string {
	tables := make(map[string]string)
	getNodesAvailableTables(tables, n.Children()...)
	return tables
}

func getNodesAvailableTables(tables map[string]string, nodes ...sql.Node) {
	for _, n := range nodes {
		switch n := n.(type) {
		case *plan.SubqueryAlias, *plan.ResolvedTable:
			name := strings.ToLower(n.(sql.Nameable).Name())
			tables[name] = name
		case *plan.TableAlias:
			switch t := n.Child.(type) {
			case *plan.ResolvedTable, *plan.UnresolvedTable:
				name := strings.ToLower(t.(sql.Nameable).Name())
				alias := strings.ToLower(n.Name())
				tables[alias] = name
				// Also add the name of the table because you can refer to a
				// table with either the alias or the name.
				tables[name] = name
			}
		default:
			getNodesAvailableTables(tables, n.Children()...)
		}
	}
}

var errGlobalVariablesNotSupported = errors.NewKind("can't resolve global variable, %s was requested")

const (
	sessionTable  = "@@" + sqlparser.SessionStr
	sessionPrefix = sqlparser.SessionStr + "."
	globalPrefix  = sqlparser.GlobalStr + "."
)

func resolveColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_columns")
	defer span.Finish()

	a.Log("resolve columns, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		expressioner, ok := n.(sql.Expressioner)
		if !ok {
			return n, nil
		}

		// We need to use the schema, so all children must be resolved.
		for _, c := range n.Children() {
			if !c.Resolved() {
				return n, nil
			}
		}

		columns := findChildIndexedColumns(n)
		return expressioner.TransformExpressions(func(e sql.Expression) (sql.Expression, error) {
			a.Log("transforming expression of type: %T", e)

			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, nil
			}

			if isGlobalOrSessionColumn(uc) {
				return resolveGlobalOrSessionColumn(ctx, uc)
			}

			return resolveColumnExpression(ctx, uc, columns)
		})
	})
}

func findChildIndexedColumns(n sql.Node) map[tableCol]indexedCol {
	var idx int
	var columns = make(map[tableCol]indexedCol)

	for _, child := range n.Children() {
		for _, col := range child.Schema() {
			columns[tableCol{
				table: strings.ToLower(col.Source),
				col:   strings.ToLower(col.Name),
			}] = indexedCol{col, idx}
			idx++
		}
	}

	return columns
}

func resolveGlobalOrSessionColumn(ctx *sql.Context, col column) (sql.Expression, error) {
	if col.Table() != "" && strings.ToLower(col.Table()) != sessionTable {
		return nil, errGlobalVariablesNotSupported.New(col)
	}

	name := strings.TrimLeft(col.Name(), "@")
	name = strings.TrimPrefix(strings.TrimPrefix(name, globalPrefix), sessionPrefix)
	typ, value := ctx.Get(name)
	return expression.NewGetSessionField(name, typ, value), nil
}

func resolveColumnExpression(
	ctx *sql.Context,
	e column,
	columns map[tableCol]indexedCol,
) (sql.Expression, error) {
	name := strings.ToLower(e.Name())
	table := strings.ToLower(e.Table())
	col, ok := columns[tableCol{table, name}]
	if !ok {
		switch uc := e.(type) {
		case *expression.UnresolvedColumn:
			// Defer the resolution of the column to give the analyzer more
			// time to resolve other parts so this can be resolved.
			return &deferredColumn{uc}, nil
		default:
			if table != "" {
				return nil, ErrColumnTableNotFound.New(e.Table(), e.Name())
			}

			return nil, ErrColumnNotFound.New(e.Name())
		}
	}

	return expression.NewGetFieldWithTable(
		col.index,
		col.Type,
		col.Source,
		col.Name,
		col.Nullable,
	), nil
}

// resolveGroupingColumns reorders the aggregation in a groupby so aliases
// defined in it can be resolved in the grouping of the groupby. To do so,
// all aliases are pushed down to a projection node under the group by.
func resolveGroupingColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	a.Log("resoving group columns")
	if n.Resolved() {
		return n, nil
	}

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		g, ok := n.(*plan.GroupBy)
		if n.Resolved() || !ok || len(g.Grouping) == 0 {
			return n, nil
		}

		// The reason we have two sets of columns, one for grouping and
		// one for aggregate is because an alias can redefine a column name
		// of the child schema. In the grouping, if that column is referenced
		// it refers to the alias, and not the one in the child. However,
		// in the aggregate, aliases in that same aggregate cannot be used,
		// so it refers to the column in the child node.
		var groupingColumns = make(map[string]struct{})
		for _, g := range g.Grouping {
			for _, n := range findAllColumns(g) {
				groupingColumns[strings.ToLower(n)] = struct{}{}
			}
		}

		var aggregateColumns = make(map[string]struct{})
		for _, agg := range g.Aggregate {
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
		for _, a := range g.Aggregate {
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
				projection = append(projection, expression.NewAlias(
					expression.NewUnresolvedColumn(col),
					name,
				))
			}
		}

		// If there is any name conflict between columns we need to rename every
		// usage inside the aggregate.
		if len(renames) > 0 {
			for i, expr := range newAggregate {
				var err error
				newAggregate[i], err = expr.TransformUp(func(e sql.Expression) (sql.Expression, error) {
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
			newAggregate, g.Grouping,
			plan.NewProject(projection, g.Child),
		), nil
	})
}

func findAllColumns(e sql.Expression) []string {
	var cols []string
	expression.Inspect(e, func(e sql.Expression) bool {
		col, ok := e.(*expression.UnresolvedColumn)
		if ok {
			cols = append(cols, col.Name())
		}
		return true
	})
	return cols
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

func isGlobalOrSessionColumn(col column) bool {
	return strings.HasPrefix(col.Name(), "@@") || strings.HasPrefix(col.Table(), "@@")
}
