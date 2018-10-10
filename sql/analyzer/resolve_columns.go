package analyzer

import (
	"fmt"
	"sort"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
	"gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
)

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

// column is the common interface that groups UnresolvedColumn and deferredColumn.
type column interface {
	sql.Nameable
	sql.Tableable
	sql.Expression
}

func qualifyColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("qualify_columns")
	defer span.Finish()

	a.Log("qualify columns")
	tables := make(map[string]sql.Node)
	tableAliases := make(map[string]string)
	colIndex := make(map[string][]string)

	indexCols := func(table string, schema sql.Schema) {
		for _, col := range schema {
			colIndex[col.Name] = append(colIndex[col.Name], table)
		}
	}

	var projects, seenProjects int
	plan.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(*plan.Project); ok {
			projects++
		}
		return true
	})

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		switch n := n.(type) {
		case *plan.TableAlias:
			switch t := n.Child.(type) {
			case *plan.ResolvedTable, *plan.UnresolvedTable:
				name := t.(sql.Nameable).Name()
				tableAliases[n.Name()] = name
			default:
				tables[n.Name()] = n.Child
				indexCols(n.Name(), n.Schema())
			}
		case *plan.ResolvedTable, *plan.SubqueryAlias:
			name := n.(sql.Nameable).Name()
			tables[name] = n
			indexCols(name, n.Schema())
		}

		result, err := n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
			a.Log("transforming expression of type: %T", e)
			switch col := e.(type) {
			case *expression.UnresolvedColumn:
				// Skip this step for global and session variables
				if isGlobalOrSessionColumn(col) {
					return col, nil
				}

				col = expression.NewUnresolvedQualifiedColumn(col.Table(), col.Name())

				if col.Table() == "" {
					tables := dedupStrings(colIndex[col.Name()])
					switch len(tables) {
					case 0:
						// If there are no tables that have any column with the column
						// name let's just return it as it is. This may be an alias, so
						// we'll wait for the reorder of the
						return col, nil
					case 1:
						col = expression.NewUnresolvedQualifiedColumn(
							tables[0],
							col.Name(),
						)
					default:
						return nil, ErrAmbiguousColumnName.New(col.Name(), strings.Join(tables, ", "))
					}
				} else {
					if real, ok := tableAliases[col.Table()]; ok {
						col = expression.NewUnresolvedQualifiedColumn(
							real,
							col.Name(),
						)
					}

					if _, ok := tables[col.Table()]; !ok {
						return nil, sql.ErrTableNotFound.New(col.Table())
					}
				}

				a.Log("column %q was qualified with table %q", col.Name(), col.Table())
				return col, nil
			case *expression.Star:
				if col.Table != "" {
					if real, ok := tableAliases[col.Table]; ok {
						col = expression.NewQualifiedStar(real)
					}

					if _, ok := tables[col.Table]; !ok {
						return nil, sql.ErrTableNotFound.New(col.Table)
					}

					return col, nil
				}
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

			return e, nil
		})

		if err != nil {
			return nil, err
		}

		// We should ignore the topmost project, because some nodes are
		// reordered, such as Sort, and they would not be resolved well.
		if n, ok := result.(*plan.Project); ok && projects-seenProjects > 1 {
			seenProjects++

			// We need to modify the indexed columns to only contain what is
			// projected in this project. If the column is not qualified by any
			// table, just keep the ones that are currently in the index.
			// If it is, then just make those tables available for the column.
			// If we don't do this, columns that are not projected will be
			// available in this step and may cause false errors or unintended
			// results.
			var projected = make(map[string][]string)
			for _, p := range n.Projections {
				var table, col string
				switch p := p.(type) {
				case column:
					table = p.Table()
					col = p.Name()
				case *expression.GetField:
					table = p.Table()
					col = p.Name()
				default:
					continue
				}

				if table != "" {
					projected[col] = append(projected[col], table)
				} else {
					projected[col] = append(projected[col], colIndex[col]...)
				}
			}

			colIndex = make(map[string][]string)
			for col, tables := range projected {
				colIndex[col] = dedupStrings(tables)
			}
		}

		return result, nil
	})
}

var errGlobalVariablesNotSupported = errors.NewKind("can't resolve global variable, %s was requested")

func resolveColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_columns")
	defer span.Finish()

	a.Log("resolve columns, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		colMap := make(map[string][]*sql.Column)
		for _, child := range n.Children() {
			if !child.Resolved() {
				return n, nil
			}

			for _, col := range child.Schema() {
				colMap[col.Name] = append(colMap[col.Name], col)
			}
		}

		var (
			aliasMap = make(map[string]struct{})
			exists   = struct{}{}
		)
		if project, ok := n.(*plan.Project); ok {
			for _, e := range project.Projections {
				if alias, ok := e.(*expression.Alias); ok {
					aliasMap[alias.Name()] = exists
				}
			}
		}

		expressioner, ok := n.(sql.Expressioner)
		if !ok {
			return n, nil
		}

		// make sure all children are resolved before resolving a node
		for _, c := range n.Children() {
			if !c.Resolved() {
				a.Log("a children with type %T of node %T were not resolved, skipping", c, n)
				return n, nil
			}
		}

		return expressioner.TransformExpressions(func(e sql.Expression) (sql.Expression, error) {
			a.Log("transforming expression of type: %T", e)
			if e.Resolved() {
				return e, nil
			}

			uc, ok := e.(column)
			if !ok {
				return e, nil
			}

			const (
				sessionTable  = "@@" + sqlparser.SessionStr
				sessionPrefix = sqlparser.SessionStr + "."
				globalPrefix  = sqlparser.GlobalStr + "."
			)
			columns, ok := colMap[uc.Name()]
			if !ok {
				switch uc := uc.(type) {
				case *expression.UnresolvedColumn:
					if isGlobalOrSessionColumn(uc) {
						if uc.Table() != "" && strings.ToLower(uc.Table()) != sessionTable {
							return nil, errGlobalVariablesNotSupported.New(uc)
						}

						name := strings.TrimLeft(uc.Name(), "@")
						if strings.HasPrefix(name, sessionPrefix) {
							name = name[len(sessionPrefix):]
						} else if strings.HasPrefix(name, globalPrefix) {
							name = name[len(globalPrefix):]
						}
						typ, value := ctx.Get(name)
						return expression.NewGetSessionField(name, typ, value), nil
					}

					a.Log("evaluation of column %q was deferred", uc.Name())
					return &deferredColumn{uc}, nil

				default:
					if uc.Table() != "" {
						return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
					}

					if _, ok := aliasMap[uc.Name()]; ok {
						// no nested aliases
						return nil, ErrMisusedAlias.New(uc.Name())
					}

					return nil, ErrColumnNotFound.New(uc.Name())
				}
			}

			var col *sql.Column
			var found bool
			for _, c := range columns {
				if c.Source == uc.Table() {
					col = c
					found = true
					break
				}
			}

			if !found {
				if uc.Table() != "" {
					return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
				}

				switch uc := uc.(type) {
				case *expression.UnresolvedColumn:
					return &deferredColumn{uc}, nil
				default:
					return nil, ErrColumnNotFound.New(uc.Name())
				}
			}

			var schema sql.Schema
			// If expressioner and unary node we must take the
			// child's schema to correctly select the indexes
			// in the row is going to be evaluated in this node
			if plan.IsUnary(n) {
				schema = n.Children()[0].Schema()
			} else {
				schema = n.Schema()
			}

			idx := schema.IndexOf(col.Name, col.Source)
			if idx < 0 {
				return nil, ErrColumnNotFound.New(col.Name)
			}

			a.Log("column resolved to %q.%q", col.Source, col.Name)

			return expression.NewGetFieldWithTable(
				idx,
				col.Type,
				col.Source,
				col.Name,
				col.Nullable,
			), nil
		})
	})
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
				groupingColumns[n] = struct{}{}
			}
		}

		var aggregateColumns = make(map[string]struct{})
		for _, agg := range g.Aggregate {
			// This alias is going to be pushed down, so don't bother gathering
			// its requirements.
			if alias, ok := agg.(*expression.Alias); ok {
				if _, ok := groupingColumns[alias.Name()]; ok {
					continue
				}
			}

			for _, n := range findAllColumns(agg) {
				aggregateColumns[n] = struct{}{}
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

			// Only if the alias is required in the grouping set needsReorder
			// to true. If it's not required, there's no need for a reorder if
			// no other alias is required.
			_, ok = groupingColumns[alias.Name()]
			if ok {
				aliases[alias.Name()] = len(newAggregate)
				needsReorder = true
				delete(groupingColumns, alias.Name())

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

func isGlobalOrSessionColumn(col *expression.UnresolvedColumn) bool {
	return strings.HasPrefix(col.Name(), "@@") || strings.HasPrefix(col.Table(), "@@")
}
