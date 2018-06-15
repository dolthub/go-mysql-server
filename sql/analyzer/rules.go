package analyzer

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{"resolve_subqueries", resolveSubqueries},
	{"resolve_tables", resolveTables},
	{"resolve_natural_joins", resolveNaturalJoins},
	{"resolve_orderby_literals", resolveOrderByLiterals},
	{"qualify_columns", qualifyColumns},
	{"resolve_columns", resolveColumns},
	{"resolve_database", resolveDatabase},
	{"resolve_star", resolveStar},
	{"resolve_functions", resolveFunctions},
	{"reorder_projection", reorderProjection},
	{"assign_indexes", assignIndexes},
	{"pushdown", pushdown},
	{"optimize_distinct", optimizeDistinct},
	{"erase_projection", eraseProjection},
	{"index_catalog", indexCatalog},
}

var (
	// ErrColumnTableNotFound is returned when the column does not exist in a
	// the table.
	ErrColumnTableNotFound = errors.NewKind("table %q does not have column %q")
	// ErrColumnNotFound is returned when the column does not exist in any
	// table in scope.
	ErrColumnNotFound = errors.NewKind("column %q could not be found in any table in scope")
	// ErrAmbiguousColumnName is returned when there is a column reference that
	// is present in more than one table.
	ErrAmbiguousColumnName = errors.NewKind("ambiguous column name %q, it's present in all these tables: %v")
	// ErrFieldMissing is returned when the field is not on the schema.
	ErrFieldMissing = errors.NewKind("field %q is not on schema")
	// ErrOrderByColumnIndex is returned when in an order clause there is a
	// column that is unknown.
	ErrOrderByColumnIndex = errors.NewKind("unknown column %d in order by clause")
)

func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.Finish()

	a.Log("resolving subqueries")
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			a.Log("found subquery %q with child of type %T", n.Name(), n.Child)
			child, err := a.Analyze(ctx, n.Child)
			if err != nil {
				return nil, err
			}
			return plan.NewSubqueryAlias(n.Name(), child), nil
		default:
			return n, nil
		}
	})
}

func resolveOrderByLiterals(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	a.Log("resolve order by literals")

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, nil
		}

		var fields = make([]plan.SortField, len(sort.SortFields))
		for i, f := range sort.SortFields {
			if lit, ok := f.Column.(*expression.Literal); ok && sql.IsNumber(f.Column.Type()) {
				// it is safe to eval literals with no context and/or row
				v, err := lit.Eval(nil, nil)
				if err != nil {
					return nil, err
				}

				v, err = sql.Int64.Convert(v)
				if err != nil {
					return nil, err
				}

				// column access is 1-indexed
				idx := int(v.(int64)) - 1

				schema := sort.Child.Schema()
				if idx >= len(schema) || idx < 0 {
					return nil, ErrOrderByColumnIndex.New(idx + 1)
				}

				fields[i] = plan.SortField{
					Column:       expression.NewUnresolvedColumn(schema[idx].Name),
					Order:        f.Order,
					NullOrdering: f.NullOrdering,
				}

				a.Log("replaced order by column %d with %s", idx+1, schema[idx].Name)
			} else {
				fields[i] = f
			}
		}

		return plan.NewSort(fields, sort.Child), nil
	})
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

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		switch n := n.(type) {
		case *plan.TableAlias:
			switch t := n.Child.(type) {
			case sql.Table:
				tableAliases[n.Name()] = t.Name()
			default:
				tables[n.Name()] = n.Child
				indexCols(n.Name(), n.Schema())
			}
		case sql.Table:
			tables[n.Name()] = n
			indexCols(n.Name(), n.Schema())
		}

		return n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
			a.Log("transforming expression of type: %T", e)
			switch col := e.(type) {
			case *expression.UnresolvedColumn:
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
			}
			return e, nil
		})
	})
}

func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_database")
	defer span.Finish()

	a.Log("resolve database, node of type: %T", n)

	// TODO Database should implement node,
	// and ShowTables and CreateTable nodes should be binaryNodes
	switch v := n.(type) {
	case *plan.ShowTables:
		db, err := a.Catalog.Database(a.CurrentDatabase)
		if err != nil {
			return n, err
		}

		v.Database = db
	case *plan.CreateTable:
		db, err := a.Catalog.Database(a.CurrentDatabase)
		if err != nil {
			return n, err
		}

		v.Database = db
	}

	return n, nil
}

var dualTable = func() sql.Table {
	t := mem.NewTable("dual", sql.Schema{
		{Name: "dummy", Source: "dual", Type: sql.Text, Nullable: false},
	})
	_ = t.Insert(sql.NewRow("x"))
	return t
}()

func resolveTables(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_tables")
	defer span.Finish()

	a.Log("resolve table, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		rt, err := a.Catalog.Table(a.CurrentDatabase, t.Name)
		if err != nil {
			if sql.ErrTableNotFound.Is(err) && t.Name == dualTable.Name() {
				rt = dualTable
			} else {
				return nil, err
			}
		}

		a.Log("table resolved: %q", rt.Name())

		return rt, nil
	})
}

func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_natural_joins")
	defer span.Finish()

	if n.Resolved() {
		return n, nil
	}

	a.Log("resolving natural joins, node of type %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		join, ok := n.(*plan.NaturalJoin)
		if !ok {
			return n, nil
		}

		// we need both leaves resolved before resolving this one
		if !join.Left.Resolved() || !join.Right.Resolved() {
			return n, nil
		}

		leftSchema, rightSchema := join.Left.Schema(), join.Right.Schema()

		var conditions, common, left, right []sql.Expression
		var seen = make(map[string]struct{})

		for i, lcol := range leftSchema {
			var found bool
			leftCol := expression.NewGetFieldWithTable(
				i,
				lcol.Type,
				lcol.Source,
				lcol.Name,
				lcol.Nullable,
			)

			for j, rcol := range rightSchema {
				if lcol.Name == rcol.Name {
					common = append(common, leftCol)

					conditions = append(
						conditions,
						expression.NewEquals(
							leftCol,
							expression.NewGetFieldWithTable(
								len(leftSchema)+j,
								rcol.Type,
								rcol.Source,
								rcol.Name,
								rcol.Nullable,
							),
						),
					)

					found = true
					seen[lcol.Name] = struct{}{}
					break
				}
			}

			if !found {
				left = append(left, leftCol)
			}
		}

		if len(conditions) == 0 {
			return plan.NewCrossJoin(join.Left, join.Right), nil
		}

		for i, col := range rightSchema {
			if _, ok := seen[col.Name]; !ok {
				right = append(
					right,
					expression.NewGetFieldWithTable(
						len(leftSchema)+i,
						col.Type,
						col.Source,
						col.Name,
						col.Nullable,
					),
				)
			}
		}

		projections := append(append(common, left...), right...)

		return plan.NewProject(
			projections,
			plan.NewInnerJoin(
				join.Left,
				join.Right,
				expression.JoinAnd(conditions...),
			),
		), nil
	})
}

func resolveStar(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_star")
	defer span.Finish()

	a.Log("resolving star, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			expressions, err := expandStars(n.Projections, n.Child.Schema())
			if err != nil {
				return nil, err
			}

			return plan.NewProject(expressions, n.Child), nil
		case *plan.GroupBy:
			aggregate, err := expandStars(n.Aggregate, n.Child.Schema())
			if err != nil {
				return nil, err
			}

			return plan.NewGroupBy(aggregate, n.Grouping, n.Child), nil
		default:
			return n, nil
		}
	})
}

func expandStars(exprs []sql.Expression, schema sql.Schema) ([]sql.Expression, error) {
	var expressions []sql.Expression
	for _, e := range exprs {
		if s, ok := e.(*expression.Star); ok {
			var exprs []sql.Expression
			for i, col := range schema {
				if s.Table == "" || s.Table == col.Source {
					exprs = append(exprs, expression.NewGetFieldWithTable(
						i, col.Type, col.Source, col.Name, col.Nullable,
					))
				}
			}

			if len(exprs) == 0 && s.Table != "" {
				return nil, sql.ErrTableNotFound.New(s.Table)
			}

			expressions = append(expressions, exprs...)
		} else {
			expressions = append(expressions, e)
		}
	}

	return expressions, nil
}

// maybeAlias is a wrapper on UnresolvedColumn used only to defer the
// resolution of the column because it could be an alias and that
// phase of the analyzer has not run yet.
type maybeAlias struct {
	*expression.UnresolvedColumn
}

func (e maybeAlias) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(e)
}

// column is the common interface that groups UnresolvedColumn and maybeAlias.
type column interface {
	sql.Nameable
	sql.Tableable
	sql.Expression
}

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

		expressioner, ok := n.(sql.Expressioner)
		if !ok {
			return n, nil
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

			columns, ok := colMap[uc.Name()]
			if !ok {
				if uc.Table() != "" {
					return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
				}

				switch uc := uc.(type) {
				case *expression.UnresolvedColumn:
					return &maybeAlias{uc}, nil
				default:
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
					return &maybeAlias{uc}, nil
				default:
					return nil, ErrColumnNotFound.New(uc.Name())
				}
			}

			var schema sql.Schema
			switch n := n.(type) {
			// If expressioner and unary node we must take the
			// child's schema to correctly select the indexes
			// in the row is going to be evaluated in this node
			case *plan.Project, *plan.Filter, *plan.GroupBy, *plan.Sort:
				schema = n.Children()[0].Schema()
			case *plan.CreateIndex:
				schema = n.Table.Schema()
			default:
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

func resolveFunctions(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_functions")
	defer span.Finish()

	a.Log("resolve functions, node of type %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		return n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
			a.Log("transforming expression of type: %T", e)
			if e.Resolved() {
				return e, nil
			}

			uf, ok := e.(*expression.UnresolvedFunction)
			if !ok {
				return e, nil
			}

			n := uf.Name()
			f, err := a.Catalog.Function(n)
			if err != nil {
				return nil, err
			}

			rf, err := f.Call(uf.Arguments...)
			if err != nil {
				return nil, err
			}

			a.Log("resolved function %q", n)

			return rf, nil
		})
	})
}

func optimizeDistinct(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("optimize_distinct")
	defer span.Finish()

	a.Log("optimize distinct, node of type: %T", node)
	if node, ok := node.(*plan.Distinct); ok {
		var isSorted bool
		_, _ = node.TransformUp(func(node sql.Node) (sql.Node, error) {
			a.Log("checking for optimization in node of type: %T", node)
			if _, ok := node.(*plan.Sort); ok {
				isSorted = true
			}
			return node, nil
		})

		if isSorted {
			a.Log("distinct optimized for ordered output")
			return plan.NewOrderedDistinct(node.Child), nil
		}
	}

	return node, nil
}

var errInvalidNodeType = errors.NewKind("reorder projection: invalid node of type: %T")

func reorderProjection(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("reorder_projection")
	defer span.Finish()

	if n.Resolved() {
		return n, nil
	}

	a.Log("reorder projection, node of type: %T", n)

	// Then we transform the projection
	return n.TransformUp(func(node sql.Node) (sql.Node, error) {
		project, ok := node.(*plan.Project)
		if !ok {
			return node, nil
		}

		// We must find all columns that may need to be moved inside the
		// projection.
		//var movedColumns = make(map[string]sql.Expression)
		var newColumns = make(map[string]sql.Expression)
		for _, col := range project.Projections {
			alias, ok := col.(*expression.Alias)
			if ok {
				newColumns[alias.Name()] = col
			}
		}

		// And add projection nodes where needed in the child tree.
		var didNeedReorder bool
		child, err := project.Child.TransformUp(func(node sql.Node) (sql.Node, error) {
			var requiredColumns []string
			switch node := node.(type) {
			case *plan.Sort, *plan.Filter:
				for _, expr := range node.(sql.Expressioner).Expressions() {
					expression.Inspect(expr, func(e sql.Expression) bool {
						if e != nil && e.Resolved() {
							return true
						}

						uc, ok := e.(column)
						if ok && uc.Table() == "" {
							if _, ok := newColumns[uc.Name()]; ok {
								requiredColumns = append(requiredColumns, uc.Name())
							}
						}

						return true
					})
				}
			default:
				return node, nil
			}

			didNeedReorder = true

			// Only add the required columns for that node in the projection.
			child := node.Children()[0]
			schema := child.Schema()
			var projections = make([]sql.Expression, 0, len(schema)+len(requiredColumns))
			for i, col := range schema {
				projections = append(projections, expression.NewGetFieldWithTable(
					i, col.Type, col.Source, col.Name, col.Nullable,
				))
			}

			for _, col := range requiredColumns {
				projections = append(projections, newColumns[col])
				delete(newColumns, col)
			}

			child = plan.NewProject(projections, child)
			switch node := node.(type) {
			case *plan.Filter:
				return plan.NewFilter(node.Expression, child), nil
			case *plan.Sort:
				return plan.NewSort(node.SortFields, child), nil
			default:
				return nil, errInvalidNodeType.New(node)
			}
		})

		if err != nil {
			return nil, err
		}

		if !didNeedReorder {
			return project, nil
		}

		child, err = resolveColumns(ctx, a, child)
		if err != nil {
			return nil, err
		}

		childSchema := child.Schema()
		// Finally, replace the columns we moved with GetFields since they
		// have already been projected.
		var projections = make([]sql.Expression, len(project.Projections))
		for i, p := range project.Projections {
			if alias, ok := p.(*expression.Alias); ok {
				var found bool
				for idx, col := range childSchema {
					if col.Name == alias.Name() {
						projections[i] = expression.NewGetField(
							idx, col.Type, col.Name, col.Nullable,
						)
						found = true
						break
					}
				}

				if !found {
					projections[i] = p
				}
			} else {
				projections[i] = p
			}
		}

		return plan.NewProject(projections, child), nil
	})
}

func eraseProjection(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("erase_projection")
	defer span.Finish()

	if !node.Resolved() {
		return node, nil
	}

	a.Log("erase projection, node of type: %T", node)

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		project, ok := node.(*plan.Project)
		if ok && project.Schema().Equals(project.Child.Schema()) {
			a.Log("project erased")
			return project.Child, nil
		}

		return node, nil
	})
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

// indexCatalog sets the catalog in the CreateIndex and DropIndex nodes.
func indexCatalog(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	span, ctx := ctx.Span("index_catalog")
	defer span.Finish()

	switch node := n.(type) {
	case *plan.CreateIndex:
		nc := *node
		nc.Catalog = a.Catalog
		nc.CurrentDatabase = a.CurrentDatabase
		return &nc, nil
	case *plan.DropIndex:
		nc := *node
		nc.Catalog = a.Catalog
		nc.CurrentDatabase = a.CurrentDatabase
		return &nc, nil
	default:
		return n, nil
	}
}

func pushdown(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown")
	defer span.Finish()

	a.Log("pushdown, node of type: %T", n)
	if !n.Resolved() {
		return n, nil
	}

	// don't do pushdown on certain queries
	switch n.(type) {
	case *plan.InsertInto, *plan.CreateIndex:
		return n, nil
	}

	var fieldsByTable = make(map[string][]string)
	var exprsByTable = make(map[string][]sql.Expression)
	type tableField struct {
		table string
		field string
	}
	var tableFields = make(map[tableField]struct{})

	a.Log("finding used columns in node")

	colSpan, _ := ctx.Span("find_pushdown_columns")

	// First step is to find all col exprs and group them by the table they mention.
	// Even if they appear multiple times, only the first one will be used.
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if e, ok := e.(*expression.GetField); ok {
			tf := tableField{e.Table(), e.Name()}
			if _, ok := tableFields[tf]; !ok {
				a.Log("found used column %s.%s", e.Table(), e.Name())
				tableFields[tf] = struct{}{}
				fieldsByTable[e.Table()] = append(fieldsByTable[e.Table()], e.Name())
				exprsByTable[e.Table()] = append(exprsByTable[e.Table()], e)
			}
		}
		return true
	})

	colSpan.Finish()

	a.Log("finding filters in node")

	filterSpan, _ := ctx.Span("find_pushdown_filters")

	// then find all filters, also by table. Note that filters that mention
	// more than one table will not be passed to neither.
	filters := make(filters)
	plan.Inspect(n, func(node sql.Node) bool {
		a.Log("inspecting node of type: %T", node)
		switch node := node.(type) {
		case *plan.Filter:
			fs := exprToTableFilters(node.Expression)
			a.Log("found filters for %d tables %s", len(fs), node.Expression)
			filters.merge(fs)
		}
		return true
	})

	filterSpan.Finish()

	a.Log("transforming nodes with pushdown of filters and projections")

	// Now all nodes can be transformed. Since traversal of the tree is done
	// from inner to outer the filters have to be processed first so they get
	// to the tables.
	var handledFilters []sql.Expression
	var queryIndexes []sql.Index
	node, err := n.TransformUp(func(node sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", node)
		switch node := node.(type) {
		case *plan.Filter:
			if len(handledFilters) == 0 {
				a.Log("no handled filters, leaving filter untouched")
				return node, nil
			}

			unhandled := getUnhandledFilters(
				splitExpression(node.Expression),
				handledFilters,
			)

			if len(unhandled) == 0 {
				a.Log("filter node has no unhandled filters, so it will be removed")
				return node.Child, nil
			}

			a.Log(
				"%d handled filters removed from filter node, filter has now %d filters",
				len(handledFilters),
				len(unhandled),
			)

			return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), nil
		case *plan.PushdownProjectionAndFiltersTable,
			*plan.PushdownProjectionTable,
			*plan.IndexableTable:
			// they also implement the interfaces for pushdown, so we better return
			// or there will be a very nice infinite loop
			return node, nil
		case sql.PushdownProjectionAndFiltersTable:
			cols := exprsByTable[node.Name()]
			tableFilters := filters[node.Name()]
			handled := node.HandledFilters(tableFilters)
			handledFilters = append(handledFilters, handled...)

			schema := node.Schema()
			cols, err := fixFieldIndexesOnExpressions(schema, cols...)
			if err != nil {
				return nil, err
			}

			handled, err = fixFieldIndexesOnExpressions(schema, handled...)
			if err != nil {
				return nil, err
			}

			indexable, ok := node.(*indexable)
			if !ok {
				a.Log(
					"table %q transformed with pushdown of projection and filters, %d filters handled of %d",
					node.Name(),
					len(handled),
					len(tableFilters),
				)

				return plan.NewPushdownProjectionAndFiltersTable(
					cols,
					handled,
					node,
				), nil
			}

			a.Log(
				"table %q transformed with pushdown of projection, filters and index, %d filters handled of %d",
				node.Name(),
				len(handled),
				len(tableFilters),
			)

			queryIndexes = append(queryIndexes, indexable.index.indexes...)
			return plan.NewIndexableTable(
				cols,
				handled,
				indexable.index.lookup,
				indexable.Indexable,
			), nil
		case sql.PushdownProjectionTable:
			cols := fieldsByTable[node.Name()]
			a.Log("table %q transformed with pushdown of projection", node.Name())
			return plan.NewPushdownProjectionTable(cols, node), nil
		}
		return node, nil
	})

	release := func() {
		for _, idx := range queryIndexes {
			a.Catalog.ReleaseIndex(idx)
		}
	}

	if err != nil {
		release()
		return nil, err
	}

	if len(queryIndexes) > 0 {
		return &releaser{node, release}, nil
	}

	return node, nil
}

// fixFieldIndexesOnExpressions executes fixFieldIndexes on a list of exprs.
func fixFieldIndexesOnExpressions(schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, error) {
	var result = make([]sql.Expression, len(expressions))
	for i, e := range expressions {
		var err error
		result[i], err = fixFieldIndexes(schema, e)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// fixFieldIndexes transforms the given expression setting correct indexes
// for GetField expressions according to the schema of the row in the table
// and not the one where the filter came from.
func fixFieldIndexes(schema sql.Schema, exp sql.Expression) (sql.Expression, error) {
	return exp.TransformUp(func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// we need to rewrite the indexes for the table row
			for i, col := range schema {
				if e.Name() == col.Name {
					return expression.NewGetFieldWithTable(
						i,
						e.Type(),
						e.Table(),
						e.Name(),
						e.IsNullable(),
					), nil
				}
			}

			return nil, ErrFieldMissing.New(e.Name())
		}

		return e, nil
	})
}

func assignIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	if !node.Resolved() {
		a.Log("node is not resolved, skipping assigning indexes")
		return node, nil
	}

	a.Log("assigning indexes, node of type: %T", node)

	var indexes map[string]*indexLookup
	// release all unused indexes
	defer func() {
		if indexes == nil {
			return
		}

		for _, i := range indexes {
			for _, index := range i.indexes {
				a.Catalog.ReleaseIndex(index)
			}
		}
	}()

	var err error
	plan.Inspect(node, func(node sql.Node) bool {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return true
		}

		var result map[string]*indexLookup
		result, err = getIndexes(filter.Expression, a)
		if err != nil {
			return false
		}

		if indexes != nil {
			indexes = indexesIntersection(indexes, result)
		} else {
			indexes = result
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		table, ok := node.(sql.Indexable)
		if !ok {
			return node, nil
		}

		// if we assign indexes to already assigned tables there will be
		// an infinite loop
		switch table.(type) {
		case *plan.IndexableTable, *indexable:
			return node, nil
		}

		index, ok := indexes[table.Name()]
		if !ok {
			return node, nil
		}

		delete(indexes, table.Name())

		return &indexable{index, table}, nil
	})
}

func containsColumns(e sql.Expression) bool {
	var result bool
	expression.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.GetField); ok {
			result = true
		}
		return true
	})
	return result
}

var errInvalidInRightEvaluation = errors.NewKind("expecting evaluation of IN expression right hand side to be a tuple, but it is %T")

// indexLookup contains an sql.IndexLookup and all sql.Index that are involved
// in it.
type indexLookup struct {
	lookup  sql.IndexLookup
	indexes []sql.Index
}

func getIndexes(e sql.Expression, a *Analyzer) (map[string]*indexLookup, error) {
	var result = make(map[string]*indexLookup)
	switch e := e.(type) {
	case *expression.Or:
		leftIndexes, err := getIndexes(e.Left, a)
		if err != nil {
			return nil, err
		}

		rightIndexes, err := getIndexes(e.Right, a)
		if err != nil {
			return nil, err
		}

		for table, idx := range leftIndexes {
			if idx2, ok := rightIndexes[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
				idx.lookup = idx.lookup.(sql.SetOperations).Union(idx2.lookup)
				idx.indexes = append(idx.indexes, idx2.indexes...)
			}
			result[table] = idx
		}

		// Put in the result map the indexes for tables we don't have indexes yet.
		// The others were already handled by the previous loop.
		for table, lookup := range rightIndexes {
			if _, ok := result[table]; !ok {
				result[table] = lookup
			}
		}
	case *expression.Equals:
		left, right := e.Left(), e.Right()
		// if the form is SOMETHING = INDEXABLE EXPR, swap it, so it's INDEXABLE EXPR = SOMETING
		if !isEvaluable(right) {
			left, right = right, left
		}

		if !isEvaluable(left) && isEvaluable(right) {
			idx := a.Catalog.IndexByExpression(a.CurrentDatabase, left)
			if idx != nil {
				// release the index if it was not used
				defer func() {
					if _, ok := result[idx.Table()]; !ok {
						a.Catalog.ReleaseIndex(idx)
					}
				}()

				value, err := right.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				lookup, err := idx.Get(value)
				if err != nil {
					return nil, err
				}

				result[idx.Table()] = &indexLookup{
					lookup:  lookup,
					indexes: []sql.Index{idx},
				}
			}
		}
	case *expression.In:
		// Take the index of a SOMETHING IN SOMETHING expression only if:
		// the right branch is evaluable and the indexlookup supports set
		// operations.
		if !isEvaluable(e.Left()) && isEvaluable(e.Right()) {
			idx := a.Catalog.IndexByExpression(a.CurrentDatabase, e.Left())
			if idx != nil {
				// release the index if it was not used
				defer func() {
					if _, ok := result[idx.Table()]; !ok {
						a.Catalog.ReleaseIndex(idx)
					}
				}()

				value, err := e.Right().Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				values, ok := value.([]interface{})
				if !ok {
					return nil, errInvalidInRightEvaluation.New(value)
				}

				lookup, err := idx.Get(values[0])
				if err != nil {
					return nil, err
				}

				for _, v := range values[1:] {
					lookup2, err := idx.Get(v)
					if err != nil {
						return nil, err
					}

					// if one of the indexes cannot be merged, return already
					if !canMergeIndexes(lookup, lookup2) {
						return result, nil
					}

					lookup = lookup.(sql.SetOperations).Union(lookup2)
				}

				result[idx.Table()] = &indexLookup{
					indexes: []sql.Index{idx},
					lookup:  lookup,
				}
			}
		}
	case *expression.And:
		exprs := splitExpression(e)
		used := make(map[sql.Expression]struct{})

		result, err := getMultiColumnIndexes(exprs, a, used)
		if err != nil {
			return nil, err
		}

		for _, e := range exprs {
			if _, ok := used[e]; ok {
				continue
			}

			indexes, err := getIndexes(e, a)
			if err != nil {
				return nil, err
			}

			result = indexesIntersection(result, indexes)
		}

		return result, nil
	}

	return result, nil
}

func indexesIntersection(left, right map[string]*indexLookup) map[string]*indexLookup {
	var result = make(map[string]*indexLookup)

	for table, idx := range left {
		if idx2, ok := right[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
			idx.lookup = idx.lookup.(sql.SetOperations).Intersection(idx2.lookup)
			idx.indexes = append(idx.indexes, idx2.indexes...)
		}
		result[table] = idx
	}

	// Put in the result map the indexes for tables we don't have indexes yet.
	// The others were already handled by the previous loop.
	for table, lookup := range right {
		if _, ok := result[table]; !ok {
			result[table] = lookup
		}
	}

	return result
}

func getMultiColumnIndexes(
	exprs []sql.Expression,
	a *Analyzer,
	used map[sql.Expression]struct{},
) (map[string]*indexLookup, error) {
	result := make(map[string]*indexLookup)
	columnExprs := columnExprsByTable(exprs)
	for table, exps := range columnExprs {
		cols := make([]sql.Expression, len(exps))
		for i, e := range exps {
			cols[i] = e.col
		}

		exprList := a.Catalog.ExpressionsWithIndexes(a.CurrentDatabase, cols...)

		var selected []sql.Expression
		for _, l := range exprList {
			if len(l) > len(selected) {
				selected = l
			}
		}

		if len(selected) > 0 {
			index := a.Catalog.IndexByExpression(a.CurrentDatabase, selected...)
			if index != nil {
				var values = make([]interface{}, len(index.ExpressionHashes()))
				for i, e := range index.ExpressionHashes() {
					col := findColumnByHash(exps, e)
					used[col.expr] = struct{}{}
					val, err := col.val.Eval(sql.NewEmptyContext(), nil)
					if err != nil {
						return nil, err
					}
					values[i] = val
				}
				lookup, err := index.Get(values...)
				if err != nil {
					return nil, err
				}

				result[table] = &indexLookup{lookup, []sql.Index{index}}
			}
		}
	}

	return result, nil
}

type columnExpr struct {
	col  *expression.GetField
	val  sql.Expression
	expr sql.Expression
}

func findColumnByHash(cols []columnExpr, hash sql.ExpressionHash) *columnExpr {
	for _, col := range cols {
		if bytes.Compare(sql.NewExpressionHash(col.col), hash) == 0 {
			return &col
		}
	}
	return nil
}

func columnExprsByTable(exprs []sql.Expression) map[string][]columnExpr {
	var result = make(map[string][]columnExpr)

	for _, expr := range exprs {
		eq, ok := expr.(*expression.Equals)
		if !ok {
			continue
		}

		left, right := eq.Left(), eq.Right()
		if !isEvaluable(right) {
			left, right = right, left
		}

		if !isEvaluable(right) {
			continue
		}

		col, ok := left.(*expression.GetField)
		if !ok {
			continue
		}

		result[col.Table()] = append(result[col.Table()], columnExpr{col, right, expr})
	}

	return result
}

func isColumn(e sql.Expression) bool {
	_, ok := e.(*expression.GetField)
	return ok
}

func isEvaluable(e sql.Expression) bool {
	return !containsColumns(e)
}

func canMergeIndexes(a, b sql.IndexLookup) bool {
	m, ok := a.(sql.Mergeable)
	if !ok {
		return false
	}

	if !m.IsMergeable(b) {
		return false
	}

	_, ok = a.(sql.SetOperations)
	return ok
}

// indexable is a wrapper to hold some information along with the table.
// It's meant to be used by the pushdown rule to finally wrap the Indexable
// table with all it requires.
type indexable struct {
	index *indexLookup
	sql.Indexable
}

func (i *indexable) Children() []sql.Node { return nil }
func (i *indexable) Name() string         { return i.Indexable.Name() }
func (i *indexable) Resolved() bool       { return i.Indexable.Resolved() }
func (i *indexable) RowIter(*sql.Context) (sql.RowIter, error) {
	return nil, fmt.Errorf("indexable is a placeholder node, but RowIter was called")
}
func (i *indexable) Schema() sql.Schema { return i.Indexable.Schema() }
func (i *indexable) String() string     { return i.Indexable.String() }
func (i *indexable) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return fn(i)
}
func (i *indexable) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	return i, nil
}

type releaser struct {
	Child   sql.Node
	Release func()
}

var _ sql.Node = (*releaser)(nil)

func (r *releaser) Resolved() bool {
	return r.Child.Resolved()
}

func (r *releaser) Children() []sql.Node {
	return []sql.Node{r.Child}
}

func (r *releaser) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	iter, err := r.Child.RowIter(ctx)
	if err != nil {
		r.Release()
		return nil, err
	}

	return &releaseIter{child: iter, release: r.Release}, nil
}

func (r *releaser) Schema() sql.Schema {
	return r.Child.Schema()
}

func (r *releaser) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := r.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(&releaser{child, r.Release})
}

func (r *releaser) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := r.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return &releaser{child, r.Release}, nil
}

func (r *releaser) String() string {
	return r.Child.String()
}

func (r *releaser) Equal(n sql.Node) bool {
	if r2, ok := n.(*releaser); ok {
		return reflect.DeepEqual(r.Child, r2.Child)
	}
	return false
}

type releaseIter struct {
	child   sql.RowIter
	release func()
	once    sync.Once
}

func (i *releaseIter) Next() (sql.Row, error) {
	row, err := i.child.Next()
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return row, nil
}

func (i *releaseIter) Close() error {
	i.once.Do(i.release)
	return nil
}
