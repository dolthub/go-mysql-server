package analyzer

import (
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{"resolve_tables", resolveTables},
	{"qualify_columns", qualifyColumns},
	{"resolve_columns", resolveColumns},
	{"resolve_database", resolveDatabase},
	{"resolve_star", resolveStar},
	{"resolve_functions", resolveFunctions},
	{"pushdown", pushdown},
	{"optimize_distinct", optimizeDistinct},
}

var (
	// ErrColumnTableNotFound is returned when the column does not exist in a
	// the table.
	ErrColumnTableNotFound = errors.NewKind("column %q is not present in table %q")
	// ErrAmbiguousColumnName is returned when there is a column reference that
	// is present in more than one table.
	ErrAmbiguousColumnName = errors.NewKind("ambiguous column name %q, it's present in all these tables: %v")
	// ErrTableNotFound is returned when the table is not available from the
	// current scope.
	ErrTableNotFound = errors.NewKind("table not found in scope: %s")
)

func qualifyColumns(a *Analyzer, n sql.Node) (sql.Node, error) {
	tables := make(map[string]sql.Node)
	tableAliases := make(map[string]string)
	colIndex := make(map[string][]string)

	indexCols := func(table string, schema sql.Schema) {
		for _, col := range schema {
			colIndex[col.Name] = append(colIndex[col.Name], table)
		}
	}

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.TableAlias:
			switch t := n.Child.(type) {
			case *plan.Project:
				// it's a subquery, index it but return
				tables[n.Name()] = n.Child
				indexCols(n.Name(), n.Schema())
				return n, nil
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
			col, ok := e.(*expression.UnresolvedColumn)
			if !ok {
				return e, nil
			}

			col = expression.NewUnresolvedQualifiedColumn(col.Table(), col.Name())

			if col.Table() == "" {
				tables := dedupStrings(colIndex[col.Name()])
				switch len(tables) {
				case 0:
					return nil, ErrColumnTableNotFound.New(col.Table(), col.Name())
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
					return nil, ErrTableNotFound.New(col.Table())
				}
			}

			return col, nil
		})
	})
}

func resolveDatabase(a *Analyzer, n sql.Node) (sql.Node, error) {
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

func resolveTables(a *Analyzer, n sql.Node) (sql.Node, error) {
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		rt, err := a.Catalog.Table(a.CurrentDatabase, t.Name)
		if err != nil {
			return nil, err
		}

		return rt, nil
	})
}

func resolveStar(a *Analyzer, n sql.Node) (sql.Node, error) {
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		p, ok := n.(*plan.Project)
		if !ok {
			return n, nil
		}

		if len(p.Expressions) != 1 {
			return n, nil
		}

		if _, ok := p.Expressions[0].(*expression.Star); !ok {
			return n, nil
		}

		var exprs []sql.Expression
		for i, e := range p.Child.Schema() {
			gf := expression.NewGetField(i, e.Type, e.Name, e.Nullable)
			exprs = append(exprs, gf)
		}

		return plan.NewProject(exprs, p.Child), nil
	})
}

type columnInfo struct {
	idx int
	col *sql.Column
}

func resolveColumns(a *Analyzer, n sql.Node) (sql.Node, error) {
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		if len(n.Children()) != 1 {
			return n, nil
		}

		child := n.Children()[0]
		if !child.Resolved() {
			return n, nil
		}

		colMap := make(map[string][]columnInfo)
		for idx, col := range child.Schema() {
			colMap[col.Name] = append(colMap[col.Name], columnInfo{idx, col})
		}

		return n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
			uc, ok := e.(*expression.UnresolvedColumn)
			if !ok {
				return e, nil
			}

			columnsInfo, ok := colMap[uc.Name()]
			if !ok {
				return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
			}

			var ci columnInfo
			var found bool
			for _, c := range columnsInfo {
				if c.col.Source == uc.Table() {
					ci = c
					found = true
					break
				}
			}

			if !found {
				return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
			}

			return expression.NewGetFieldWithTable(
				ci.idx,
				ci.col.Type,
				ci.col.Source,
				ci.col.Name,
				ci.col.Nullable,
			), nil
		})
	})
}

func resolveFunctions(a *Analyzer, n sql.Node) (sql.Node, error) {
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		return n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
			uf, ok := e.(*expression.UnresolvedFunction)
			if !ok {
				return e, nil
			}

			n := uf.Name()
			f, err := a.Catalog.Function(n)
			if err != nil {
				return nil, err
			}

			rf, err := f.Call(uf.Children...)
			if err != nil {
				return nil, err
			}

			return rf, nil
		})
	})
}

func optimizeDistinct(a *Analyzer, node sql.Node) (sql.Node, error) {
	if node, ok := node.(*plan.Distinct); ok {
		var isSorted bool
		_, _ = node.TransformUp(func(node sql.Node) (sql.Node, error) {
			if _, ok := node.(*plan.Sort); ok {
				isSorted = true
			}
			return node, nil
		})

		if isSorted {
			return plan.NewOrderedDistinct(node.Child), nil
		}
	}

	return node, nil
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

func pushdown(a *Analyzer, n sql.Node) (sql.Node, error) {
	var fieldsByTable = make(map[string][]string)
	var exprsByTable = make(map[string][]sql.Expression)
	type tableField struct {
		table string
		field string
	}
	var tableFields = make(map[tableField]struct{})

	// First step is to find all col exprs and group them by the table they mention.
	// Even if they appear multiple times, only the first one will be used.
	_, _ = n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
		if e, ok := e.(*expression.GetField); ok {
			tf := tableField{e.Table(), e.Name()}
			if _, ok := tableFields[tf]; !ok {
				tableFields[tf] = struct{}{}
				fieldsByTable[e.Table()] = append(fieldsByTable[e.Table()], e.Name())
				exprsByTable[e.Table()] = append(exprsByTable[e.Table()], e)
			}
		}
		return e, nil
	})

	// then find all filters, also by table. Note that filters that mention
	// more than one table will not be passed to neither.
	filters := make(filters)
	node, err := n.TransformUp(func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			filters.merge(exprToTableFilters(node.Expression))
		case *plan.TableAlias:
			// handle subqueries
			if _, ok := node.Child.(*plan.Project); ok {
				subquery, err := pushdown(a, node.Child)
				if err != nil {
					return nil, err
				}
				return plan.NewTableAlias(node.Name(), subquery), nil
			}
		}

		return node, nil
	})

	if err != nil {
		return nil, err
	}

	// Now all nodes can be transformed. Since traversal of the tree is done
	// from inner to outer the filters have to be processed first so they get
	// to the tables.
	var handledFilters []sql.Expression
	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			unhandled := getUnhandledFilters(
				splitExpression(node.Expression),
				handledFilters,
			)
			if len(unhandled) == 0 {
				return node.Child, nil
			}

			return plan.NewFilter(filtersToExpression(unhandled), node.Child), nil
		case sql.PushdownProjectionAndFiltersTable:
			cols := exprsByTable[node.Name()]
			tableFilters := filters[node.Name()]
			handled := node.HandledFilters(tableFilters)
			handledFilters = append(handledFilters, handled...)

			return plan.NewPushdownProjectionAndFiltersTable(
				cols,
				handled,
				node,
			), nil
		case sql.PushdownProjectionTable:
			cols := fieldsByTable[node.Name()]
			return plan.NewPushdownProjectionTable(cols, node), nil
		}
		return node, nil
	})
}
