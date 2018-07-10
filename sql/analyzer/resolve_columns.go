package analyzer

import (
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// deferredColumn is a wrapper on UnresolvedColumn used only to defer the
// resolution of the column because it may require some work done by
// other analyzer phases.
type deferredColumn struct {
	*expression.UnresolvedColumn
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

		var aliasMap = map[string]struct{}{}
		var exists = struct{}{}
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

			columns, ok := colMap[uc.Name()]
			if !ok {
				switch uc := uc.(type) {
				case *expression.UnresolvedColumn:
					a.Log("evaluation of column %q was deferred", uc.Name())
					return &deferredColumn{uc}, nil
				default:
					if uc.Table() != "" {
						return nil, ErrColumnTableNotFound.New(uc.Table(), uc.Name())
					}

					if _, ok := aliasMap[uc.Name()]; ok {
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
