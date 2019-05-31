package analyzer

import (
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func resolveOrderBy(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_orderby")
	defer span.Finish()

	a.Log("resolving order bys, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, nil
		}

		if !sort.Child.Resolved() {
			a.Log("child of type %T is not resolved yet, skipping", sort.Child)
			return n, nil
		}

		childNewCols := columnsDefinedInNode(sort.Child)
		var schemaCols []string
		for _, col := range sort.Child.Schema() {
			schemaCols = append(schemaCols, strings.ToLower(col.Name))
		}

		var colsFromChild []string
		var missingCols []string
		for _, f := range sort.SortFields {
			ns := findExprNameables(f.Column)

			for _, n := range ns {
				name := strings.ToLower(n.Name())
				if stringContains(childNewCols, name) {
					colsFromChild = append(colsFromChild, n.Name())
				} else if !stringContains(schemaCols, name) {
					missingCols = append(missingCols, n.Name())
				}
			}
		}

		// If all the columns required by the order by are available, do nothing about it.
		if len(missingCols) == 0 {
			a.Log("no missing columns, skipping")
			return n, nil
		}

		// If there are no columns required by the order by available, then move the order by
		// below its child.
		if len(colsFromChild) == 0 && len(missingCols) > 0 {
			a.Log("pushing down sort, missing columns: %s", strings.Join(missingCols, ", "))
			return pushSortDown(sort)
		}

		a.Log("fixing sort dependencies, missing columns: %s", strings.Join(missingCols, ", "))

		// If there are some columns required by the order by on the child but some are missing
		// we have to do some more complex logic and split the projection in two.
		return fixSortDependencies(sort, missingCols)
	})
}

// fixSortDependencies replaces the sort node by a node with the child projection
// followed by the sort, an intermediate projection or group by with all the missing
// columns required for the sort and then the child of the child projection or group by.
func fixSortDependencies(sort *plan.Sort, missingCols []string) (sql.Node, error) {
	var expressions []sql.Expression
	switch child := sort.Child.(type) {
	case *plan.Project:
		expressions = child.Projections
	case *plan.GroupBy:
		expressions = child.Aggregate
	default:
		return nil, errSortPushdown.New(child)
	}

	var newExpressions = append([]sql.Expression{}, expressions...)
	for _, col := range missingCols {
		newExpressions = append(newExpressions, expression.NewUnresolvedColumn(col))
	}

	for i, e := range expressions {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}
		expressions[i] = expression.NewGetFieldWithTable(
			i, e.Type(), table, name, e.IsNullable(),
		)
	}

	switch child := sort.Child.(type) {
	case *plan.Project:
		return plan.NewProject(
			expressions,
			plan.NewSort(
				sort.SortFields,
				plan.NewProject(newExpressions, child.Child),
			),
		), nil
	case *plan.GroupBy:
		return plan.NewProject(
			expressions,
			plan.NewSort(
				sort.SortFields,
				plan.NewGroupBy(newExpressions, child.Grouping, child.Child),
			),
		), nil
	default:
		return nil, errSortPushdown.New(child)
	}
}

// columnsDefinedInNode returns the columns that were defined in this node,
// which, by definition, can only be plan.Project or plan.GroupBy.
func columnsDefinedInNode(n sql.Node) []string {
	var exprs []sql.Expression
	switch n := n.(type) {
	case *plan.Project:
		exprs = n.Projections
	case *plan.GroupBy:
		exprs = n.Aggregate
	}

	var cols []string
	for _, e := range exprs {
		alias, ok := e.(*expression.Alias)
		if ok {
			cols = append(cols, strings.ToLower(alias.Name()))
		}
	}

	return cols
}

var errSortPushdown = errors.NewKind("unable to push plan.Sort node below %T")

func pushSortDown(sort *plan.Sort) (sql.Node, error) {
	switch child := sort.Child.(type) {
	case *plan.Project:
		return plan.NewProject(
			child.Projections,
			plan.NewSort(sort.SortFields, child.Child),
		), nil
	case *plan.GroupBy:
		return plan.NewGroupBy(
			child.Aggregate,
			child.Grouping,
			plan.NewSort(sort.SortFields, child.Child),
		), nil
	case *plan.ResolvedTable:
		return child, nil
	default:
		// Can't do anything here, there should be either a project or a groupby
		// below an order by.
		return nil, errSortPushdown.New(child)
	}
}

func resolveOrderByLiterals(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	a.Log("resolve order by literals")

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, nil
		}

		// wait for the child to be resolved
		if !sort.Child.Resolved() {
			return n, nil
		}

		schema := sort.Child.Schema()
		var (
			fields     = make([]plan.SortField, len(sort.SortFields))
			schemaCols = make([]string, len(schema))
		)
		for i, col := range sort.Child.Schema() {
			schemaCols[i] = col.Name
		}
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
				if idx >= len(schema) || idx < 0 {
					return nil, ErrOrderByColumnIndex.New(idx + 1)
				}

				fields[i] = plan.SortField{
					Column:       expression.NewUnresolvedColumn(schemaCols[idx]),
					Order:        f.Order,
					NullOrdering: f.NullOrdering,
				}

				a.Log("replaced order by column %d with %s", idx+1, schemaCols[idx])
			} else {
				if agg, ok := f.Column.(sql.Aggregation); ok {
					name := agg.String()
					if nameable, ok := f.Column.(sql.Nameable); ok {
						name = nameable.Name()
					}

					fields[i] = plan.SortField{
						Column:       expression.NewUnresolvedColumn(name),
						Order:        f.Order,
						NullOrdering: f.NullOrdering,
					}
				} else {
					fields[i] = f
				}
			}
		}

		return plan.NewSort(fields, sort.Child), nil
	})
}

func findExprNameables(e sql.Expression) []sql.Nameable {
	var result []sql.Nameable
	expression.Inspect(e, func(e sql.Expression) bool {
		n, ok := e.(sql.Nameable)
		if ok {
			result = append(result, n)
			return false
		}
		return true
	})
	return result
}
