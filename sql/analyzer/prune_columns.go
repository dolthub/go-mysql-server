package analyzer

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

type usedColumns map[string]map[string]struct{}

func pruneColumns(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	a.Log("pruning columns, node of type %T", n)
	if !n.Resolved() {
		return n, nil
	}

	// Skip pruning columns for insert statements. For inserts involving a select (INSERT INTO table1 SELECT a,b FROM
	// table2), all columns from the select are used for the insert, and error checking for schema compatibility
	// happens at execution time. Otherwise the logic below will convert a Project to a ResolvedTable for the selected
	// table, which can alter the column order of the select.
	if _, ok := n.(*plan.InsertInto); ok {
		return n, nil
	}

	if describe, ok := n.(*plan.DescribeQuery); ok {
		pruned, err := pruneColumns(ctx, a, describe.Child)
		if err != nil {
			return nil, err
		}

		return plan.NewDescribeQuery(describe.Format, pruned), nil
	}

	columns := findRequiredColumns(n)

	findUsedColumns(columns, n)

	n, err := pruneUnusedColumns(n, columns)
	if err != nil {
		return nil, err
	}

	n, err = pruneSubqueries(ctx, a, n, columns)
	if err != nil {
		return nil, err
	}

	return fixRemainingFieldsIndexes(n)
}

func findRequiredColumns(n sql.Node) usedColumns {
	columns := make(usedColumns)

	// All the columns required for the output of the query must be mark as
	// used, otherwise the schema would change.
	for _, col := range n.Schema() {
		if _, ok := columns[col.Source]; !ok {
			columns[col.Source] = make(map[string]struct{})
		}
		columns[col.Source][col.Name] = struct{}{}
	}

	return columns
}

func pruneSubqueryColumns(
	ctx *sql.Context,
	a *Analyzer,
	n *plan.SubqueryAlias,
	parentColumns usedColumns,
) (sql.Node, error) {
	a.Log("pruning columns of subquery with alias %q", n.Name())

	columns := make(usedColumns)

	// The columns coming from the parent have the subquery alias name as the
	// source. We need to find the real table in order to prune the subquery
	// correctly.
	tableByCol := make(map[string]string)
	for _, col := range n.Child.Schema() {
		tableByCol[col.Name] = col.Source
	}

	for col := range parentColumns[n.Name()] {
		table, ok := tableByCol[col]
		if !ok {
			// This should never happen, but better be safe than sorry.
			return nil, fmt.Errorf("this is likely a bug: missing projected column %q on subquery %q", col, n.Name())
		}

		if _, ok := columns[table]; !ok {
			columns[table] = make(map[string]struct{})
		}

		columns[table][col] = struct{}{}
	}

	findUsedColumns(columns, n.Child)

	node, err := pruneUnusedColumns(n.Child, columns)
	if err != nil {
		return nil, err
	}

	node, err = pruneSubqueries(ctx, a, node, columns)
	if err != nil {
		return nil, err
	}

	// There is no need to fix the field indexes after pruning here
	// because the main query will take care of fixing the indexes of all the
	// nodes in the tree.

	return plan.NewSubqueryAlias(n.Name(), node), nil
}

func findUsedColumns(columns usedColumns, n sql.Node) {
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Project:
			addUsedProjectColumns(columns, n.Projections)
			return true
		case *plan.GroupBy:
			addUsedProjectColumns(columns, n.Aggregate)
			addUsedColumns(columns, n.Grouping)
			return true
		case *plan.SubqueryAlias:
			return false
		}

		exp, ok := n.(sql.Expressioner)
		if ok {
			addUsedColumns(columns, exp.Expressions())
		}

		return true
	})
}

func pruneSubqueries(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	parentColumns usedColumns,
) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		subq, ok := n.(*plan.SubqueryAlias)
		if !ok {
			return n, nil
		}

		return pruneSubqueryColumns(ctx, a, subq, parentColumns)
	})
}

func pruneUnusedColumns(n sql.Node, columns usedColumns) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Project:
			return pruneProject(n, columns), nil
		case *plan.GroupBy:
			return pruneGroupBy(n, columns), nil
		default:
			return n, nil
		}
	})
}

func fixRemainingFieldsIndexes(n sql.Node) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			child, err := fixRemainingFieldsIndexes(n.Child)
			if err != nil {
				return nil, err
			}

			return plan.NewSubqueryAlias(n.Name(), child), nil
		default:
			if _, ok := n.(sql.Expressioner); !ok {
				return n, nil
			}

			var schema sql.Schema
			for _, c := range n.Children() {
				schema = append(schema, c.Schema()...)
			}

			if len(schema) == 0 {
				return n, nil
			}

			indexes := make(map[tableCol]int)
			for i, col := range schema {
				indexes[tableCol{col.Source, col.Name}] = i
			}

			return plan.TransformExpressions(n, func(e sql.Expression) (sql.Expression, error) {
				gf, ok := e.(*expression.GetField)
				if !ok {
					return e, nil
				}

				idx, ok := indexes[tableCol{gf.Table(), gf.Name()}]
				if !ok {
					return nil, ErrColumnTableNotFound.New(gf.Table(), gf.Name())
				}

				if idx == gf.Index() {
					return gf, nil
				}

				ngf := *gf
				return ngf.WithIndex(idx), nil
			})
		}
	})
}

func addUsedProjectColumns(
	columns usedColumns,
	projection []sql.Expression,
) {
	var candidates []sql.Expression
	for _, e := range projection {
		// Only check for expressions that are not directly a GetField. This
		// is because in a projection we only care about those that were used
		// to compute new columns, such as aliases and so on. The fields that
		// are just passed up in the tree will already be in some other part
		// if they are really used.
		if _, ok := e.(*expression.GetField); !ok {
			candidates = append(candidates, e)
		}
	}

	addUsedColumns(columns, candidates)
}

func addUsedColumns(columns usedColumns, exprs []sql.Expression) {
	for _, e := range exprs {
		sql.Inspect(e, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if _, ok := columns[gf.Table()]; !ok {
					columns[gf.Table()] = make(map[string]struct{})
				}
				columns[gf.Table()][gf.Name()] = struct{}{}
			}
			return true
		})
	}
}

func pruneProject(n *plan.Project, columns usedColumns) sql.Node {
	var remaining []sql.Expression
	for _, e := range n.Projections {
		if !shouldPruneExpr(e, columns) {
			remaining = append(remaining, e)
		}
	}

	if len(remaining) == 0 {
		return n.Child
	}

	return plan.NewProject(remaining, n.Child)
}

func pruneGroupBy(n *plan.GroupBy, columns usedColumns) sql.Node {
	var remaining []sql.Expression
	for _, e := range n.Aggregate {
		if !shouldPruneExpr(e, columns) {
			remaining = append(remaining, e)
		}
	}

	if len(remaining) == 0 {
		return n.Child
	}

	return plan.NewGroupBy(remaining, n.Grouping, n.Child)
}

func shouldPruneExpr(e sql.Expression, cols usedColumns) bool {
	gf, ok := e.(*expression.GetField)
	if !ok {
		return false
	}

	if gf.Table() == "" {
		return false
	}

	if c, ok := cols[gf.Table()]; ok {
		if _, ok := c[gf.Name()]; ok {
			return false
		}
	}

	return true
}
