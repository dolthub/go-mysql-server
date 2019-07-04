package analyzer

import (
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_natural_joins")
	defer span.Finish()

	var replacements = make(map[tableCol]tableCol)
	var tableAliases = make(map[string]string)

	return plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch n := node.(type) {
		case *plan.TableAlias:
			alias := n.Name()
			table := n.Child.(*plan.ResolvedTable).Name()
			tableAliases[strings.ToLower(alias)] = table
			return n, nil
		case *plan.NaturalJoin:
			return resolveNaturalJoin(n, replacements)
		case sql.Expressioner:
			return replaceExpressions(node, replacements, tableAliases)
		default:
			return n, nil
		}
	})
}

func resolveNaturalJoin(
	n *plan.NaturalJoin,
	replacements map[tableCol]tableCol,
) (sql.Node, error) {
	// Both sides of the natural join need to be resolved in order to resolve
	// the natural join itself.
	if !n.Left.Resolved() || !n.Right.Resolved() {
		return n, nil
	}

	leftSchema := n.Left.Schema()
	rightSchema := n.Right.Schema()

	var conditions, common, left, right []sql.Expression
	for i, lcol := range leftSchema {
		leftCol := expression.NewGetFieldWithTable(
			i,
			lcol.Type,
			lcol.Source,
			lcol.Name,
			lcol.Nullable,
		)
		if idx, rcol := findCol(rightSchema, lcol.Name); rcol != nil {
			common = append(common, leftCol)
			replacements[tableCol{strings.ToLower(rcol.Source), strings.ToLower(rcol.Name)}] = tableCol{
				strings.ToLower(lcol.Source), strings.ToLower(lcol.Name),
			}

			conditions = append(
				conditions,
				expression.NewEquals(
					leftCol,
					expression.NewGetFieldWithTable(
						len(leftSchema)+idx,
						rcol.Type,
						rcol.Source,
						rcol.Name,
						rcol.Nullable,
					),
				),
			)
		} else {
			left = append(left, leftCol)
		}
	}

	if len(conditions) == 0 {
		return plan.NewCrossJoin(n.Left, n.Right), nil
	}

	for i, col := range rightSchema {
		source := strings.ToLower(col.Source)
		name := strings.ToLower(col.Name)
		if _, ok := replacements[tableCol{source, name}]; !ok {
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

	return plan.NewProject(
		append(append(common, left...), right...),
		plan.NewInnerJoin(n.Left, n.Right, expression.JoinAnd(conditions...)),
	), nil
}

func findCol(s sql.Schema, name string) (int, *sql.Column) {
	for i, c := range s {
		if strings.ToLower(c.Name) == strings.ToLower(name) {
			return i, c
		}
	}
	return -1, nil
}

func replaceExpressions(
	n sql.Node,
	replacements map[tableCol]tableCol,
	tableAliases map[string]string,
) (sql.Node, error) {
	return plan.TransformExpressions(n, func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.GetField, *expression.UnresolvedColumn:
			var tableName = e.(sql.Tableable).Table()
			if t, ok := tableAliases[strings.ToLower(tableName)]; ok {
				tableName = t
			}

			name := e.(sql.Nameable).Name()
			if col, ok := replacements[tableCol{strings.ToLower(tableName), strings.ToLower(name)}]; ok {
				return expression.NewUnresolvedQualifiedColumn(col.table, col.col), nil
			}
		}
		return e, nil
	})
}
