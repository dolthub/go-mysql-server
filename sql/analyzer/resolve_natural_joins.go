package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type transformedJoin struct {
	node     sql.Node
	condCols map[string][]string
}

func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_natural_joins")
	defer span.Finish()

	if n.Resolved() {
		return n, nil
	}

	var transformed []*transformedJoin
	var aliasTables = map[string][]string{}
	a.Log("resolving natural joins, node of type %T", n)
	node, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)

		if alias, ok := n.(*plan.TableAlias); ok {
			table := alias.Child.(sql.Table).Name()
			aliasTables[alias.Name()] = append(aliasTables[alias.Name()], table)
			return n, nil
		}

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
		var colsToUnresolve = map[string][]string{}

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
					colsToUnresolve[lcol.Name] = []string{lcol.Source, rcol.Source}
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

		tj := &transformedJoin{
			node: plan.NewProject(
				projections,
				plan.NewInnerJoin(
					join.Left,
					join.Right,
					expression.JoinAnd(conditions...),
				),
			),
			condCols: colsToUnresolve,
		}

		transformed = append(transformed, tj)

		return tj.node, nil
	})

	if err != nil || len(transformed) == 0 {
		return node, err
	}

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		above, colsToUnresolve := isOverTranformedNode(node, transformed)
		if !above {
			return node, nil
		}

		expressioner, ok := node.(sql.Expressioner)
		if !ok {
			return node, nil
		}

		return expressioner.TransformExpressions(func(e sql.Expression) (sql.Expression, error) {
			var col, table string
			switch e := e.(type) {
			case *expression.GetField:
				col, table = e.Name(), e.Table()
			case *expression.UnresolvedColumn:
				col, table = e.Name(), e.Table()
			default:
				return e, nil
			}

			sources, ok := colsToUnresolve[col]
			if !ok {
				return e, nil
			}

			correctSource := sources[0]
			wrongSource := sources[1]

			if !mustUnresolve(aliasTables, table, wrongSource) {
				return e, nil
			}

			return expression.NewUnresolvedQualifiedColumn(
				correctSource,
				col,
			), nil
		})
	})
}

func isOverTranformedNode(node sql.Node, transformed []*transformedJoin) (over bool, colsToUnresolve map[string][]string) {
	plan.Inspect(node, func(n sql.Node) bool {
		if is, cols := isTransformedNode(n, transformed); is {
			if n != node {
				over, colsToUnresolve = is, cols
			}
		}

		return true
	})

	return
}

func isTransformedNode(node sql.Node, transformed []*transformedJoin) (is bool, colsToUnresolve map[string][]string) {
	project, ok := node.(*plan.Project)
	if !ok {
		return
	}

	join, ok := project.Child.(*plan.InnerJoin)
	if !ok {
		return
	}

	for _, t := range transformed {
		tproject, ok := t.node.(*plan.Project)
		if !ok {
			return
		}

		tjoin, ok := tproject.Child.(*plan.InnerJoin)
		if !ok {
			return
		}

		if reflect.DeepEqual(project.Projections, tproject.Projections) &&
			reflect.DeepEqual(join.Cond, tjoin.Cond) {
			is = true
			colsToUnresolve = t.condCols
		}
	}

	return
}

func mustUnresolve(aliasTable map[string][]string, table, wrongSource string) bool {
	return table == wrongSource || isAliasFor(aliasTable, table, wrongSource)
}

func isAliasFor(aliasTable map[string][]string, table, wrongSource string) bool {
	tables, ok := aliasTable[table]
	if !ok {
		return false
	}

	for _, t := range tables {
		if t == wrongSource {
			return true
		}
	}

	return false
}
