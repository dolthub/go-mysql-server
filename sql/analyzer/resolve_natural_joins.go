package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_natural_joins")
	defer span.Finish()

	if n.Resolved() {
		return n, nil
	}

	var transformed sql.Node
	var colsToUnresolve = map[string][]string{}
	a.Log("resolving natural joins, node of type %T", n)
	node, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
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

		transformed = plan.NewProject(
			projections,
			plan.NewInnerJoin(
				join.Left,
				join.Right,
				expression.JoinAnd(conditions...),
			),
		)

		return transformed, nil
	})

	if err != nil || transformed == nil {
		return node, err
	}

	var nodesToUnresolve []sql.Node
	plan.Inspect(node, func(n sql.Node) bool {
		if n == transformed || n == nil {
			return false
		}

		nodesToUnresolve = append(nodesToUnresolve, n)
		return true
	})

	if len(nodesToUnresolve) == 0 {
		return node, err
	}

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		var found bool
		for _, nu := range nodesToUnresolve {
			if reflect.DeepEqual(nu, node) {
				found = true
				break
			}
		}

		if !found {
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

			if table != wrongSource {
				return e, nil
			}

			return expression.NewUnresolvedQualifiedColumn(
				correctSource,
				col,
			), nil
		})
	})
}
