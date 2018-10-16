package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

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
				if c, ok := newColumns[col]; ok {
					projections = append(projections, c)
					delete(newColumns, col)
				}
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

func moveJoinConditionsToFilter(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		a.Log("node is not resolved, skip moving join conditions to filter")
		return n, nil
	}

	a.Log("moving join conditions to filter, node of type: %T", n)

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		join, ok := n.(*plan.InnerJoin)
		if !ok {
			return n, nil
		}

		leftSources := nodeSources(join.Left)
		rightSources := nodeSources(join.Right)
		var leftFilters, rightFilters, condFilters []sql.Expression
		for _, e := range splitExpression(join.Cond) {
			sources := expressionSources(e)

			canMoveLeft := containsSources(leftSources, sources)
			if canMoveLeft {
				leftFilters = append(leftFilters, e)
			}

			canMoveRight := containsSources(rightSources, sources)
			if canMoveRight {
				rightFilters = append(rightFilters, e)
			}

			if !canMoveLeft && !canMoveRight {
				condFilters = append(condFilters, e)
			}
		}

		var left, right sql.Node = join.Left, join.Right
		if len(leftFilters) > 0 {
			leftFilters, err := fixFieldIndexes(left.Schema(), expression.JoinAnd(leftFilters...))
			if err != nil {
				return nil, err
			}

			left = plan.NewFilter(leftFilters, left)
		}

		if len(rightFilters) > 0 {
			rightFilters, err := fixFieldIndexes(right.Schema(), expression.JoinAnd(rightFilters...))
			if err != nil {
				return nil, err
			}

			right = plan.NewFilter(rightFilters, right)
		}

		if len(condFilters) > 0 {
			return plan.NewInnerJoin(
				left, right,
				expression.JoinAnd(condFilters...),
			), nil
		}

		// if there are no cond filters left we can just convert it to a cross join
		return plan.NewCrossJoin(left, right), nil
	})
}

// containsSources checks that all `needle` sources are contained inside `haystack`.
func containsSources(haystack, needle []string) bool {
	for _, s := range needle {
		var found bool
		for _, s2 := range haystack {
			if s2 == s {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func nodeSources(node sql.Node) []string {
	var sources = make(map[string]struct{})
	var result []string

	for _, col := range node.Schema() {
		if _, ok := sources[col.Source]; !ok {
			sources[col.Source] = struct{}{}
			result = append(result, col.Source)
		}
	}

	return result
}

func expressionSources(expr sql.Expression) []string {
	var sources = make(map[string]struct{})
	var result []string

	expression.Inspect(expr, func(expr sql.Expression) bool {
		f, ok := expr.(*expression.GetField)
		if ok {
			if _, ok := sources[f.Table()]; !ok {
				sources[f.Table()] = struct{}{}
				result = append(result, f.Table())
			}
		}

		return true
	})

	return result
}

func evalFilter(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	if !node.Resolved() {
		return node, nil
	}

	a.Log("evaluating filters, node of type: %T", node)

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, nil
		}

		e, err := filter.Expression.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			switch e := e.(type) {
			case *expression.Or:
				if isTrue(e.Left) {
					return e.Left, nil
				}

				if isTrue(e.Right) {
					return e.Right, nil
				}

				if isFalse(e.Left) {
					return e.Right, nil
				}

				if isFalse(e.Right) {
					return e.Left, nil
				}

				return e, nil
			case *expression.And:
				if isFalse(e.Left) {
					return e.Left, nil
				}

				if isFalse(e.Right) {
					return e.Right, nil
				}

				if isTrue(e.Left) {
					return e.Right, nil
				}

				if isTrue(e.Right) {
					return e.Left, nil
				}

				return e, nil
			default:
				if !isEvaluable(e) {
					return e, nil
				}

				if _, ok := e.(*expression.Literal); ok {
					return e, nil
				}

				val, err := e.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				val, err = sql.Boolean.Convert(val)
				if err != nil {
					// don't make it fail because of this, just return the
					// original expression
					return e, nil
				}

				return expression.NewLiteral(val.(bool), sql.Boolean), nil
			}
		})
		if err != nil {
			return nil, err
		}

		if isFalse(e) {
			return plan.EmptyTable, nil
		}

		if isTrue(e) {
			return filter.Child, nil
		}

		return plan.NewFilter(e, filter.Child), nil
	})
}

func isFalse(e sql.Expression) bool {
	lit, ok := e.(*expression.Literal)
	return ok &&
		lit.Type() == sql.Boolean &&
		!lit.Value().(bool)
}

func isTrue(e sql.Expression) bool {
	lit, ok := e.(*expression.Literal)
	return ok &&
		lit.Type() == sql.Boolean &&
		lit.Value().(bool)
}
