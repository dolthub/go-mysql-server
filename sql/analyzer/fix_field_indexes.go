package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

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
	return expression.TransformUp(exp, func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// we need to rewrite the indexes for the table row
			for i, col := range schema {
				if e.Name() == col.Name && e.Table() == col.Source {
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

// Transforms the expressions in the Node given, fixing the field indexes.
func fixFieldIndexesForExpressions(node sql.Node) (sql.Node, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, nil
	}

	n, err := plan.TransformExpressions(node, func(e sql.Expression) (sql.Expression, error) {
		for _, schema := range schemas {
			fixed, err := fixFieldIndexes(schema, e)
			if err == nil {
				return fixed, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, err
		}

		return e, nil
	})

	if err != nil {
		return nil, err
	}

	switch j := n.(type) {
	case *plan.InnerJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewInnerJoin(j.Left, j.Right, cond)
	case *plan.RightJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewRightJoin(j.Left, j.Right, cond)
	case *plan.LeftJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewLeftJoin(j.Left, j.Right, cond)
	}

	return n, nil
}
