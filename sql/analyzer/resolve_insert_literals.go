package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	errors "gopkg.in/src-d/go-errors.v1"
)

var errWrongNumberOfValues = errors.NewKind("the number of values to insert differ from the expected columns")

func convertIntegerLiteralsInsert(ctx *sql.Context, analyzer *Analyzer, originalNode sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_insert_literals")
	defer span.Finish()

	return plan.TransformUp(originalNode, func(node sql.Node) (sql.Node, error) {
		if node, ok := node.(*plan.InsertInto); ok {
			resolvedTable, ok := node.BinaryNode.Left.(*plan.ResolvedTable)
			if !ok {
				return node, nil
			}

			values := node.BinaryNode.Right.(*plan.Values)
			if !ok {
				return node, nil
			}

			analyzer.Log("Transforming integer literals in INSERT node")

			schema := resolvedTable.Table.Schema()

			// If the InsertInto node does not have any explicit columns,
			// we assume the values are in the same order as in the table schema
			if len(node.Columns) == 0 {
				node.Columns = make([]string, len(schema))
				for i, column := range schema {
					node.Columns[i] = column.Name
				}
			}

			// Check that all tuples contain as many values as needed
			numColumns := len(node.Columns)
			for _, tuple := range values.ExpressionTuples {
				if len(tuple) != numColumns {
					return nil, errWrongNumberOfValues.New()
				}
			}

			// Get the columns that should be converted: only those that are in
			// node.Columns and whose corresponding type in the schema is an integer
			columnsToConvert := make(map[int]sql.Type)
			for _, schemaColumn := range schema {
				colType := schemaColumn.Type
				if sql.IsInteger(colType) {
					for nodeIdx, insertColumn := range node.Columns {
						if schemaColumn.Name == insertColumn {
							columnsToConvert[nodeIdx] = colType
						}
					}
				}
			}

			// Replace the values in the node with the converted ones
			for _, valuesTuple := range values.ExpressionTuples {
				for colIdx, newType := range columnsToConvert {
					oldValue := valuesTuple[colIdx].(*expression.Literal).Value()
					// Do not convert nil values, Convert() may make them zero
					if oldValue != nil {
						newValue, err := newType.Convert(oldValue)
						if err != nil {
							return nil, err
						}
						valuesTuple[colIdx] = expression.NewLiteral(newValue, newType)
					}
				}
			}
		}
		return node, nil
	})
}
