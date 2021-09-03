package aggregation

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func duplicateExpression(ctx *sql.Context, expr sql.Expression) (sql.Expression, error) {
	return expression.TransformUp(ctx, expr, func(e sql.Expression) (sql.Expression, error) {
		return e, nil
	})
}
