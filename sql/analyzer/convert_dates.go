package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
)

// convertDates wraps all expressions of date and datetime type with converts
// to ensure the date range is validated.
func convertDates(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	return n.TransformExpressionsUp(func(e sql.Expression) (sql.Expression, error) {
		// No need to wrap expressions that already validate times, such as
		// convert, date_add, etc and those expressions whose Type method
		// cannot be called because they are placeholders.
		switch e.(type) {
		case *expression.Convert,
			*expression.Arithmetic,
			*function.DateAdd,
			*function.DateSub,
			*expression.Star,
			*expression.DefaultColumn,
			*expression.Alias:
			return e, nil
		default:
			switch e.Type() {
			case sql.Date:
				return expression.NewConvert(e, expression.ConvertToDate), nil
			case sql.Timestamp:
				return expression.NewConvert(e, expression.ConvertToDatetime), nil
			default:
				return e, nil
			}
		}
	})
}
