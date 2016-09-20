package parse

import (
	"strconv"
	"strings"

	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/expression"
)

func assembleExpression(s *tokenStack) (sql.Expression, error) {
	tk := s.pop()

	switch tk.Type {
	case OpToken:
		op := strings.ToLower(tk.Value)
		var left, right sql.Expression
		var err error
		right, err = assembleExpression(s)
		if err != nil {
			return nil, err
		}

		if op != "not" {
			left, err = assembleExpression(s)
			if err != nil {
				return nil, err
			}
		}

		switch tk.Value {
		case "not":
			return expression.NewNot(right), nil
		case "=":
			return expression.NewEquals(left, right), nil
		}
	case IdentifierToken:
		if kwMatches(tk.Value, "true") {
			return expression.NewLiteral(true, sql.Boolean), nil
		}

		if kwMatches(tk.Value, "false") {
			return expression.NewLiteral(false, sql.Boolean), nil
		}

		return expression.NewUnresolvedColumn(tk.Value), nil
	case StringToken:
		// TODO: Parse timestamp
		return expression.NewLiteral(
			strings.Trim(tk.Value, `"'`),
			sql.String,
		), nil
	case IntToken:
		// error is avoided because number format is known to be ok
		n, _ := strconv.ParseInt(tk.Value, 10, 64)
		return expression.NewLiteral(n, sql.BigInteger), nil
	case FloatToken:
		// error is avoided because number format is known to be ok
		f, _ := strconv.ParseFloat(tk.Value, 64)
		return expression.NewLiteral(f, sql.Float), nil
	}

	// TODO: this should not be possible
	return nil, nil
}
