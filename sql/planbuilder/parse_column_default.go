package planbuilder

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// StringToColumnDefaultValue takes in a string representing a default value and returns the equivalent Expression.
func StringToColumnDefaultValue(ctx *sql.Context, exprStr string) (*sql.ColumnDefaultValue, error) {
	// all valid default expressions will parse correctly with SELECT prepended, as the parser will not parse raw expressions
	stmt, err := sqlparser.Parse("SELECT " + exprStr)
	if err != nil {
		return nil, err
	}
	parserSelect, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("DefaultStringToExpression expected sqlparser.Select but received %T", stmt)
	}
	if len(parserSelect.SelectExprs) != 1 {
		return nil, fmt.Errorf("default string does not have only one expression")
	}
	aliasedExpr, ok := parserSelect.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("DefaultStringToExpression expected *sqlparser.AliasedExpr but received %T", parserSelect.SelectExprs[0])
	}
	proj, err := Parse(ctx, nil, fmt.Sprintf("SELECT %s", aliasedExpr.Expr))
	if err != nil {
		return nil, err
	}
	parsedExpr := proj.(*plan.Project).Projections[0]
	if a, ok := parsedExpr.(*expression.Alias); ok {
		parsedExpr = a.Child
	}
	_, isParenthesized := aliasedExpr.Expr.(*sqlparser.ParenExpr)

	var isLiteral bool
	switch e := parsedExpr.(type) {
	case *expression.UnaryMinus:
		_, isLiteral = e.Child.(*expression.Literal)
	case *expression.UnresolvedFunction:
		isLiteral = false
	default:
		isLiteral = len(parsedExpr.Children()) == 0 && !strings.HasPrefix(exprStr, "(")
	}
	return ExpressionToColumnDefaultValue(parsedExpr, isLiteral, isParenthesized), nil
}

// MustStringToColumnDefaultValue is used for creating default values on tables that do not go through the analyzer. Does not handle
// function nor column references.
func MustStringToColumnDefaultValue(ctx *sql.Context, exprStr string, outType sql.Type, nullable bool) *sql.ColumnDefaultValue {
	expr, err := StringToColumnDefaultValue(ctx, exprStr)
	if err != nil {
		panic(err)
	}
	expr, err = sql.NewColumnDefaultValue(expr.Expression, outType, expr.IsLiteral(), !expr.IsLiteral(), nullable)
	if err != nil {
		panic(err)
	}
	return expr
}
