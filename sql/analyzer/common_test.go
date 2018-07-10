package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func or(left, right sql.Expression) sql.Expression {
	return expression.NewOr(left, right)
}

func and(left, right sql.Expression) sql.Expression {
	return expression.NewAnd(left, right)
}

func col(idx int, table, col string) sql.Expression {
	return expression.NewGetFieldWithTable(idx, sql.Int64, table, col, false)
}

func eq(left, right sql.Expression) sql.Expression {
	return expression.NewEquals(left, right)
}

func lit(n int64) sql.Expression {
	return expression.NewLiteral(n, sql.Int64)
}

func getRule(name string) Rule {
	for _, rule := range DefaultRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
