package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

const (
	validateResolvedRule = "validate_resolved"
	validateOrderByRule  = "validate_order_by"
	validateGroupByRule  = "validate_group_by"
)

var (
	ValidationResolvedErr = errors.NewKind("plan is not resolved because of node the '%T'")
	ValidationOrderByErr  = errors.NewKind("OrderBy does not support aggregation expressions")
	ValidationGroupByErr  = errors.NewKind("GroupBy aggregate expression '%v' doesn't appear in the grouping columns")
)

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []ValidationRule{
	{validateResolvedRule, validateIsResolved},
	{validateOrderByRule, validateOrderBy},
	{validateGroupByRule, validateGroupBy},
}

func validateIsResolved(n sql.Node) error {
	if !n.Resolved() {
		return ValidationResolvedErr.New(n)
	}

	return nil
}

func validateOrderBy(n sql.Node) error {
	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.AggregationExpression:
				return ValidationOrderByErr.New()
			}
		}
	}

	return nil
}

func validateGroupBy(n sql.Node) error {
	switch n := n.(type) {
	case *plan.GroupBy:
		// Allow the parser use the GroupBy node to eval the aggregation functions
		// for sql statementes that aren't really make use of the GROUP BY expression.
		if len(n.Grouping) == 0 {
			return nil
		}

		validAggs := []string{}
		for _, expr := range n.Grouping {
			validAggs = append(validAggs, expr.Name())
		}

		for _, expr := range n.Aggregate {
			if _, ok := expr.(sql.AggregationExpression); !ok {
				if !isValidAgg(validAggs, expr) {
					return ValidationGroupByErr.New(expr.Name())
				}
			}
		}

		return nil
	}

	return nil
}

func isValidAgg(validAggs []string, expr sql.Expression) bool {
	for _, validAgg := range validAggs {
		if validAgg == expr.Name() {
			return true
		}
	}

	return false
}
