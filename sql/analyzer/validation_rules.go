package analyzer

import (
	"errors"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/plan"
)

var DefaultValidationRules = []ValidationRule{
	{"validate_resolved", validateIsResolved},
	{"validate_order_by", validateOrderBy},
}

func validateIsResolved(a *Analyzer, n sql.Node) error {
	if !n.Resolved() {
		return errors.New("plan is not resolved")
	}

	return nil
}

func validateOrderBy(a *Analyzer, n sql.Node) error {
	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.AggregationExpression:
				return errors.New("OrderBy does not support aggregation expressions")
			}
		}
	}

	return nil
}
