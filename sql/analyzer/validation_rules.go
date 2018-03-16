package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

const (
	validateResolvedRule     = "validate_resolved"
	validateOrderByRule      = "validate_order_by"
	validateGroupByRule      = "validate_group_by"
	validateSchemaSourceRule = "validate_schema_source"
)

var (
	// ErrValidationResolved is returned when the plan can not be resolved.
	ErrValidationResolved = errors.NewKind("plan is not resolved because of node '%T'")
	// ErrValidationOrderBy is returned when the order by contains aggregation
	// expressions.
	ErrValidationOrderBy = errors.NewKind("OrderBy does not support aggregation expressions")
	// ErrValidationGroupBy is returned when the aggregation expression does not
	// appear in the grouping columns.
	ErrValidationGroupBy = errors.NewKind("GroupBy aggregate expression '%v' doesn't appear in the grouping columns")
	// ErrValidationSchemaSource is returned when there is any column source
	// that does not match the table name.
	ErrValidationSchemaSource = errors.NewKind("all schema column sources don't match table name, expecting %q, but found: %s")
)

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []ValidationRule{
	{validateResolvedRule, validateIsResolved},
	{validateOrderByRule, validateOrderBy},
	{validateGroupByRule, validateGroupBy},
	{validateSchemaSourceRule, validateSchemaSource},
}

func validateIsResolved(n sql.Node) error {
	if !n.Resolved() {
		return ErrValidationResolved.New(n)
	}

	return nil
}

func validateOrderBy(n sql.Node) error {
	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.Aggregation:
				return ErrValidationOrderBy.New()
			}
		}
	}

	return nil
}

func validateGroupBy(n sql.Node) error {
	switch n := n.(type) {
	case *plan.GroupBy:
		// Allow the parser use the GroupBy node to eval the aggregation functions
		// for sql statementes that don't make use of the GROUP BY expression.
		if len(n.Grouping) == 0 {
			return nil
		}

		var validAggs []string
		for _, expr := range n.Grouping {
			validAggs = append(validAggs, expr.String())
		}

		// TODO: validate columns inside aggregations
		// and allow any kind of expression that make use of the grouping
		// columns.
		for _, expr := range n.Aggregate {
			if _, ok := expr.(sql.Aggregation); !ok {
				if !isValidAgg(validAggs, expr) {
					return ErrValidationGroupBy.New(expr.String())
				}
			}
		}

		return nil
	}

	return nil
}

func isValidAgg(validAggs []string, expr sql.Expression) bool {
	switch expr := expr.(type) {
	case sql.Aggregation:
		return true
	case *expression.Alias:
		return isValidAgg(validAggs, expr.Child)
	default:
		return stringContains(validAggs, expr.String())
	}
}

func validateSchemaSource(n sql.Node) error {
	switch n := n.(type) {
	case *plan.TableAlias:
		// table aliases should not be validated
		if child, ok := n.Child.(sql.Table); ok {
			return validateSchema(child)
		}
	case sql.Table:
		return validateSchema(n)
	}
	return nil
}

func validateSchema(t sql.Table) error {
	name := t.Name()
	for _, col := range t.Schema() {
		if col.Source != name {
			return ErrValidationSchemaSource.New(name, col.Source)
		}
	}
	return nil
}

func stringContains(strs []string, target string) bool {
	for _, s := range strs {
		if s == target {
			return true
		}
	}
	return false
}
