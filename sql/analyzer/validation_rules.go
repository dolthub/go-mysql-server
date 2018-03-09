package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

		validAggs := []string{}
		for _, expr := range n.Grouping {
			validAggs = append(validAggs, expr.Name())
		}

		for _, expr := range n.Aggregate {
			if _, ok := expr.(sql.Aggregation); !ok {
				if !isValidAgg(validAggs, expr) {
					return ErrValidationGroupBy.New(expr.Name())
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

func validateSchemaSource(n sql.Node) error {
	switch n := n.(type) {
	case *plan.TableAlias:
		// table aliases are expected to bypass this validation only if what's
		// inside of them is not a subquery, because by definition, their
		// schema will have a different name.
		if _, ok := n.Child.(*plan.Project); !ok {
			return nil
		}

		return validateSchema(n)
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
