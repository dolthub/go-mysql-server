package analyzer

import (
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

const (
	validateResolvedRule      = "validate_resolved"
	validateOrderByRule       = "validate_order_by"
	validateGroupByRule       = "validate_group_by"
	validateSchemaSourceRule  = "validate_schema_source"
	validateProjectTuplesRule = "validate_project_tuples"
	validateIndexCreationRule = "validate_index_creation"
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
	ErrValidationSchemaSource = errors.NewKind("one or more schema sources are empty")
	// ErrProjectTuple is returned when there is a tuple of more than 1 column
	// inside a projection.
	ErrProjectTuple = errors.NewKind("selected field %d should have 1 column, but has %d")
	// ErrUnknownIndexColumns is returned when there are columns in the expr
	// to index that are unknown in the table.
	ErrUnknownIndexColumns = errors.NewKind("unknown columns to index for table %q: %s")
)

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []Rule{
	{validateResolvedRule, validateIsResolved},
	{validateOrderByRule, validateOrderBy},
	{validateGroupByRule, validateGroupBy},
	{validateSchemaSourceRule, validateSchemaSource},
	{validateProjectTuplesRule, validateProjectTuples},
	{validateIndexCreationRule, validateIndexCreation},
}

func validateIsResolved(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_is_resolved")
	defer span.Finish()

	if !n.Resolved() {
		return nil, ErrValidationResolved.New(n)
	}

	return n, nil
}

func validateOrderBy(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_order_by")
	defer span.Finish()

	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.Aggregation:
				return nil, ErrValidationOrderBy.New()
			}
		}
	}

	return n, nil
}

func validateGroupBy(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_order_by")
	defer span.Finish()

	switch n := n.(type) {
	case *plan.GroupBy:
		// Allow the parser use the GroupBy node to eval the aggregation functions
		// for sql statementes that don't make use of the GROUP BY expression.
		if len(n.Grouping) == 0 {
			return n, nil
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
					return nil, ErrValidationGroupBy.New(expr.String())
				}
			}
		}

		return n, nil
	}

	return n, nil
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

func validateSchemaSource(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_schema_source")
	defer span.Finish()

	switch n := n.(type) {
	case *plan.TableAlias:
		// table aliases should not be validated
		if child, ok := n.Child.(*plan.ResolvedTable); ok {
			return n, validateSchema(child)
		}
	case *plan.ResolvedTable:
		return n, validateSchema(n)
	}
	return n, nil
}

func validateIndexCreation(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_index_creation")
	defer span.Finish()

	ci, ok := n.(*plan.CreateIndex)
	if !ok {
		return n, nil
	}

	schema := ci.Table.Schema()
	table := schema[0].Source

	var unknownColumns []string
	for _, expr := range ci.Exprs {
		expression.Inspect(expr, func(e sql.Expression) bool {
			gf, ok := e.(*expression.GetField)
			if ok {
				if gf.Table() != table || !schema.Contains(gf.Name(), gf.Table()) {
					unknownColumns = append(unknownColumns, gf.Name())
				}
			}
			return true
		})
	}

	if len(unknownColumns) > 0 {
		return nil, ErrUnknownIndexColumns.New(table, strings.Join(unknownColumns, ", "))
	}

	return n, nil
}

func validateSchema(t *plan.ResolvedTable) error {
	for _, col := range t.Schema() {
		if col.Source == "" {
			return ErrValidationSchemaSource.New()
		}
	}
	return nil
}

func validateProjectTuples(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("validate_project_tuples")
	defer span.Finish()

	switch n := n.(type) {
	case *plan.Project:
		for i, e := range n.Projections {
			if sql.IsTuple(e.Type()) {
				return nil, ErrProjectTuple.New(i+1, sql.NumColumns(e.Type()))
			}
		}
	case *plan.GroupBy:
		for i, e := range n.Aggregate {
			if sql.IsTuple(e.Type()) {
				return nil, ErrProjectTuple.New(i+1, sql.NumColumns(e.Type()))
			}
		}
	}
	return n, nil
}

func stringContains(strs []string, target string) bool {
	for _, s := range strs {
		if s == target {
			return true
		}
	}
	return false
}
