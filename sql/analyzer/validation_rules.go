// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
	validateResolvedRule          = "validate_resolved"
	validateOrderByRule           = "validate_order_by"
	validateGroupByRule           = "validate_group_by"
	validateSchemaSourceRule      = "validate_schema_source"
	validateOperandsRule          = "validate_operands_rule"
	validateIndexCreationRule     = "validate_index_creation"
	validateCaseResultTypesRule   = "validate_case_result_types"
	validateIntervalUsageRule     = "validate_interval_usage"
	validateExplodeUsageRule      = "validate_explode_usage"
	validateSubqueryColumnsRule   = "validate_subquery_columns"
	validateUnionSchemasMatchRule = "validate_union_schemas_match"
	validateAggregationsRule      = "validate_aggregations"
)

var (
	// ErrValidationResolved is returned when the plan can not be resolved.
	ErrValidationResolved = errors.NewKind("plan is not resolved because of node '%T'")
	// ErrValidationOrderBy is returned when the order by contains aggregation
	// expressions.
	ErrValidationOrderBy = errors.NewKind("OrderBy does not support aggregation expressions")
	// ErrValidationGroupBy is returned when the aggregation expression does not
	// appear in the grouping columns.
	ErrValidationGroupBy = errors.NewKind("expression '%v' doesn't appear in the group by expressions")
	// ErrValidationSchemaSource is returned when there is any column source
	// that does not match the table name.
	ErrValidationSchemaSource = errors.NewKind("one or more schema sources are empty")
	// ErrUnknownIndexColumns is returned when there are columns in the expr
	// to index that are unknown in the table.
	ErrUnknownIndexColumns = errors.NewKind("unknown columns to index for table %q: %s")
	// ErrCaseResultType is returned when one or more of the types of the values in
	// a case expression don't match.
	ErrCaseResultType = errors.NewKind(
		"expecting all case branches to return values of type %s, " +
			"but found value %q of type %s on %s",
	)
	// ErrIntervalInvalidUse is returned when an interval expression is not
	// correctly used.
	ErrIntervalInvalidUse = errors.NewKind(
		"invalid use of an interval, which can only be used with DATE_ADD, " +
			"DATE_SUB and +/- operators to subtract from or add to a date",
	)
	// ErrExplodeInvalidUse is returned when an EXPLODE function is used
	// outside a Project node.
	ErrExplodeInvalidUse = errors.NewKind(
		"using EXPLODE is not supported outside a Project node",
	)

	// ErrSubqueryFieldIndex is returned when an expression subquery references a field outside the range of the rows it
	// works on.
	ErrSubqueryFieldIndex = errors.NewKind(
		"subquery field index out of range for expression %s: only %d columns available",
	)

	// ErrUnionSchemasMatch is returned when both sides of a UNION do not
	// have the same schema.
	ErrUnionSchemasMatch = errors.NewKind(
		"the schema of the left side of union does not match the right side, expected %s to match %s",
	)

	// ErrReadOnlyDatabase is returned when a write is attempted to a ReadOnlyDatabse.
	ErrReadOnlyDatabase = errors.NewKind("Database %s is read-only.")

	// ErrAggregationUnsupported is returned when the analyzer has failed
	// to push down an Aggregation in an expression to a GroupBy node.
	ErrAggregationUnsupported = errors.NewKind(
		"an aggregation remained in the expression '%s' after analysis, outside of a node capable of evaluating it; this query is currently unsupported.",
	)
)

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []Rule{
	{validateResolvedRule, validateIsResolved},
	{validateOrderByRule, validateOrderBy},
	{validateGroupByRule, validateGroupBy},
	{validateSchemaSourceRule, validateSchemaSource},
	{validateIndexCreationRule, validateIndexCreation},
	{validateOperandsRule, validateOperands},
	{validateCaseResultTypesRule, validateCaseResultTypes},
	{validateIntervalUsageRule, validateIntervalUsage},
	{validateExplodeUsageRule, validateExplodeUsage},
	{validateSubqueryColumnsRule, validateSubqueryColumns},
	{validateUnionSchemasMatchRule, validateUnionSchemasMatch},
	{validateAggregationsRule, validateAggregations},
}

// validateLimitAndOffset ensures that only integer literals are used for limit and offset values
func validateLimitAndOffset(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Limit:
			switch e := n.Limit.(type) {
			case *expression.Literal:
				if !sql.IsInteger(e.Type()) {
					return nil, sql.ErrInvalidType.New(e.Type().String())
				}
				i, err := e.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				i64, err := sql.Int64.Convert(i)
				if err != nil {
					return nil, err
				}
				if i64.(int64) < 0 {
					return nil, sql.ErrInvalidSyntax.New("negative limit")
				}
			case *expression.BindVar:
				return n, nil
			default:
				return nil, sql.ErrInvalidType.New(e.Type().String())
			}
			return n, nil
		case *plan.Offset:
			switch e := n.Offset.(type) {
			case *expression.Literal:
				if !sql.IsInteger(e.Type()) {
					return nil, sql.ErrInvalidType.New(e.Type().String())
				}
				i, err := e.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				i64, err := sql.Int64.Convert(i)
				if err != nil {
					return nil, err
				}
				if i64.(int64) < 0 {
					return nil, sql.ErrInvalidSyntax.New("negative offset")
				}
			case *expression.BindVar:
				return n, nil
			default:
				return nil, sql.ErrInvalidType.New(e.Type().String())
			}
			return n, nil
		default:
			return n, nil
		}
	})
}

func validateIsResolved(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_is_resolved")
	defer span.Finish()

	if !n.Resolved() {
		return nil, unresolvedError(n)
	}

	return n, nil
}

// unresolvedError returns an appropriate error message for the unresolved node given
func unresolvedError(n sql.Node) error {
	var err error
	var walkFn func(sql.Expression) bool
	walkFn = func(e sql.Expression) bool {
		switch e := e.(type) {
		case *plan.Subquery:
			plan.InspectExpressions(e.Query, walkFn)
			if err != nil {
				return false
			}
		case *deferredColumn:
			if e.Table() != "" {
				err = sql.ErrTableColumnNotFound.New(e.Table(), e.Name())
			} else {
				err = sql.ErrColumnNotFound.New(e.Name())
			}
			return false
		}
		return true
	}
	plan.InspectExpressions(n, walkFn)

	if err != nil {
		return err
	}
	return ErrValidationResolved.New(n)
}

func validateOrderBy(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_order_by")
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

func validateGroupBy(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_group_by")
	defer span.Finish()

	switch n := n.(type) {
	case *plan.GroupBy:
		// Allow the parser use the GroupBy node to eval the aggregation functions
		// for sql statements that don't make use of the GROUP BY expression.
		if len(n.GroupByExprs) == 0 {
			return n, nil
		}

		var groupBys []string
		for _, expr := range n.GroupByExprs {
			groupBys = append(groupBys, expr.String())
		}

		for _, expr := range n.SelectedExprs {
			if _, ok := expr.(sql.Aggregation); !ok {
				if !expressionReferencesOnlyGroupBys(groupBys, expr) {
					return nil, ErrValidationGroupBy.New(expr.String())
				}
			}
		}

		return n, nil
	}

	return n, nil
}

func expressionReferencesOnlyGroupBys(groupBys []string, expr sql.Expression) bool {
	valid := true
	sql.Inspect(expr, func(expr sql.Expression) bool {
		switch expr := expr.(type) {
		case nil, sql.Aggregation, *expression.Literal:
			return false
		case *expression.Alias, sql.FunctionExpression:
			if stringContains(groupBys, expr.String()) {
				return false
			}
			return true
		// cc: https://dev.mysql.com/doc/refman/8.0/en/group-by-handling.html
		// Each part of the SelectExpr must refer to the aggregated columns in some way
		// TODO: this isn't complete, it's overly restrictive. Dependant columns are fine to reference.
		default:
			if stringContains(groupBys, expr.String()) {
				return true
			}

			if len(expr.Children()) == 0 {
				valid = false
				return false
			}

			return true
		}
	})

	return valid
}

func validateSchemaSource(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_schema_source")
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

func validateIndexCreation(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_index_creation")
	defer span.Finish()

	ci, ok := n.(*plan.CreateIndex)
	if !ok {
		return n, nil
	}

	schema := ci.Table.Schema()
	table := schema[0].Source

	var unknownColumns []string
	for _, expr := range ci.Exprs {
		sql.Inspect(expr, func(e sql.Expression) bool {
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

func validateUnionSchemasMatch(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_union_schemas_match")
	defer span.Finish()

	var firstmismatch []string
	plan.Inspect(n, func(n sql.Node) bool {
		if u, ok := n.(*plan.Union); ok {
			ls := u.Left().Schema()
			rs := u.Right().Schema()
			if len(ls) != len(rs) {
				firstmismatch = []string{
					fmt.Sprintf("%d columns", len(ls)),
					fmt.Sprintf("%d columns", len(rs)),
				}
				return false
			}
			for i := range ls {
				if !reflect.DeepEqual(ls[i].Type, rs[i].Type) {
					firstmismatch = []string{
						ls[i].Type.String(),
						rs[i].Type.String(),
					}
					return false
				}
			}
		}
		return true
	})
	if firstmismatch != nil {
		return nil, ErrUnionSchemasMatch.New(firstmismatch[0], firstmismatch[1])
	}
	return n, nil
}

func validateCaseResultTypes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("validate_case_result_types")
	defer span.Finish()

	var err error
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.Case:
			typ := e.Type()
			for _, b := range e.Branches {
				if !sql.AreComparable(b.Value.Type(), typ) && b.Value.Type() != sql.Null {
					err = ErrCaseResultType.New(typ, b.Value, b.Value.Type(), e)
					return false
				}
			}

			if e.Else != nil {
				if !sql.AreComparable(e.Else.Type(), typ) && e.Else.Type() != sql.Null {
					err = ErrCaseResultType.New(typ, e.Else, e.Else.Type(), e)
					return false
				}
			}

			return false
		default:
			return true
		}
	})

	if err != nil {
		return nil, err
	}

	return n, nil
}

func validateIntervalUsage(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	var invalid bool
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		// If it's already invalid just skip everything else.
		if invalid {
			return false
		}

		switch e := e.(type) {
		case *function.DateAdd, *function.DateSub:
			return false
		case *expression.Arithmetic:
			if e.Op == "+" || e.Op == "-" {
				return false
			}
		case *expression.Interval:
			invalid = true
		}

		return true
	})

	if invalid {
		return nil, ErrIntervalInvalidUse.New()
	}

	return n, nil
}

func validateExplodeUsage(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	var invalid bool
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		// If it's already invalid just skip everything else.
		if invalid {
			return false
		}

		// All usage of Explode will be incorrect because the ones in projects
		// would have already been converted to Generate, so we only have to
		// look for those.
		if _, ok := e.(*function.Explode); ok {
			invalid = true
		}

		return true
	})

	if invalid {
		return nil, ErrExplodeInvalidUse.New()
	}

	return n, nil
}

func validateOperands(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Validate that the number of columns in an operand or a top level
	// expression are as expected. The current rules are:
	// * Every top level expression of a node must have 1 column.
	// * The following expression nodes are allowed to have `n` columns as
	// long as `n` matches:
	//   * *plan.InSubquery, *expression.{Equals,NullSafeEquals,GreaterThan,LessThan,GreaterThanOrEqual,LessThanOrEqual}
	// * *expression.InTuple must have a tuple on the right side, the # of
	// columns for each element of the tuple must match the number of
	// columns of the expression on the left.
	// * Every other expression with operands must have NumColumns == 1.

	// We do not use plan.InspectExpressions here because we're treating
	// top-level expressions of sql.Node differently from subexpressions.
	var err error
	plan.Inspect(n, func(n sql.Node) bool {
		if n == nil {
			return false
		}
		if er, ok := n.(sql.Expressioner); ok {
			for _, e := range er.Expressions() {
				nc := sql.NumColumns(e.Type())
				if nc != 1 {
					err = sql.ErrInvalidOperandColumns.New(1, nc)
					return false
				}
				sql.Inspect(e, func(e sql.Expression) bool {
					if e == nil {
						return err == nil
					}
					if err != nil {
						return false
					}
					switch e.(type) {
					case *plan.InSubquery, *expression.Equals, *expression.NullSafeEquals, *expression.GreaterThan,
						*expression.LessThan, *expression.GreaterThanOrEqual, *expression.LessThanOrEqual:
						err = sql.ErrIfMismatchedColumns(e.Children()[0].Type(), e.Children()[1].Type())
					case *expression.InTuple:
						t, ok := e.Children()[1].(expression.Tuple)
						if ok && len(t.Children()) == 1 {
							// A single element Tuple treats itself like the element it contains.
							err = sql.ErrIfMismatchedColumns(e.Children()[0].Type(), e.Children()[1].Type())
						} else {
							err = sql.ErrIfMismatchedColumnsInTuple(e.Children()[0].Type(), e.Children()[1].Type())
						}
					case *aggregation.Count, *aggregation.CountDistinct, *aggregation.JSONArrayAgg:
						if _, s := e.Children()[0].(*expression.Star); s {
							return false
						}
						for _, e := range e.Children() {
							nc := sql.NumColumns(e.Type())
							if nc != 1 {
								err = sql.ErrInvalidOperandColumns.New(1, nc)
							}
						}
					case expression.Tuple:
						// Tuple expressions can contain tuples...
					default:
						for _, e := range e.Children() {
							nc := sql.NumColumns(e.Type())
							if nc != 1 {
								err = sql.ErrInvalidOperandColumns.New(1, nc)
							}
						}
					}
					return err == nil
				})
			}
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

func validateSubqueryColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Then validate that every subquery has field indexes within the correct range
	// TODO: Why is this only for subqueries?

	// TODO: Currently disabled.
	if true {
		return n, nil
	}

	var outOfRangeIndexExpression sql.Expression
	var outOfRangeColumns int
	plan.InspectExpressionsWithNode(n, func(n sql.Node, e sql.Expression) bool {
		s, ok := e.(*plan.Subquery)
		if !ok {
			return true
		}

		outerScopeRowLen := len(scope.Schema()) + len(schemas(n.Children()))
		plan.Inspect(s.Query, func(n sql.Node) bool {
			if n == nil {
				return true
			}
			// TODO: the schema of the rows seen by children of
			// these nodes are not reflected in the schema
			// calculations here. This needs to be rationalized
			// across the analyzer.
			switch n.(type) {
			case *plan.IndexedJoin, *plan.IndexedInSubqueryFilter:
				return false
			}
			if es, ok := n.(sql.Expressioner); ok {
				childSchemaLen := len(schemas(n.Children()))
				for _, e := range es.Expressions() {
					sql.Inspect(e, func(e sql.Expression) bool {
						if gf, ok := e.(*expression.GetField); ok {
							if gf.Index() >= outerScopeRowLen+childSchemaLen {
								outOfRangeIndexExpression = gf
								outOfRangeColumns = outerScopeRowLen + childSchemaLen
							}
						}
						return outOfRangeIndexExpression == nil
					})
				}
			}
			return outOfRangeIndexExpression == nil
		})
		return outOfRangeIndexExpression == nil
	})
	if outOfRangeIndexExpression != nil {
		return nil, ErrSubqueryFieldIndex.New(outOfRangeIndexExpression, outOfRangeColumns)
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

func tableColsContains(strs []tableCol, target tableCol) bool {
	for _, s := range strs {
		if s == target {
			return true
		}
	}
	return false
}

// validateReadOnlyDatabase invalidates queries that attempt to write to ReadOnlyDatabases.
func validateReadOnlyDatabase(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	valid := true
	var readOnlyDB sql.ReadOnlyDatabase

	// if a ReadOnlyDatabase is found, invalidate the query
	readOnlyDBSearch := func(node sql.Node) bool {
		if rt, ok := node.(*plan.ResolvedTable); ok {
			if ro, ok := rt.Database.(sql.ReadOnlyDatabase); ok {
				if ro.IsReadOnly() {
					readOnlyDB = ro
					valid = false
				}
			}
		}
		return valid
	}

	plan.Inspect(n, func(node sql.Node) bool {
		switch n := n.(type) {
		case *plan.DeleteFrom, *plan.Update, *plan.LockTables, *plan.UnlockTables:
			plan.Inspect(node, readOnlyDBSearch)
			return false

		case *plan.InsertInto:
			// ReadOnlyDatabase can be an insertion Source,
			// only inspect the Destination tree
			plan.Inspect(n.Destination, readOnlyDBSearch)
			return false

		case *plan.CreateTable:
			if ro, ok := n.Database().(sql.ReadOnlyDatabase); ok {
				if ro.IsReadOnly() {
					readOnlyDB = ro
					valid = false
				}
			}
			// "CREATE TABLE ... LIKE ..." and
			// "CREATE TABLE ... AS ..."
			// can both use ReadOnlyDatabases as a source,
			// so don't descend here.
			return false

		default:
			// CreateTable is the only DDL node allowed
			// to contain a ReadOnlyDatabase
			if plan.IsDDLNode(n) {
				plan.Inspect(n, readOnlyDBSearch)
				return false
			}
		}

		return valid
	})
	if !valid {
		return nil, ErrReadOnlyDatabase.New(readOnlyDB.Name())
	}

	return n, nil
}

// validateReadOnlyTransaction invalidates read only transactions that try to perform improper write operations.
func validateReadOnlyTransaction(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	t := ctx.GetTransaction()

	if t == nil {
		return n, nil
	}

	// If this is a normal read write transaction don't enforce read-only. Otherwise we must prevent an invalid query.
	if !t.IsReadOnly() {
		return n, nil
	}

	valid := true

	isTempTable := func(table sql.Table) bool {
		tt, isTempTable := table.(sql.TemporaryTable)
		if !isTempTable {
			valid = false
		}

		return tt.IsTemporary()
	}

	temporaryTableSearch := func(node sql.Node) bool {
		if rt, ok := node.(*plan.ResolvedTable); ok {
			valid = isTempTable(rt.Table)
		}
		return valid
	}

	plan.Inspect(n, func(node sql.Node) bool {
		switch n := n.(type) {
		case *plan.DeleteFrom, *plan.Update, *plan.UnlockTables:
			plan.Inspect(node, temporaryTableSearch)
			return false
		case *plan.InsertInto:
			plan.Inspect(n.Destination, temporaryTableSearch)
			return false
		case *plan.LockTables:
			// TODO: Technically we should allow for the locking of temporary tables but the LockTables implementation
			// needs substantial refactoring.
			valid = false
			return false
		case *plan.CreateTable:
			// MySQL explicitly blocks the creation of temporary tables in a read only transaction.
			if n.Temporary() == plan.IsTempTable {
				valid = false
			}

			return false
		default:
			// DDL statements have an implicit commits which makes them valid to be executed in READ ONLY transactions.
			if plan.IsDDLNode(n) {
				valid = true
				return false
			}

			return valid
		}
	})

	if !valid {
		return nil, sql.ErrReadOnlyTransaction.New()
	}

	return n, nil
}

// validateAggregations returns an error if an Aggregation
// expression node appears outside of a GroupBy or Window node. Only GroupBy
// and Window nodes know how to evaluate Aggregation expressions.
//
// See https://github.com/dolthub/go-mysql-server/issues/542 for some queries
// that should be supported but that currently trigger this validation because
// aggregation expressions end up in the wrong place.
func validateAggregations(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	var invalidExpr sql.Expression
	checkExpressions := func(exprs []sql.Expression) bool {
		for _, e := range exprs {
			sql.Inspect(e, func(ie sql.Expression) bool {
				if _, ok := ie.(sql.Aggregation); ok {
					invalidExpr = e
				}
				return invalidExpr == nil
			})
		}
		return invalidExpr == nil
	}
	plan.Inspect(n, func(n sql.Node) bool {
		if gb, ok := n.(*plan.GroupBy); ok {
			return checkExpressions(gb.GroupByExprs)
		} else if _, ok := n.(*plan.Window); ok {
		} else if n, ok := n.(sql.Expressioner); ok {
			return checkExpressions(n.Expressions())
		}
		return invalidExpr == nil
	})
	if invalidExpr != nil {
		return nil, ErrAggregationUnsupported.New(invalidExpr.String())
	}
	return n, nil
}
