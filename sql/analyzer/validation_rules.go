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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
	validateResolvedRule          = "validate_resolved"
	validateOrderByRule           = "validate_order_by"
	validateGroupByRule           = "validate_group_by"
	validateSchemaSourceRule      = "validate_schema_source"
	validateProjectTuplesRule     = "validate_project_tuples"
	validateIndexCreationRule     = "validate_index_creation"
	validateCaseResultTypesRule   = "validate_case_result_types"
	validateIntervalUsageRule     = "validate_interval_usage"
	validateExplodeUsageRule      = "validate_explode_usage"
	validateSubqueryColumnsRule   = "validate_subquery_columns"
	validateUnionSchemasMatchRule = "validate_union_schemas_match"
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
	// ErrProjectTuple is returned when there is a tuple of more than 1 column
	// inside a projection.
	ErrProjectTuple = errors.NewKind("selected field %d should have 1 column, but has %d")
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
)

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []Rule{
	{validateResolvedRule, validateIsResolved},
	{validateOrderByRule, validateOrderBy},
	{validateGroupByRule, validateGroupBy},
	{validateSchemaSourceRule, validateSchemaSource},
	{validateProjectTuplesRule, validateProjectTuples},
	{validateIndexCreationRule, validateIndexCreation},
	{validateCaseResultTypesRule, validateCaseResultTypes},
	{validateIntervalUsageRule, validateIntervalUsage},
	{validateExplodeUsageRule, validateExplodeUsage},
	{validateSubqueryColumnsRule, validateSubqueryColumns},
	{validateUnionSchemasMatchRule, validateUnionSchemasMatch},
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

func findProjectTuples(n sql.Node) (sql.Node, error) {
	if n == nil {
		return n, nil
	}

	switch n := n.(type) {
	case *plan.Project, *plan.GroupBy, *plan.Window:
		for i, e := range n.(sql.Expressioner).Expressions() {
			if sql.IsTuple(e.Type()) {
				return nil, ErrProjectTuple.New(i+1, sql.NumColumns(e.Type()))
			}
		}
	default:
		for _, ch := range n.Children() {
			_, err := findProjectTuples(ch)
			if err != nil {
				return nil, err
			}
		}
	}

	return n, nil
}

func validateProjectTuples(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("validate_project_tuples")
	defer span.Finish()
	return findProjectTuples(n)
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

func validateSubqueryColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {

	// First validate that every subquery expression returns a single column
	valid := true
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		s, ok := e.(*plan.Subquery)
		if ok && len(s.Query.Schema()) != 1 {
			valid = false
			return false
		}

		return true
	})

	if !valid {
		return nil, sql.ErrSubqueryMultipleColumns.New()
	}

	// Then validate that every subquery has field indexes within the correct range
	var outOfRangeIndexExpression sql.Expression
	var outOfRangeColumns int
	plan.InspectExpressionsWithNode(n, func(n sql.Node, e sql.Expression) bool {
		s, ok := e.(*plan.Subquery)
		if !ok {
			return true
		}

		outerScopeRowLen := len(schemas(n.Children()))
		plan.InspectExpressionsWithNode(s.Query, func(n sql.Node, e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() >= outerScopeRowLen+len(schemas(n.Children())) {
					outOfRangeIndexExpression = gf
					outOfRangeColumns = outerScopeRowLen + len(schemas(n.Children()))
					return false
				}
			}
			return true
		})

		return outOfRangeIndexExpression == nil
	})

	if !valid {
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
