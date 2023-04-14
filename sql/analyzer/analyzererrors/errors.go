// Copyright 2020-2023 Dolthub, Inc.
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

package analyzererrors

import "gopkg.in/src-d/go-errors.v1"

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

	ErrWindowUnsupported = errors.NewKind(
		"a window function '%s' is in a context where it cannot be evaluated.",
	)

	ErrStarUnsupported = errors.NewKind(
		"a '*' is in a context where it is not allowed.",
	)
)
