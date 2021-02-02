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

package sql

import "gopkg.in/src-d/go-errors.v1"

var (
	// ErrInvalidType is thrown when there is an unexpected type at some part of
	// the execution tree.
	ErrInvalidType = errors.NewKind("invalid type: %s")

	// ErrTableAlreadyExists is thrown when someone tries to create a
	// table with a name of an existing one
	ErrTableAlreadyExists = errors.NewKind("table with name %s already exists")

	// ErrTableNotFound is returned when the table is not available from the
	// current scope.
	ErrTableNotFound = errors.NewKind("table not found: %s")

	// ErrColumnNotFound is thrown when a column named cannot be found in scope
	ErrTableColumnNotFound = errors.NewKind("table %q does not have column %q")

	// ErrColumnNotFound is returned when the column does not exist in any
	// table in scope.
	ErrColumnNotFound = errors.NewKind("column %q could not be found in any table in scope")

	// ErrAmbiguousColumnName is returned when there is a column reference that
	// is present in more than one table.
	ErrAmbiguousColumnName = errors.NewKind("ambiguous column name %q, it's present in all these tables: %v")

	// ErrUnexpectedRowLength is thrown when the obtained row has more columns than the schema
	ErrUnexpectedRowLength = errors.NewKind("expected %d values, got %d")

	// ErrInvalidChildrenNumber is returned when the WithChildren method of a
	// node or expression is called with an invalid number of arguments.
	ErrInvalidChildrenNumber = errors.NewKind("%T: invalid children number, got %d, expected %d")

	// ErrInvalidChildType is returned when the WithChildren method of a
	// node or expression is called with an invalid child type. This error is indicative of a bug.
	ErrInvalidChildType = errors.NewKind("%T: invalid child type, got %T, expected %T")

	// ErrDeleteRowNotFound
	ErrDeleteRowNotFound = errors.NewKind("row was not found when attempting to delete")

	// ErrDuplicateAlias should be returned when a query contains a duplicate alias / table name.
	ErrDuplicateAliasOrTable = errors.NewKind("Not unique table/alias: %s")

	// ErrPrimaryKeyViolation is returned when a primary key constraint is violated
	ErrPrimaryKeyViolation = errors.NewKind("duplicate primary key given: %s")

	// ErrUniqueKeyViolation is returned when a unique key constraint is violated
	ErrUniqueKeyViolation = errors.NewKind("duplicate unique key given: %s")

	// ErrMisusedAlias is returned when a alias is defined and used in the same projection.
	ErrMisusedAlias = errors.NewKind("column %q does not exist in scope, but there is an alias defined in" +
		" this projection with that name. Aliases cannot be used in the same projection they're defined in")

	// ErrInvalidAsOfExpression is returned when an expression for AS OF cannot be used
	ErrInvalidAsOfExpression = errors.NewKind("expression %s cannot be used in AS OF")

	// ErrIncompatibleDefaultType is returned when a provided default cannot be coerced into the type of the column
	ErrIncompatibleDefaultType = errors.NewKind("incompatible type for default value")

	// ErrInvalidTextBlobColumnDefault is returned when a column of type text/blob (or related) has a literal default set.
	ErrInvalidTextBlobColumnDefault = errors.NewKind("text/blob types may only have expression default values")

	// ErrInvalidColumnDefaultFunction is returned when an invalid function is used in a default value.
	ErrInvalidColumnDefaultFunction = errors.NewKind("function `%s` on column `%s` is not valid for usage in a default value")

	// ErrColumnDefaultDatetimeOnlyFunc is returned when a non datetime/timestamp column attempts to declare now/current_timestamp as a default value literal.
	ErrColumnDefaultDatetimeOnlyFunc = errors.NewKind("only datetime/timestamp may declare default values of now()/current_timestamp() without surrounding parentheses")

	// ErrColumnDefaultSubquery is returned when a default value contains a subquery.
	ErrColumnDefaultSubquery = errors.NewKind("default value on column `%s` may not contain subqueries")

	// ErrInvalidDefaultValueOrder is returned when a default value references a column that comes after it and contains a default expression.
	ErrInvalidDefaultValueOrder = errors.NewKind(`default value of column "%s" cannot refer to a column defined after it if those columns have an expression default value`)

	// ErrColumnDefaultReturnedNull is returned when a default expression evaluates to nil but the column is non-nullable.
	ErrColumnDefaultReturnedNull = errors.NewKind(`default value attempted to return null but column is non-nullable`)

	// ErrDropColumnReferencedInDefault is returned when a column cannot be dropped as it is referenced by another column's default value.
	ErrDropColumnReferencedInDefault = errors.NewKind(`cannot drop column "%s" as default value of column "%s" references it`)

	// ErrTriggersNotSupported is returned when attempting to create a trigger on a database that doesn't support them
	ErrTriggersNotSupported = errors.NewKind(`database "%s" doesn't support triggers`)

	// ErrTriggerCreateStatementInvalid is returned when a TriggerDatabase returns a CREATE TRIGGER statement that is invalid
	ErrTriggerCreateStatementInvalid = errors.NewKind(`Invalid CREATE TRIGGER statement: %s`)

	// ErrTriggerDoesNotExist is returned when a trigger does not exist.
	ErrTriggerDoesNotExist = errors.NewKind(`trigger "%s" does not exist`)

	// ErrTriggerTableInUse is returned when trigger execution calls for a table that invoked a trigger being updated by it
	ErrTriggerTableInUse = errors.NewKind("Can't update table %s in stored function/trigger because it is already used by statement which invoked this stored function/trigger")

	// ErrTriggerCannotBeDropped is returned when dropping a trigger would cause another trigger to reference a non-existent trigger.
	ErrTriggerCannotBeDropped = errors.NewKind(`trigger "%s" cannot be dropped as it is referenced by trigger "%s"`)

	// ErrUnknownSystemVariable is returned when a query references a system variable that doesn't exist
	ErrUnknownSystemVariable = errors.NewKind(`Unknown system variable '%s'`)

	// ErrInvalidUseOfOldNew is returned when a trigger attempts to make use of OLD or NEW references when they don't exist
	ErrInvalidUseOfOldNew = errors.NewKind("There is no %s row in on %s trigger")

	// ErrInvalidUpdateOfOldRow is returned when a trigger attempts to assign to an old row's value with SET
	ErrInvalidUpdateOfOldRow = errors.NewKind("Updating of old row is not allowed in trigger")

	// ErrInvalidUpdateInAfterTrigger is returned when a trigger attempts to assign to a new row in an AFTER trigger
	ErrInvalidUpdateInAfterTrigger = errors.NewKind("Updating of new row is not allowed in after trigger")

	// ErrUnboundPreparedStatementVariable is returned when a query is executed without a binding for one its variables.
	ErrUnboundPreparedStatementVariable = errors.NewKind(`unbound variable "%s" in query`)

	// ErrTruncateReferencedFromForeignKey is returned when a table is referenced in a foreign key and TRUNCATE is called on it.
	ErrTruncateReferencedFromForeignKey = errors.NewKind("cannot truncate table %s as it is referenced in foreign key %s on table %s")
)
