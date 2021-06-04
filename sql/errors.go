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

import (
	"fmt"

	"github.com/dolthub/vitess/go/mysql"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrSyntaxError is returned when a syntax error in vitess is encountered.
	ErrSyntaxError = errors.NewKind("%s")

	// ErrUnsupportedFeature is thrown when a feature is not already supported
	ErrUnsupportedFeature = errors.NewKind("unsupported feature: %s")

	// ErrInvalidSystemVariableValue is returned when a system variable is assigned a value that it does not accept.
	ErrInvalidSystemVariableValue = errors.NewKind("Variable '%s' can't be set to the value of '%v'")

	// ErrSystemVariableCodeFail is returned when failing to encode/decode a system variable.
	ErrSystemVariableCodeFail = errors.NewKind("unable to encode/decode value '%v' for '%s'")

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

	// ErrAmbiguousColumnInOrderBy is returned when an order by column is ambiguous
	ErrAmbiguousColumnInOrderBy = errors.NewKind("Column %q in order clause is ambiguous")

	// ErrUnexpectedRowLength is thrown when the obtained row has more columns than the schema
	ErrUnexpectedRowLength = errors.NewKind("expected %d values, got %d")

	// ErrInvalidChildrenNumber is returned when the WithChildren method of a
	// node or expression is called with an invalid number of arguments.
	ErrInvalidChildrenNumber = errors.NewKind("%T: invalid children number, got %d, expected %d")

	// ErrInvalidChildType is returned when the WithChildren method of a
	// node or expression is called with an invalid child type. This error is indicative of a bug.
	ErrInvalidChildType = errors.NewKind("%T: invalid child type, got %T, expected %T")

	// ErrInvalidJSONText is returned when a JSON string cannot be parsed or unmarshalled
	ErrInvalidJSONText = errors.NewKind("Invalid JSON text: %s")

	// ErrDeleteRowNotFound
	ErrDeleteRowNotFound = errors.NewKind("row was not found when attempting to delete")

	// ErrDuplicateAlias should be returned when a query contains a duplicate alias / table name.
	ErrDuplicateAliasOrTable = errors.NewKind("Not unique table/alias: %s")

	// ErrPrimaryKeyViolation is returned when a primary key constraint is violated
	// This is meant to wrap a sql.UniqueKey error, which provides the key string
	ErrPrimaryKeyViolation = errors.NewKind("duplicate primary key given")

	// ErrUniqueKeyViolation is returned when a unique key constraint is violated
	// This is meant to wrap a sql.UniqueKey error, which provides the key string
	ErrUniqueKeyViolation = errors.NewKind("duplicate unique key given")

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

	// ErrStoredProceduresNotSupported is returned when attempting to create a stored procedure on a database that doesn't support them.
	ErrStoredProceduresNotSupported = errors.NewKind(`database "%s" doesn't support stored procedures`)

	// ErrTriggerDoesNotExist is returned when a stored procedure does not exist.
	ErrStoredProcedureAlreadyExists = errors.NewKind(`stored procedure "%s" already exists`)

	// ErrTriggerDoesNotExist is returned when a stored procedure does not exist.
	ErrStoredProcedureDoesNotExist = errors.NewKind(`stored procedure "%s" does not exist`)

	// ErrProcedureCreateStatementInvalid is returned when a StoredProcedureDatabase returns a CREATE PROCEDURE statement that is invalid.
	ErrProcedureCreateStatementInvalid = errors.NewKind(`Invalid CREATE PROCEDURE statement: %s`)

	// ErrProcedureDuplicateParameterName is returned when a stored procedure has two (or more) parameters with the same name.
	ErrProcedureDuplicateParameterName = errors.NewKind("duplicate parameter name `%s` on stored procedure `%s`")

	// ErrProcedureRecursiveCall is returned when a stored procedure has a CALL statement that refers to itself.
	ErrProcedureRecursiveCall = errors.NewKind("recursive CALL on stored procedure `%s`")

	// ErrProcedureInvalidBodyStatement is returned when a stored procedure has a statement that is invalid inside of procedures.
	ErrProcedureInvalidBodyStatement = errors.NewKind("`%s` statements are invalid inside of stored procedures")

	// ErrCallIncorrectParameterCount is returned when a CALL statement has the incorrect number of parameters.
	ErrCallIncorrectParameterCount = errors.NewKind("`%s` expected `%d` parameters but got `%d`")

	// ErrUnknownSystemVariable is returned when a query references a system variable that doesn't exist
	ErrUnknownSystemVariable = errors.NewKind(`Unknown system variable '%s'`)

	// ErrSystemVariableReadOnly is returned when attempting to set a value to a non-Dynamic system variable.
	ErrSystemVariableReadOnly = errors.NewKind(`Variable '%s' is a read only variable`)

	// ErrSystemVariableSessionOnly is returned when attempting to set a SESSION-only variable using SET GLOBAL.
	ErrSystemVariableSessionOnly = errors.NewKind(`Variable '%s' is a SESSION variable and can't be used with SET GLOBAL`)

	// ErrSystemVariableGlobalOnly is returned when attempting to set a GLOBAL-only variable using SET SESSION.
	ErrSystemVariableGlobalOnly = errors.NewKind(`Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL`)

	// ErrUserVariableNoDefault is returned when attempting to set the default value on a user variable.
	ErrUserVariableNoDefault = errors.NewKind(`User variable '%s' does not have a default value`)

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

	// ErrInvalidColTypeDefinition is returned when a column type-definition has argument violations.
	ErrInvalidColTypeDefinition = errors.NewKind("column %s type definition is invalid: %s")

	// ErrCannotCreateDatabaseExists is returned when a CREATE DATABASE is called on a table that already exists.
	ErrCannotCreateDatabaseExists = errors.NewKind("can't create database %s; database exists")

	// ErrCannotDropDatabaseDoesntExist is returned when a DROP DATABASE is callend when a table is dropped that doesn't exist.
	ErrCannotDropDatabaseDoesntExist = errors.NewKind("can't drop database %s; database doesn't exist")

	// ErrInvalidConstraintFunctionsNotSupported is returned when a CONSTRAINT CHECK is called with a sub-function expression.
	ErrInvalidConstraintFunctionsNotSupported = errors.NewKind("Invalid constraint expression, functions not supported: %s")

	// ErrInvalidConstraintSubqueryNotSupported is returned when a CONSTRAINT CHECK is called with a sub-query expression.
	ErrInvalidConstraintSubqueryNotSupported = errors.NewKind("Invalid constraint expression, sub-queries not supported: %s")

	ErrCheckConstraintViolatedFmtStr = "Check constraint %q violated"

	// ErrCheckConstraintViolated is returned when a CONSTRAINT CHECK is called with a sub-query expression.
	ErrCheckConstraintViolated = errors.NewKind(ErrCheckConstraintViolatedFmtStr)

	// ErrColumnCountMismatch is returned when a view, derived table or common table expression has a declared column
	// list with a different number of columns than the schema of the table.
	ErrColumnCountMismatch = errors.NewKind("In definition of view, derived table or common table expression, SELECT list and column names list have different column counts")

	// ErrUuidUnableToParse is returned when a UUID is unable to be parsed.
	ErrUuidUnableToParse = errors.NewKind("unable to parse '%s' to UUID: %s")

	// ErrLoadDataCannotOpen is returned when a LOAD DATA operation is unable to open the file specified.
	ErrLoadDataCannotOpen = errors.NewKind("LOAD DATA is unable to open file: %s")

	// ErrLoadDataCharacterLength is returned when a symbol is of the wrong character length for a LOAD DATA operation.
	ErrLoadDataCharacterLength = errors.NewKind("%s must be 1 character long")

	// ErrSecureFileDirNotSet is returned when LOAD DATA INFILE is called but the secure_file_priv system variable is not set.
	ErrSecureFileDirNotSet = errors.NewKind("secure_file_priv needs to be set to a directory")

	// ErrJSONObjectAggNullKey is returned when JSON_OBJECTAGG is run on a table with NULL keys
	ErrJSONObjectAggNullKey = errors.NewKind("JSON documents may not contain NULL member names")

	// ErrDeclareOrderInvalid is returned when a DECLARE statement is at an invalid location.
	ErrDeclareOrderInvalid = errors.NewKind("DECLARE may only exist at the beginning of a BEGIN/END block")

	// ErrDeclareConditionNotFound is returned when SIGNAL/RESIGNAL references a non-existent DECLARE CONDITION.
	ErrDeclareConditionNotFound = errors.NewKind("condition %s does not exist")

	// ErrDeclareConditionDuplicate is returned when a DECLARE CONDITION statement with the same name was declared in the current scope.
	ErrDeclareConditionDuplicate = errors.NewKind("duplicate condition '%s'")

	// ErrSignalOnlySqlState is returned when SIGNAL/RESIGNAL references a DECLARE CONDITION for a MySQL error code.
	ErrSignalOnlySqlState = errors.NewKind("SIGNAL/RESIGNAL can only use a condition defined with SQLSTATE")

	// ErrExpectedSingleRow is returned when a subquery executed in normal queries or aggregation function returns
	// more than 1 row without an attached IN clause.
	ErrExpectedSingleRow = errors.NewKind("the subquery returned more than 1 row")

	// ErrSubqueryMultipleColumns is returned when an expression subquery returns
	// more than a single column.
	ErrSubqueryMultipleColumns = errors.NewKind(
		"operand contains more than one column",
	)
	// ErrUnknownConstraint is returned when a DROP CONSTRAINT statement refers to a constraint that doesn't exist
	ErrUnknownConstraint = errors.NewKind("Constraint %q does not exist")

	// ErrInsertIntoNonNullableDefaultNullColumn is returned when an INSERT excludes a field which is non-nullable and has no default/autoincrement.
	ErrInsertIntoNonNullableDefaultNullColumn = errors.NewKind("Field '%s' doesn't have a default value")

	// ErrAlterTableNotSupported is thrown when the table doesn't support ALTER TABLE statements
	ErrAlterTableNotSupported = errors.NewKind("table %s cannot be altered")

	// ErrPartitionNotFound is thrown when a partition key on a table is not found
	ErrPartitionNotFound = errors.NewKind("partition not found %q")

	// ErrInsertIntoNonNullableProvidedNull is called when a null value is inserted into a non-nullable column
	ErrInsertIntoNonNullableProvidedNull = errors.NewKind("column name '%v' is non-nullable but attempted to set a value of null")

	// ErrForeignKeyChildViolation is called when a rows is added but there is no parent row, and a foreign key constraint fails. Add the parent row first.
	ErrForeignKeyChildViolation = errors.NewKind("cannot add or update a child row - Foreign key violation on fk: `%s`, table: `%s`, referenced table: `%s`, key: `%s`")

	// ErrForeignKeyParentViolation is called when a parent row that is deleted has children, and a foreign key constraint fails. Delete the children first.
	ErrForeignKeyParentViolation = errors.NewKind("cannot delete or update a parent row - Foreign key violation on fk: `%s`, table: `%s`, referenced table: `%s`, key: `%s`")

	// ErrForeignKeyColumnCountMismatch is called when the declared column and referenced column counts do not match.
	ErrForeignKeyColumnCountMismatch = errors.NewKind("the foreign key must reference an equivalent number of columns")

	// ErrDuplicateEntry is returns when a duplicate entry is placed on an index such as a UNIQUE or a Primary Key.
	ErrDuplicateEntry = errors.NewKind("Duplicate entry for key '%s'")

	// ErrInvalidArgument is returned when an argument to a function is invalid.
	ErrInvalidArgument = errors.NewKind("Incorrect arguments to %s")

	// ErrSavepointDoesNotExist is returned when a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT statement references a
	// non-existent savepoint identifier
	ErrSavepointDoesNotExist = errors.NewKind("SAVEPOINT %s does not exist")

	// ErrTableCreatedNotFound is thrown when an integrator attempts to create a temporary tables without temporary table
	// support.
	ErrTemporaryTableNotSupported = errors.NewKind("database does not support temporary tables")

	// ErrInvalidSyntax is returned for syntax errors that aren't picked up by the parser, e.g. the wrong type of
	// expression used in part of statement.
	ErrInvalidSyntax = errors.NewKind("Invalid syntax: %s")

	// ErrTableCopyingNotSupported is returned when a table invokes the TableCopierDatabase interface's
	// CopyTableData method without supporting the interface
	ErrTableCopyingNotSupported = errors.NewKind("error: Table copying not supported")
)

func CastSQLError(err error) (*mysql.SQLError, bool) {
	if err == nil {
		return nil, true
	}
	if mysqlErr, ok := err.(*mysql.SQLError); ok {
		return mysqlErr, false
	}

	var code int
	var sqlState string = ""

	switch {
	case ErrTableNotFound.Is(err):
		code = mysql.ERNoSuchTable
	case ErrCannotCreateDatabaseExists.Is(err):
		code = mysql.ERDbCreateExists
	case ErrExpectedSingleRow.Is(err):
		code = mysql.ERSubqueryNo1Row
	case ErrSubqueryMultipleColumns.Is(err):
		code = mysql.EROperandColumns
	case ErrInsertIntoNonNullableProvidedNull.Is(err):
		code = mysql.ERBadNullError
	case ErrPrimaryKeyViolation.Is(err):
		code = mysql.ERDupEntry
	case ErrUniqueKeyViolation.Is(err):
		code = mysql.ERDupEntry
	case ErrPartitionNotFound.Is(err):
		code = 1526 // TODO: Needs to be added to vitess
	case ErrForeignKeyChildViolation.Is(err):
		code = mysql.ErNoReferencedRow2 // test with mysql returns 1452 vs 1216
	case ErrForeignKeyParentViolation.Is(err):
		code = mysql.ERRowIsReferenced2 // test with mysql returns 1451 vs 1215
	case ErrDuplicateEntry.Is(err):
		code = mysql.ERDupEntry
	case ErrInvalidJSONText.Is(err):
		code = 3141 // TODO: Needs to be added to vitess
	default:
		code = mysql.ERUnknownError
	}

	return mysql.NewSQLError(code, sqlState, err.Error()), false
}

type UniqueKeyError struct {
	keyStr   string
	IsPK     bool
	Existing Row
}

func NewUniqueKeyErr(keyStr string, isPK bool, existing Row) error {
	ue := UniqueKeyError{
		keyStr:   keyStr,
		IsPK:     isPK,
		Existing: existing,
	}

	if isPK {
		return ErrPrimaryKeyViolation.Wrap(ue)
	} else {
		return ErrUniqueKeyViolation.Wrap(ue)
	}
}

func (ue UniqueKeyError) Error() string {
	return fmt.Sprintf("%s", ue.keyStr)
}
