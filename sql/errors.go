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

	// ErrNotAuthorized is returned when the engine has been set to Read Only but a write operation was attempted.
	ErrNotAuthorized = errors.NewKind("not authorized")

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

	// ErrColumnExists is returned when an ALTER TABLE statement would create a duplicate column
	ErrColumnExists = errors.NewKind("Column %q already exists")

	// ErrCreateTableNotSupported is thrown when the database doesn't support table creation
	ErrCreateTableNotSupported = errors.NewKind("tables cannot be created on database %s")

	// ErrDropTableNotSupported is thrown when the database doesn't support dropping tables
	ErrDropTableNotSupported = errors.NewKind("tables cannot be dropped on database %s")

	// ErrRenameTableNotSupported is thrown when the database doesn't support renaming tables
	ErrRenameTableNotSupported = errors.NewKind("tables cannot be renamed on database %s")

	// ErrTableCreatedNotFound is thrown when a table is created from CREATE TABLE but cannot be found immediately afterward
	ErrTableCreatedNotFound = errors.NewKind("table was created but could not be found")

	// ErrUnexpectedRowLength is thrown when the obtained row has more columns than the schema
	ErrUnexpectedRowLength = errors.NewKind("expected %d values, got %d")

	// ErrInvalidChildrenNumber is returned when the WithChildren method of a
	// node or expression is called with an invalid number of arguments.
	ErrInvalidChildrenNumber = errors.NewKind("%T: invalid children number, got %d, expected %d")

	// ErrInvalidExpressionNumber is returned when the WithExpression method of a node
	// is called with an invalid number of arguments.
	ErrInvalidExpressionNumber = errors.NewKind("%T: invalid expression number, got %d, expected %d")

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

	// ErrDatabaseExists is returned when CREATE DATABASE attempts to create a database that already exists.
	ErrDatabaseExists = errors.NewKind("can't create database %s; database exists")

	// ErrInvalidConstraintFunctionNotSupported is returned when a CONSTRAINT CHECK is called with an unsupported function expression.
	ErrInvalidConstraintFunctionNotSupported = errors.NewKind("Invalid constraint expression, function not supported: %s")

	// ErrInvalidConstraintSubqueryNotSupported is returned when a CONSTRAINT CHECK is called with a sub-query expression.
	ErrInvalidConstraintSubqueryNotSupported = errors.NewKind("Invalid constraint expression, sub-queries not supported: %s")

	// ErrCheckConstraintViolated is returned when a CONSTRAINT CHECK is called with a sub-query expression.
	ErrCheckConstraintViolated = errors.NewKind("Check constraint %q violated")

	// ErrCheckConstraintInvalidatedByColumnAlter is returned when an alter column statement would invalidate a check constraint.
	ErrCheckConstraintInvalidatedByColumnAlter = errors.NewKind("can't alter column %q because it would invalidate check constraint %q")

	// ErrColumnCountMismatch is returned when a view, derived table or common table expression has a declared column
	// list with a different number of columns than the schema of the table.
	ErrColumnCountMismatch = errors.NewKind("In definition of view, derived table or common table expression, SELECT list and column names list have different column counts")

	// ErrUuidUnableToParse is returned when a UUID is unable to be parsed.
	ErrUuidUnableToParse = errors.NewKind("unable to parse '%s' to UUID: %s")

	// ErrLoadDataCannotOpen is returned when a LOAD DATA operation is unable to open the file specified.
	ErrLoadDataCannotOpen = errors.NewKind("LOAD DATA is unable to open file: %s")

	// ErrLoadDataCharacterLength is returned when a symbol is of the wrong character length for a LOAD DATA operation.
	ErrLoadDataCharacterLength = errors.NewKind("%s must be 1 character long")

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

	// ErrForeignKeyColumnTypeMismatch is returned when the declared column's type and referenced column's type do not match.
	ErrForeignKeyColumnTypeMismatch = errors.NewKind("column type mismatch on `%s` and `%s`")

	// ErrForeignKeyNotResolved is called when an add or update is attempted on a foreign key that has not been resolved yet.
	ErrForeignKeyNotResolved = errors.NewKind("cannot add or update a child row: a foreign key constraint fails (`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`))")

	// ErrNoForeignKeySupport is returned when the table does not support FOREIGN KEY operations.
	ErrNoForeignKeySupport = errors.NewKind("the table does not support foreign key operations: %s")

	// ErrForeignKeyMissingColumns is returned when an ALTER TABLE ADD FOREIGN KEY statement does not provide any columns
	ErrForeignKeyMissingColumns = errors.NewKind("cannot create a foreign key without columns")

	// ErrForeignKeyDropColumn is returned when attempting to drop a column used in a foreign key
	ErrForeignKeyDropColumn = errors.NewKind("cannot drop column `%s` as it is used in foreign key `%s`")

	// ErrForeignKeyDropTable is returned when attempting to drop a table used in a foreign key
	ErrForeignKeyDropTable = errors.NewKind("cannot drop table `%s` as it is referenced in foreign key `%s`")

	// ErrForeignKeyDropIndex is returned when attempting to drop an index used in a foreign key when there are no other
	// indexes which may be used in its place.
	ErrForeignKeyDropIndex = errors.NewKind("cannot drop index: `%s` is used by foreign key `%s`")

	// ErrForeignKeyDuplicateName is returned when a foreign key already exists with the given name.
	ErrForeignKeyDuplicateName = errors.NewKind("duplicate foreign key constraint name `%s`")

	// ErrAddForeignKeyDuplicateColumn is returned when an ALTER TABLE ADD FOREIGN KEY statement has the same column multiple times
	ErrAddForeignKeyDuplicateColumn = errors.NewKind("cannot have duplicates of columns in a foreign key: `%v`")

	// ErrTemporaryTablesForeignKeySupport is returned when a user tries to create a temporary table with a foreign key
	ErrTemporaryTablesForeignKeySupport = errors.NewKind("temporary tables do not support foreign keys")

	// ErrForeignKeyNotFound is returned when a foreign key was not found.
	ErrForeignKeyNotFound = errors.NewKind("foreign key `%s` was not found on the table `%s`")

	// ErrForeignKeySetDefault is returned when attempting to set a referential action as SET DEFAULT.
	ErrForeignKeySetDefault = errors.NewKind(`"SET DEFAULT" is not supported`)

	// ErrForeignKeySetNullNonNullable is returned when attempting to set a referential action as SET NULL when the
	// column is non-nullable.
	ErrForeignKeySetNullNonNullable = errors.NewKind("cannot use SET NULL as column `%s` is non-nullable")

	// ErrForeignKeyTypeChangeSetNull is returned when attempting to change a column's type to disallow NULL values when
	// a foreign key referential action is SET NULL.
	ErrForeignKeyTypeChangeSetNull = errors.NewKind("column `%s` must allow NULL values as foreign key `%s` has SET NULL")

	// ErrForeignKeyMissingReferenceIndex is returned when the referenced columns in a foreign key do not have an index.
	ErrForeignKeyMissingReferenceIndex = errors.NewKind("missing index for foreign key `%s` on the referenced table `%s`")

	// ErrForeignKeyTextBlob is returned when a TEXT or BLOB column is used in a foreign key, which are not valid types.
	ErrForeignKeyTextBlob = errors.NewKind("TEXT/BLOB are not valid types for foreign keys")

	// ErrForeignKeyTypeChange is returned when attempting to change the type of some column used in a foreign key.
	ErrForeignKeyTypeChange = errors.NewKind("unable to change type of column `%s` as it is used by foreign keys")

	// ErrDuplicateEntry is returns when a duplicate entry is placed on an index such as a UNIQUE or a Primary Key.
	ErrDuplicateEntry = errors.NewKind("Duplicate entry for key '%s'")

	// ErrInvalidArgument is returned when an argument to a function is invalid.
	ErrInvalidArgument = errors.NewKind("Invalid argument to %s")

	// ErrInvalidArgumentDetails is returned when the argument is invalid with details of a specific function
	ErrInvalidArgumentDetails = errors.NewKind("Invalid argument to %s: %s")

	// ErrSavepointDoesNotExist is returned when a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT statement references a
	// non-existent savepoint identifier
	ErrSavepointDoesNotExist = errors.NewKind("SAVEPOINT %s does not exist")

	// ErrTemporaryTableNotSupported is thrown when an integrator attempts to create a temporary tables without temporary table
	// support.
	ErrTemporaryTableNotSupported = errors.NewKind("database does not support temporary tables")

	// ErrInvalidSyntax is returned for syntax errors that aren't picked up by the parser, e.g. the wrong type of
	// expression used in part of statement.
	ErrInvalidSyntax = errors.NewKind("Invalid syntax: %s")

	// ErrTableCopyingNotSupported is returned when a table invokes the TableCopierDatabase interface's
	// CopyTableData method without supporting the interface
	ErrTableCopyingNotSupported = errors.NewKind("error: Table copying not supported")

	// ErrMultiplePrimaryKeysDefined is returned when a table invokes CreatePrimaryKey with a primary key already
	// defined.
	ErrMultiplePrimaryKeysDefined = errors.NewKind("error: Multiple primary keys defined")

	// ErrWrongAutoKey is returned when a table invokes DropPrimaryKey without first removing the auto increment property
	// (if it exists) on it.
	ErrWrongAutoKey = errors.NewKind("error: incorrect table definition: there can be only one auto column and it must be defined as a key")

	// ErrKeyColumnDoesNotExist is returned when a table invoked CreatePrimaryKey with a non-existent column.
	ErrKeyColumnDoesNotExist = errors.NewKind("error: key column '%s' doesn't exist in table")

	// ErrCantDropFieldOrKey is returned when a table invokes DropPrimaryKey on a keyless table.
	ErrCantDropFieldOrKey = errors.NewKind("error: can't drop '%s'; check that column/key exists")

	// ErrCantDropIndex is return when a table can't drop an index due to a foreign key relationship.
	ErrCantDropIndex = errors.NewKind("error: can't drop index '%s': needed in a foreign key constraint")

	// ErrImmutableDatabaseProvider is returned when attempting to edit an immutable database databaseProvider.
	ErrImmutableDatabaseProvider = errors.NewKind("error: can't modify database databaseProvider")

	// ErrInvalidValue is returned when a given value does not match what is expected.
	ErrInvalidValue = errors.NewKind(`error: '%v' is not a valid value for '%v'`)

	// ErrInvalidValueType is returned when a given value's type does not match what is expected.
	ErrInvalidValueType = errors.NewKind(`error: '%T' is not a valid value type for '%v'`)

	// ErrFunctionNotFound is thrown when a function is not found
	ErrFunctionNotFound = errors.NewKind("function: '%s' not found")

	// ErrTableFunctionNotFound is thrown when a table function is not found
	ErrTableFunctionNotFound = errors.NewKind("table function: '%s' not found")

	// ErrInvalidArgumentNumber is returned when the number of arguments to call a
	// function is different from the function arity.
	ErrInvalidArgumentNumber = errors.NewKind("function '%s' expected %v arguments, %v received")

	// ErrDatabaseNotFound is thrown when a database is not found
	ErrDatabaseNotFound = errors.NewKind("database not found: %s")

	// ErrNoDatabaseSelected is thrown when a database is not selected and the query requires one
	ErrNoDatabaseSelected = errors.NewKind("no database selected")

	// ErrAsOfNotSupported is thrown when an AS OF query is run on a database that can't support it
	ErrAsOfNotSupported = errors.NewKind("AS OF not supported for database %s")

	// ErrIncompatibleAsOf is thrown when an AS OF clause is used in an incompatible manner, such as when using an AS OF
	// expression with a view when the view definition has its own AS OF expressions.
	ErrIncompatibleAsOf = errors.NewKind("incompatible use of AS OF: %s")

	// ErrPidAlreadyUsed is returned when the pid is already registered.
	ErrPidAlreadyUsed = errors.NewKind("pid %d is already in use")

	// ErrInvalidOperandColumns is returned when the columns in the left
	// operand and the elements of the right operand don't match. Also
	// returned for invalid number of columns in projections, filters,
	// joins, etc.
	ErrInvalidOperandColumns = errors.NewKind("operand should have %d columns, but has %d")

	// ErrReadOnlyTransaction is returned when a write query is executed in a READ ONLY transaction.
	ErrReadOnlyTransaction = errors.NewKind("cannot execute statement in a READ ONLY transaction")

	// ErrExistingView is returned when a CREATE VIEW statement uses a name that already exists
	ErrExistingView = errors.NewKind("the view %s.%s already exists")

	// ErrViewDoesNotExist is returned when a DROP VIEW statement drops a view that does not exist
	ErrViewDoesNotExist = errors.NewKind("the view %s.%s does not exist")

	// ErrSessionDoesNotSupportPersistence is thrown when a feature is not already supported
	ErrSessionDoesNotSupportPersistence = errors.NewKind("session does not support persistence")

	// ErrInvalidGISData is thrown when a "ST_<spatial_type>FromText" function receives a malformed string
	ErrInvalidGISData = errors.NewKind("invalid GIS data provided to function %s")

	// ErrIllegalGISValue is thrown when a spatial type constructor receives a non-geometric when one should be provided
	ErrIllegalGISValue = errors.NewKind("illegal non geometric '%v' value found during parsing")

	// ErrUnsupportedSyntax is returned when syntax that parses correctly is not supported
	ErrUnsupportedSyntax = errors.NewKind("unsupported syntax: %s")

	// ErrInvalidSQLValType is returned when a SQL value is of the incorrect type during parsing
	ErrInvalidSQLValType = errors.NewKind("invalid SQLVal of type: %d")

	// ErrInvalidIndexPrefix is returned when an index prefix is outside the accepted range
	ErrInvalidIndexPrefix = errors.NewKind("invalid index prefix: %v")

	// ErrUnknownIndexColumn is returned when a column in an index is not in the table
	ErrUnknownIndexColumn = errors.NewKind("unknown column: '%s' in index '%s'")

	// ErrInvalidAutoIncCols is returned when an auto_increment column cannot be applied
	ErrInvalidAutoIncCols = errors.NewKind("there can be only one auto_increment column and it must be defined as a key")

	// ErrUnknownConstraintDefinition is returned when an unknown constraint type is used
	ErrUnknownConstraintDefinition = errors.NewKind("unknown constraint definition: %s, %T")

	// ErrInvalidCheckConstraint is returned when a check constraint is defined incorrectly
	ErrInvalidCheckConstraint = errors.NewKind("invalid constraint definition: %s")

	// ErrUserCreationFailure is returned when attempting to create a user and it fails for any reason.
	ErrUserCreationFailure = errors.NewKind("Operation CREATE USER failed for %s")

	// ErrRoleCreationFailure is returned when attempting to create a role and it fails for any reason.
	ErrRoleCreationFailure = errors.NewKind("Operation CREATE ROLE failed for %s")

	// ErrUserDeletionFailure is returned when attempting to create a user and it fails for any reason.
	ErrUserDeletionFailure = errors.NewKind("Operation DROP USER failed for %s")

	// ErrRoleDeletionFailure is returned when attempting to create a role and it fails for any reason.
	ErrRoleDeletionFailure = errors.NewKind("Operation DROP ROLE failed for %s")

	// ErrDatabaseAccessDeniedForUser is returned when attempting to access a database that the user does not have
	// permission for, regardless of whether that database actually exists.
	ErrDatabaseAccessDeniedForUser = errors.NewKind("Access denied for user %s to database '%s'")

	// ErrTableAccessDeniedForUser is returned when attempting to access a table that the user does not have permission
	// for, regardless of whether that table actually exists.
	ErrTableAccessDeniedForUser = errors.NewKind("Access denied for user %s to table '%s'")

	// ErrPrivilegeCheckFailed is returned when a user does not have the correct privileges to perform an operation.
	ErrPrivilegeCheckFailed = errors.NewKind("command denied to user %s")

	// ErrGrantUserDoesNotExist is returned when a user does not exist when attempting to grant them privileges.
	ErrGrantUserDoesNotExist = errors.NewKind("You are not allowed to create a user with GRANT")

	// ErrRevokeUserDoesNotExist is returned when a user does not exist when attempting to revoke privileges from them.
	ErrRevokeUserDoesNotExist = errors.NewKind("There is no such grant defined for user '%s' on host '%s'")

	// ErrGrantRevokeRoleDoesNotExist is returned when a user or role does not exist when attempting to grant or revoke roles.
	ErrGrantRevokeRoleDoesNotExist = errors.NewKind("Unknown authorization ID %s")

	// ErrShowGrantsUserDoesNotExist is returned when a user does not exist when attempting to show their grants.
	ErrShowGrantsUserDoesNotExist = errors.NewKind("There is no such grant defined for user '%s' on host '%s'")

	// ErrInvalidRecursiveCteUnion is returned when a recursive CTE is not a UNION or UNION ALL node.
	ErrInvalidRecursiveCteUnion = errors.NewKind("recursive cte top-level query must be a union; found: %v")

	// ErrInvalidRecursiveCteInitialQuery is returned when the recursive CTE base clause is not supported.
	ErrInvalidRecursiveCteInitialQuery = errors.NewKind("recursive cte initial query must be non-recursive projection; found: %v")

	// ErrInvalidRecursiveCteRecursiveQuery is returned when the recursive CTE recursion clause is not supported.
	ErrInvalidRecursiveCteRecursiveQuery = errors.NewKind("recursive cte recursive query must be a recursive projection; found: %v")

	// ErrCteRecursionLimitExceeded is returned when a recursive CTE's execution stack depth exceeds the static limit.
	ErrCteRecursionLimitExceeded = errors.NewKind("WITH RECURSIVE iteration limit exceeded")

	// ErrGrantRevokeIllegalPrivilege is returned when a GRANT or REVOKE statement is malformed, or attempts to use privilege incorrectly.
	ErrGrantRevokeIllegalPrivilege = errors.NewKind("Illegal GRANT/REVOKE command")

	// ErrInvalidWindowInheritance is returned when a window and its dependency contains conflicting partitioning, ordering, or framing clauses
	ErrInvalidWindowInheritance = errors.NewKind("window '%s' cannot inherit '%s' since %s")

	// ErrCircularWindowInheritance is returned when a WINDOW clause has a circular dependency
	ErrCircularWindowInheritance = errors.NewKind("there is a circularity in the window dependency graph")

	// ErrCannotCopyWindowFrame is returned when we inherit a window frame with a frame clause (replacement without parenthesis is OK)
	ErrCannotCopyWindowFrame = errors.NewKind("cannot copy window '%s' because it has a frame clause")

	// ErrUnknownWindowName is returned when an over by clause references an unknown window definition
	ErrUnknownWindowName = errors.NewKind("named window not found: '%s'")

	// ErrUnexpectedNilRow is returned when an invalid operation is applied to an empty row
	ErrUnexpectedNilRow = errors.NewKind("unexpected nil row")
)

func CastSQLError(err error) (*mysql.SQLError, error, bool) {
	if err == nil {
		return nil, nil, true
	}
	if mysqlErr, ok := err.(*mysql.SQLError); ok {
		return mysqlErr, nil, false
	}

	var code int
	var sqlState string = ""

	if w, ok := err.(WrappedInsertError); ok {
		return CastSQLError(w.Cause)
	}

	switch {
	case ErrTableNotFound.Is(err):
		code = mysql.ERNoSuchTable
	case ErrDatabaseExists.Is(err):
		code = mysql.ERDbCreateExists
	case ErrExpectedSingleRow.Is(err):
		code = mysql.ERSubqueryNo1Row
	case ErrInvalidOperandColumns.Is(err):
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
	case ErrMultiplePrimaryKeysDefined.Is(err):
		code = mysql.ERMultiplePriKey
	case ErrWrongAutoKey.Is(err):
		code = mysql.ERWrongAutoKey
	case ErrKeyColumnDoesNotExist.Is(err):
		code = mysql.ERKeyColumnDoesNotExist
	case ErrCantDropFieldOrKey.Is(err):
		code = mysql.ERCantDropFieldOrKey
	case ErrReadOnlyTransaction.Is(err):
		code = 1792 // TODO: Needs to be added to vitess
	case ErrCantDropIndex.Is(err):
		code = 1553 // TODO: Needs to be added to vitess
	case ErrInvalidValue.Is(err):
		code = mysql.ERTruncatedWrongValueForField
	default:
		code = mysql.ERUnknownError
	}

	return mysql.NewSQLError(code, sqlState, err.Error()), err, false // return the original error as well
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

type WrappedInsertError struct {
	OffendingRow Row
	Cause        error
}

func NewWrappedInsertError(r Row, err error) WrappedInsertError {
	return WrappedInsertError{
		OffendingRow: r,
		Cause:        err,
	}
}

func (w WrappedInsertError) Error() string {
	return w.Cause.Error()
}

type ErrInsertIgnore struct {
	OffendingRow Row
}

func NewErrInsertIgnore(row Row) ErrInsertIgnore {
	return ErrInsertIgnore{OffendingRow: row}
}

func (e ErrInsertIgnore) Error() string {
	return "Insert ignore error shoudl never be printed"
}
