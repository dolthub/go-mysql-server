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
	"math"
	"strconv"
	"strings"
	"time"
)

// Nameable is something that has a name.
type Nameable interface {
	// Name returns the name.
	Name() string
}

// Tableable is something that has a table.
type Tableable interface {
	// Table returns the table name.
	Table() string
}

// Resolvable is something that can be resolved or not.
type Resolvable interface {
	// Resolved returns whether the node is resolved.
	Resolved() bool
}

// TransformNodeFunc is a function that given a node will return that node
// as is or transformed along with an error, if any.
type TransformNodeFunc func(Node) (Node, error)

// TransformExprFunc is a function that given an expression will return that
// expression as is or transformed along with an error, if any.
type TransformExprFunc func(Expression) (Expression, error)

// Expression is a combination of one or more SQL expressions.
type Expression interface {
	Resolvable
	fmt.Stringer
	// Type returns the expression type.
	Type() Type
	// IsNullable returns whether the expression can be null.
	IsNullable() bool
	// Eval evaluates the given row and returns a result.
	Eval(ctx *Context, row Row) (interface{}, error)
	// Children returns the children expressions of this expression.
	Children() []Expression
	// WithChildren returns a copy of the expression with children replaced.
	// It will return an error if the number of children is different than
	// the current number of children. They must be given in the same order
	// as they are returned by Children.
	WithChildren(ctx *Context, children ...Expression) (Expression, error)
}

// FunctionExpression is an Expression that represents a function.
type FunctionExpression interface {
	Expression
	FunctionName() string
}

// NonDeterministicExpression allows a way for expressions to declare that they are non-deterministic, which will
// signal the engine to not cache their results when this would otherwise appear to be safe.
type NonDeterministicExpression interface {
	Expression
	// IsNonDeterministic returns whether this expression returns a non-deterministic result. An expression is
	// non-deterministic if it can return different results on subsequent evaluations.
	IsNonDeterministic() bool
}

// Aggregation implements an aggregation expression, where an
// aggregation buffer is created for each grouping (NewBuffer) and rows in the
// grouping are fed to the buffer (Update). Multiple buffers can be merged
// (Merge), making partial aggregations possible.
// Note that Eval must be called with the final aggregation buffer in order to
// get the final result.
type Aggregation interface {
	Expression
	// NewBuffer creates a new aggregation buffer and returns it as a Row.
	NewBuffer() Row
	// Update updates the given buffer with the given row.
	Update(ctx *Context, buffer, row Row) error
	// Merge merges a partial buffer into a global one.
	Merge(ctx *Context, buffer, partial Row) error
}

// WindowAggregation implements a window aggregation expression. A WindowAggregation is similar to an Aggregation,
// except that it returns a result row for every input row, as opposed to as single for the entire result set. Every
// WindowAggregation is expected to track its input rows in the order received, and to return the value for the row
// index given on demand.
type WindowAggregation interface {
	Expression
	// Window returns this expression's window
	Window() *Window
	// WithWindow returns a version of this window aggregation with the window given
	WithWindow(window *Window) (WindowAggregation, error)
	// NewBuffer creates a new buffer and returns it as a Row. This buffer will be provided for all further operations.
	NewBuffer() Row
	// Add updates the aggregation with the input row given. Implementors must keep track of rows added in order so
	// that they can later be retrieved by EvalRow(int)
	Add(ctx *Context, buffer, row Row) error
	// Finish gives aggregations that need to final computation once all rows have been added (like sorting their
	// inputs) a chance to do before iteration begins
	Finish(ctx *Context, buffer Row) error
	// EvalRow returns the value of the expression for the row with the index given
	EvalRow(i int, buffer Row) (interface{}, error)
}

// Node is a node in the execution plan tree.
type Node interface {
	Resolvable
	fmt.Stringer
	// Schema of the node.
	Schema() Schema
	// Children nodes.
	Children() []Node
	// RowIter produces a row iterator from this node. The current row being evaluated is provided, as well the context
	// of the query.
	RowIter(ctx *Context, row Row) (RowIter, error)
	// WithChildren returns a copy of the node with children replaced.
	// It will return an error if the number of children is different than
	// the current number of children. They must be given in the same order
	// as they are returned by Children.
	WithChildren(...Node) (Node, error)
}

// CommentedNode allows comments to be set and retrieved on it
type CommentedNode interface {
	Node
	WithComment(string) Node
	Comment() string
}

// DebugStringer is shared by implementors of Node and Expression, and is used for debugging the analyzer. It allows
// a node or expression to be printed in greater detail than its default String() representation.
type DebugStringer interface {
	// DebugString prints a debug string of the node in question.
	DebugString() string
}

// DebugString returns a debug string for the Node or Expression given.
func DebugString(nodeOrExpression interface{}) string {
	if ds, ok := nodeOrExpression.(DebugStringer); ok {
		return ds.DebugString()
	}
	if s, ok := nodeOrExpression.(fmt.Stringer); ok {
		return s.String()
	}
	panic(fmt.Sprintf("Expected sql.DebugString or fmt.Stringer for %T", nodeOrExpression))
}

// OpaqueNode is a node that doesn't allow transformations to its children and
// acts a a black box.
type OpaqueNode interface {
	Node
	// Opaque reports whether the node is opaque or not.
	Opaque() bool
}

// Expressioner is a node that contains expressions.
type Expressioner interface {
	// Expressions returns the list of expressions contained by the node.
	Expressions() []Expression
	// WithExpressions returns a copy of the node with expressions replaced.
	// It will return an error if the number of expressions is different than
	// the current number of expressions. They must be given in the same order
	// as they are returned by Expressions.
	WithExpressions(...Expression) (Node, error)
}

// Databaser is a node that contains a reference to a database.
type Databaser interface {
	// Database the current database.
	Database() Database
	// WithDatabase returns a new node instance with the database replaced with
	// the one given as parameter.
	WithDatabase(Database) (Node, error)
}

// Partition represents a partition from a SQL table.
type Partition interface {
	Key() []byte
}

// PartitionIter is an iterator that retrieves partitions.
type PartitionIter interface {
	Closer
	Next() (Partition, error)
}

// Table represents the backend of a SQL table.
type Table interface {
	Nameable
	String() string
	Schema() Schema
	Partitions(*Context) (PartitionIter, error)
	PartitionRows(*Context, Partition) (RowIter, error)
}

type TemporaryTable interface {
	IsTemporary() bool
}

// TableWrapper is a node that wraps the real table. This is needed because
// wrappers cannot implement some methods the table may implement.
type TableWrapper interface {
	// Underlying returns the underlying table.
	Underlying() Table
}

// PartitionCounter can return the number of partitions.
type PartitionCounter interface {
	// PartitionCount returns the number of partitions.
	PartitionCount(*Context) (int64, error)
}

// FilteredTable is a table that can produce a specific RowIter
// that's more optimized given the filters.
type FilteredTable interface {
	Table
	HandledFilters(filters []Expression) []Expression
	WithFilters(ctx *Context, filters []Expression) Table
}

// ProjectedTable is a table that can produce a specific RowIter
// that's more optimized given the columns that are projected.
type ProjectedTable interface {
	Table
	WithProjection(colNames []string) Table
}

// StatisticsTable is a table that can provide information about its number of rows and other facts to improve query
// planning performance.
type StatisticsTable interface {
	Table
	// NumRows returns the unfiltered count of rows contained in the table
	NumRows(*Context) (uint64, error)
	// DataLength returns the length of the data file (varies by engine).
	DataLength(ctx *Context) (uint64, error)
}

// IndexUsing is the desired storage type.
type IndexUsing byte

const (
	IndexUsing_Default IndexUsing = iota
	IndexUsing_BTree
	IndexUsing_Hash
)

// IndexConstraint represents any constraints that should be applied to the index.
type IndexConstraint byte

const (
	IndexConstraint_None IndexConstraint = iota
	IndexConstraint_Unique
	IndexConstraint_Fulltext
	IndexConstraint_Spatial
)

// IndexColumn is the column by which to add to an index.
type IndexColumn struct {
	Name string
	// Length represents the index prefix length. If zero, then no length was specified.
	Length int64
}

// IndexedTable represents a table that has one or more native indexes on its columns, and can use those indexes to
// speed up execution of queries that reference those columns. Unlike DriverIndexableTable, IndexedTable doesn't need a
// separate index driver to function.
type IndexedTable interface {
	IndexAddressableTable
	// GetIndexes returns all indexes on this table.
	GetIndexes(ctx *Context) ([]Index, error)
}

// IndexAddressableTable is a table that can restrict its row iteration to only the rows that match a given index
// lookup.
type IndexAddressableTable interface {
	Table
	// WithIndexLookup returns a version of the table that will return only the rows specified by the given IndexLookup,
	// which was in turn created by a call to Index.Get() for a set of keys for this table.
	WithIndexLookup(IndexLookup) Table
}

// IndexAlterableTable represents a table that supports index modification operations.
type IndexAlterableTable interface {
	Table
	// CreateIndex creates an index for this table, using the provided parameters.
	// Returns an error if the index name already exists, or an index with the same columns already exists.
	CreateIndex(ctx *Context, indexName string, using IndexUsing, constraint IndexConstraint, columns []IndexColumn, comment string) error
	// DropIndex removes an index from this table, if it exists.
	// Returns an error if the removal failed or the index does not exist.
	DropIndex(ctx *Context, indexName string) error
	// RenameIndex renames an existing index to another name that is not already taken by another index on this table.
	RenameIndex(ctx *Context, fromIndexName string, toIndexName string) error
}

// ForeignKeyTable is a table that can declare its foreign key constraints.
type ForeignKeyTable interface {
	Table
	// GetForeignKeys returns the foreign key constraints on this table.
	GetForeignKeys(ctx *Context) ([]ForeignKeyConstraint, error)
}

// ForeignKeyAlterableTable represents a table that supports foreign key modification operations.
type ForeignKeyAlterableTable interface {
	Table
	// CreateForeignKey creates an index for this table, using the provided parameters.
	// Returns an error if the foreign key name already exists.
	CreateForeignKey(ctx *Context, fkName string, columns []string, referencedTable string, referencedColumns []string,
		onUpdate, onDelete ForeignKeyReferenceOption) error
	// DropForeignKey removes a foreign key from the database.
	DropForeignKey(ctx *Context, fkName string) error
}

// CheckTable is a table that can declare its check constraints.
type CheckTable interface {
	Table
	// GetChecks returns the check constraints on this table.
	GetChecks(ctx *Context) ([]CheckDefinition, error)
}

// CheckAlterableTable represents a table that supports check constraints.
type CheckAlterableTable interface {
	Table
	// CreateCheck creates an check constraint for this table, using the provided parameters.
	// Returns an error if the constraint name already exists.
	CreateCheck(ctx *Context, check *CheckDefinition) error
	// DropCheck removes a check constraint from the database.
	DropCheck(ctx *Context, chName string) error
}

// TableEditor is the base interface for sub interfaces that can update rows in a table during an INSERT, REPLACE,
// UPDATE, or DELETE statement.
type TableEditor interface {
	// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
	// in some way that it may be returned to in the case of an error.
	StatementBegin(ctx *Context)
	// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
	// should be discarded.
	DiscardChanges(ctx *Context, errorEncountered error) error
	// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
	// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
	StatementComplete(ctx *Context) error
}

// InsertableTable is a table that can process insertion of new rows.
type InsertableTable interface {
	Table
	// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
	// inserted, and will end with a call to Close() to finalize the insert operation.
	Inserter(*Context) RowInserter
}

// RowInserter is an insert cursor that can insert one or more values to a table.
type RowInserter interface {
	TableEditor
	// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
	// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
	// is called.
	Insert(*Context, Row) error
	// Close finalizes the insert operation, persisting its result.
	Closer
}

// DeleteableTable is a table that can process the deletion of rows
type DeletableTable interface {
	Table
	// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
	// and will end with a call to Close() to finalize the delete operation.
	Deleter(*Context) RowDeleter
}

// RowDeleter is a delete cursor that can delete one or more rows from a table.
type RowDeleter interface {
	TableEditor
	// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
	// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
	// Close is called.
	Delete(*Context, Row) error
	// Close finalizes the delete operation, persisting the result.
	Closer
}

// TruncateableTable is a table that can process the deletion of all rows.
type TruncateableTable interface {
	Table
	// Truncate removes all rows from the table. If the table also implements DeletableTable and it is determined that
	// truncate would be equivalent to a DELETE which spans the entire table, then this function will be called instead.
	// Returns the number of rows that were removed.
	Truncate(*Context) (int, error)
}

// AutoIncrementTable is a table that supports AUTO_INCREMENT.
// Getter and Setter methods access the table's AUTO_INCREMENT
// sequence. These methods should only be used for tables with
// and AUTO_INCREMENT column in their schema.
type AutoIncrementTable interface {
	Table
	// PeekNextAutoIncrementValue returns the expected next AUTO_INCREMENT value but does not require
	// implementations to update their state.
	PeekNextAutoIncrementValue(*Context) (interface{}, error)
	// GetNextAutoIncrementValue gets the next AUTO_INCREMENT value. In the case that a table with an autoincrement
	// column is passed in a row with the autoinc column failed, the next auto increment value must
	// update its internal state accordingly and use the insert val at runtime.
	//Implementations are responsible for updating their state to provide the correct values.
	GetNextAutoIncrementValue(ctx *Context, insertVal interface{}) (interface{}, error)
	// AutoIncrementSetter returns an AutoIncrementSetter.
	AutoIncrementSetter(*Context) AutoIncrementSetter
}

var ErrNoAutoIncrementCol = fmt.Errorf("this table has no AUTO_INCREMENT columns")

// AutoIncrementSetter provides support for altering a table's
// AUTO_INCREMENT sequence, eg 'ALTER TABLE t AUTO_INCREMENT = 10;'
type AutoIncrementSetter interface {
	// SetAutoIncrementValue sets a new AUTO_INCREMENT value.
	SetAutoIncrementValue(*Context, interface{}) error
	// Close finalizes the set operation, persisting the result.
	Closer
}

type Closer interface {
	Close(*Context) error
}

// RowReplacer is a combination of RowDeleter and RowInserter.
// TODO: We can't embed those interfaces because go 1.13 doesn't allow for overlapping interfaces (they both declare
//  Close). Go 1.14 fixes this problem, but we aren't ready to drop support for 1.13 yet.
type RowReplacer interface {
	TableEditor
	// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
	// for the replace operation, which may involve many rows. After all rows in an operation have been processed, Close
	// is called.
	Insert(*Context, Row) error
	// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
	// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
	// Close is called.
	Delete(*Context, Row) error
	// Close finalizes the replace operation, persisting the result.
	Closer
}

// Replacer allows rows to be replaced through a Delete (if applicable) then Insert.
type ReplaceableTable interface {
	Table
	// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
	// for each row, followed by a call to Close() when all rows have been processed.
	Replacer(ctx *Context) RowReplacer
}

// UpdateableTable is a table that can process updates of existing rows via update statements.
type UpdatableTable interface {
	Table
	// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
	// updated, followed by a call to Close() when all rows have been processed.
	Updater(ctx *Context) RowUpdater
}

// RowUpdater is an update cursor that can update one or more rows in a table.
type RowUpdater interface {
	TableEditor
	// Update the given row. Provides both the old and new rows.
	Update(ctx *Context, old Row, new Row) error
	// Close finalizes the update operation, persisting the result.
	Closer
}

// DatabaseProvider is a collection of Database.
type DatabaseProvider interface {
	// Database gets a Database from the provider.
	Database(name string) (Database, error)

	// HasDatabase checks if the Database exists in the provider.
	HasDatabase(name string) bool

	// AllDatabases returns a slice of all Databases in the provider.
	AllDatabases() []Database
}

type MutableDatabaseProvider interface {
	DatabaseProvider

	// AddDatabase adds a new Database to the provider's collection.
	AddDatabase(db Database)

	// DropDatabase removes a database from the providers's collection.
	DropDatabase(name string)
}

// Database represents the database.
type Database interface {
	Nameable

	// GetTableInsensitive retrieves a table by its case insensitive name.  Implementations should look for exact
	// (case-sensitive matches) first.  If no exact matches are found then any table matching the name case insensitively
	// should be returned.  If there is more than one table that matches a case insensitive comparison the resolution
	// strategy is not defined.
	GetTableInsensitive(ctx *Context, tblName string) (Table, bool, error)

	// GetTableNames returns the table names of every table in the database. It does not return the names of temporary
	// tables
	GetTableNames(ctx *Context) ([]string, error)
}

type ReadOnlyDatabase interface {
	Database

	// IsReadOnly returns whether this database is read-only.
	IsReadOnly() bool
}

// VersionedDatabase is a Database that can return tables as they existed at different points in time. The engine
// supports queries on historical table data via the AS OF construct introduced in SQL 2011.
type VersionedDatabase interface {
	Database

	// GetTableInsensitiveAsOf retrieves a table by its case-insensitive name with the same semantics as
	// Database.GetTableInsensitive, but at a particular revision of the database. Implementors must choose which types
	// of expressions to accept as revision names.
	GetTableInsensitiveAsOf(ctx *Context, tblName string, asOf interface{}) (Table, bool, error)

	// GetTableNamesAsOf returns the table names of every table in the database as of the revision given. Implementors
	// must choose which types of expressions to accept as revision names.
	GetTableNamesAsOf(ctx *Context, asOf interface{}) ([]string, error)
}

// Transaction is an opaque type implemented by an integrator to record necessary information at the start of a
// transaction. Active transactions will be recorded in the session.
type Transaction interface {
	fmt.Stringer
}

// TransactionDatabase is a Database that can BEGIN, ROLLBACK and COMMIT transactions, as well as create SAVEPOINTS and
// restore to them.
type TransactionDatabase interface {
	Database

	// StartTransaction starts a new transaction and returns it
	StartTransaction(ctx *Context) (Transaction, error)

	// CommitTransaction commits the transaction given
	CommitTransaction(ctx *Context, tx Transaction) error

	// Rollback restores the database to the state recorded in the transaction given
	Rollback(ctx *Context, transaction Transaction) error

	// CreateSavepoint records a savepoint for the transaction given with the name given. If the name is already in use
	// for this transaction, the new savepoint replaces the old one.
	CreateSavepoint(ctx *Context, transaction Transaction, name string) error

	// RollbackToSavepoint restores the database to the state named by the savepoint
	RollbackToSavepoint(ctx *Context, transaction Transaction, name string) error

	// ReleaseSavepoint removes the savepoint named from the transaction given
	ReleaseSavepoint(ctx *Context, transaction Transaction, name string) error
}

// TriggerDefinition defines a trigger. Integrators are not expected to parse or understand the trigger definitions,
// but must store and return them when asked.
type TriggerDefinition struct {
	Name            string // The name of this trigger. Trigger names in a database are unique.
	CreateStatement string // The text of the statement to create this trigger.
}

// TriggerDatabase is a Database that supports the creation and execution of triggers. The engine handles all parsing
// and execution logic for triggers. Integrators are not expected to parse or understand the trigger definitions, but
// must store and return them when asked.
type TriggerDatabase interface {
	Database

	// GetTriggers returns all trigger definitions for the database
	GetTriggers(ctx *Context) ([]TriggerDefinition, error)

	// CreateTrigger is called when an integrator is asked to create a trigger. The create trigger statement string is
	// provided to store, along with the name of the trigger.
	CreateTrigger(ctx *Context, definition TriggerDefinition) error

	// DropTrigger is called when a trigger should no longer be stored. The name has already been validated.
	// Returns ErrTriggerDoesNotExist if the trigger was not found.
	DropTrigger(ctx *Context, name string) error
}

// TemporaryTableDatabase is a database that can query the session (which manages the temporary table state) to
// retrieve the name of all temporary tables.
type TemporaryTableDatabase interface {
	GetAllTemporaryTables(ctx *Context) ([]Table, error)
}

// TableCopierDatabase is a database that can copy a source table's data (without preserving indexed, fks, etc.) into
// another destination table.
type TableCopierDatabase interface {
	// CopyTableData copies the sourceTable data to the destinationTable and returns the number of rows copied.
	CopyTableData(ctx *Context, sourceTable string, destinationTable string) (uint64, error)
}

// GetTableInsensitive implements a case insensitive map lookup for tables keyed off of the table name.
// Looks for exact matches first.  If no exact matches are found then any table matching the name case insensitively
// should be returned.  If there is more than one table that matches a case insensitive comparison the resolution
// strategy is not defined.
func GetTableInsensitive(tblName string, tables map[string]Table) (Table, bool) {
	if tbl, ok := tables[tblName]; ok {
		return tbl, true
	}

	lwrName := strings.ToLower(tblName)

	for k, tbl := range tables {
		if lwrName == strings.ToLower(k) {
			return tbl, true
		}
	}

	return nil, false
}

// GetTableNameInsensitive implements a case insensitive search of a slice of table names. It looks for exact matches
// first.  If no exact matches are found then any table matching the name case insensitively should be returned.  If
// there is more than one table that matches a case insensitive comparison the resolution strategy is not defined.
func GetTableNameInsensitive(tblName string, tableNames []string) (string, bool) {
	for _, name := range tableNames {
		if tblName == name {
			return name, true
		}
	}

	lwrName := strings.ToLower(tblName)

	for _, name := range tableNames {
		if lwrName == strings.ToLower(name) {
			return name, true
		}
	}

	return "", false
}

// DBTableIter iterates over all tables returned by db.GetTableNames() calling cb for each one until all tables have
// been processed, or an error is returned from the callback, or the cont flag is false when returned from the callback.
func DBTableIter(ctx *Context, db Database, cb func(Table) (cont bool, err error)) error {
	names, err := db.GetTableNames(ctx)

	if err != nil {
		return err
	}

	for _, name := range names {
		tbl, ok, err := db.GetTableInsensitive(ctx, name)

		if err != nil {
			return err
		} else if !ok {
			return ErrTableNotFound.New(name)
		}

		cont, err := cb(tbl)

		if err != nil {
			return err
		}

		if !cont {
			break
		}
	}

	return nil
}

// TableCreator should be implemented by databases that can create new tables.
type TableCreator interface {
	// Creates the table with the given name and schema. If a table with that name already exists, must return
	// sql.ErrTableAlreadyExists.
	CreateTable(ctx *Context, name string, schema Schema) error
}

// TemporaryTableCreator is a database that can create temporary tables that persist only as long as the session.
// Note that temporary tables with the same name as persisted tables take precedence in most SQL operations.
type TemporaryTableCreator interface {
	Database
	// Creates the table with the given name and schema. If a temporary table with that name already exists, must
	// return sql.ErrTableAlreadyExists
	CreateTemporaryTable(ctx *Context, name string, schema Schema) error
}

// ViewCreator should be implemented by databases that want to know when a view
// has been created.
type ViewCreator interface {
	// Notifies the database that a view with the given name and the given
	// select statement as been created.
	CreateView(ctx *Context, name string, selectStatement string) error
}

// TableDropper should be implemented by databases that can drop tables.
type TableDropper interface {
	DropTable(ctx *Context, name string) error
}

// ViewDropper should be implemented by databases that want to know when a view
// is dropped.
type ViewDropper interface {
	DropView(ctx *Context, name string) error
}

// TableRenamer should be implemented by databases that can rename tables.
type TableRenamer interface {
	// Renames a table from oldName to newName as given. If a table with newName already exists, must return
	// sql.ErrTableAlreadyExists.
	RenameTable(ctx *Context, oldName, newName string) error
}

// ColumnOrder is used in ALTER TABLE statements to change the order of inserted / modified columns.
type ColumnOrder struct {
	First       bool   // True if this column should come first
	AfterColumn string // Set to the name of the column after which this column should appear
}

// AlterableTable should be implemented by tables that can receive ALTER TABLE statements to modify their schemas.
type AlterableTable interface {
	Table
	// AddColumn adds a column to this table as given. If non-nil, order specifies where in the schema to add the column.
	AddColumn(ctx *Context, column *Column, order *ColumnOrder) error
	// DropColumn drops the column with the name given.
	DropColumn(ctx *Context, columnName string) error
	// ModifyColumn modifies the column with the name given, replacing with the new column definition provided (which may
	// include a name change). If non-nil, order specifies where in the schema to move the column.
	ModifyColumn(ctx *Context, columnName string, column *Column, order *ColumnOrder) error
}

// Lockable should be implemented by tables that can be locked and unlocked.
type Lockable interface {
	Nameable
	// Lock locks the table either for reads or writes. Any session clients can
	// read while the table is locked for read, but not write.
	// When the table is locked for write, nobody can write except for the
	// session client that requested the lock.
	Lock(ctx *Context, write bool) error
	// Unlock releases the lock for the current session client. It blocks until
	// all reads or writes started during the lock are finished.
	// Context may be nil if the unlock it's because the connection was closed.
	// The id will always be provided, since in some cases context is not
	// available.
	Unlock(ctx *Context, id uint32) error
}

// StoredProcedureDetails are the details of the stored procedure. Integrators only need to store and retrieve the given
// details for a stored procedure, as the engine handles all parsing and processing.
type StoredProcedureDetails struct {
	Name            string    // The name of this stored procedure. Names must be unique within a database.
	CreateStatement string    // The CREATE statement for this stored procedure.
	CreatedAt       time.Time // The time that the stored procedure was created.
	ModifiedAt      time.Time // The time of the last modification to the stored procedure.
}

// StoredProcedureDatabase is a database that supports the creation and execution of stored procedures. The engine will
// handle all parsing and execution logic for stored procedures. Integrators only need to store and retrieve
// StoredProcedureDetails, while verifying that all stored procedures have a unique name without regard to
// case-sensitivity.
type StoredProcedureDatabase interface {
	Database

	// GetStoredProcedures returns all StoredProcedureDetails for the database.
	GetStoredProcedures(ctx *Context) ([]StoredProcedureDetails, error)

	// SaveStoredProcedure stores the given StoredProcedureDetails to the database. The integrator should verify that
	// the name of the new stored procedure is unique amongst existing stored procedures.
	SaveStoredProcedure(ctx *Context, spd StoredProcedureDetails) error

	// DropStoredProcedure removes the StoredProcedureDetails with the matching name from the database.
	DropStoredProcedure(ctx *Context, name string) error
}

// EvaluateCondition evaluates a condition, which is an expression whose value
// will be nil or coerced boolean.
func EvaluateCondition(ctx *Context, cond Expression, row Row) (interface{}, error) {
	v, err := cond.Eval(ctx, row)
	if err != nil {
		return false, err
	}
	if v == nil {
		return nil, nil
	}

	switch b := v.(type) {
	case bool:
		return b, nil
	case int:
		return b != int(0), nil
	case int64:
		return b != int64(0), nil
	case int32:
		return b != int32(0), nil
	case int16:
		return b != int16(0), nil
	case int8:
		return b != int8(0), nil
	case uint:
		return b != uint(0), nil
	case uint64:
		return b != uint64(0), nil
	case uint32:
		return b != uint32(0), nil
	case uint16:
		return b != uint16(0), nil
	case uint8:
		return b != uint8(0), nil
	case time.Duration:
		return int64(b) != 0, nil
	case time.Time:
		return b.UnixNano() != 0, nil
	case float64:
		return int(math.Round(v.(float64))) != 0, nil
	case float32:
		return int(math.Round(float64(v.(float32)))) != 0, nil
	case string:
		parsed, err := strconv.ParseFloat(v.(string), 64)
		return err == nil && int(parsed) != 0, nil
	default:
		return false, nil
	}
}

// IsFalse coerces EvaluateCondition interface{} response to boolean
func IsFalse(val interface{}) bool {
	res, ok := val.(bool)
	return ok && !res
}

// IsTrue coerces EvaluateCondition interface{} response to boolean
func IsTrue(val interface{}) bool {
	res, ok := val.(bool)
	return ok && res
}

// TypesEqual compares two Types and returns whether they are equivalent.
func TypesEqual(a, b Type) bool {
	//TODO: replace all of the Type() == Type() calls with TypesEqual
	if tupA, ok := a.(tupleType); ok {
		if tupB, ok := b.(tupleType); ok && len(tupA) == len(tupB) {
			for i := range tupA {
				if !TypesEqual(tupA[i], tupB[i]) {
					return false
				}
			}
			return true
		}
		return false
	} else if _, ok := b.(tupleType); ok {
		return false
	}
	return a == b
}
