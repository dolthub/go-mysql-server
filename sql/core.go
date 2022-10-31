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

// BinaryNode has two children
type BinaryNode interface {
	Left() Node
	Right() Node
}

// UnaryNode has one child
type UnaryNode interface {
	Child() Node
}

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
	WithChildren(children ...Expression) (Expression, error)
}

type Expression2 interface {
	Expression
	// Eval2 evaluates the given row frame and returns a result.
	Eval2(ctx *Context, row Row2) (Value, error)
	// Type2 returns the expression type.
	Type2() Type2
}

// UnsupportedFunctionStub is a marker interface for function stubs that are unsupported
type UnsupportedFunctionStub interface {
	IsUnsupported() bool
}

// FunctionExpression is an Expression that represents a function.
type FunctionExpression interface {
	Expression
	FunctionName() string
	Description() string
	// TODO: add Example() function
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
// aggregation buffer is created for each grouping (NewBuffer). Rows for the
// grouping should be fed to the buffer with |Update| and the buffer should be
// eval'd with |Eval|. Calling |Eval| directly on an Aggregation expression is
// typically an error.
type Aggregation interface {
	WindowAdaptableExpression
	// NewBuffer creates a new aggregation buffer and returns it as a Row.
	NewBuffer() (AggregationBuffer, error)
	// WithWindow returns a version of this aggregation with the WindowDefinition given
	WithWindow(window *WindowDefinition) (Aggregation, error)
	// Window returns this expression's window
	Window() *WindowDefinition
}

// WindowBuffer is a type alias for a window materialization
type WindowBuffer []Row

// WindowInterval is a WindowBuffer index range, where [Start] is inclusive, and [End] is exclusive
type WindowInterval struct {
	Start, End int
}

// WindowFunction performs aggregations on buffer intervals, optionally maintaining internal state
// for performance optimizations
type WindowFunction interface {
	Disposable

	// WithWindow passes fields from the parent WindowDefinition, deferring partial construction of a WindowFunction
	WithWindow(w *WindowDefinition) (WindowFunction, error)
	// StartPartition discards any previous state and initializes the aggregation for a new partition
	StartPartition(*Context, WindowInterval, WindowBuffer) error
	// DefaultFramer returns a new instance of the default WindowFramer for a particular aggregation
	DefaultFramer() WindowFramer
	// NewSlidingFrameInterval is updates the function's internal aggregation state for the next
	// Compute call using three WindowInterval: added, dropped, and current.
	//TODO: implement sliding window interface in aggregation functions and windowBlockIter
	//NewSlidingFrameInterval(added, dropped WindowInterval)
	// Compute returns an aggregation result for a given interval and buffer
	Compute(*Context, WindowInterval, WindowBuffer) interface{}
}

// WindowAdaptableExpression is an Expression that can be executed as a window aggregation
type WindowAdaptableExpression interface {
	Expression

	// NewEvalable constructs an executable aggregation WindowFunction
	NewWindowFunction() (WindowFunction, error)
}

// WindowFramer is responsible for tracking window frame indices for partition rows.
// WindowFramer is aware of the framing strategy (offsets, ranges, etc),
// and is responsible for returning a WindowInterval for each partition row.
type WindowFramer interface {
	// NewFramer is a prototype constructor that create a new Framer with pass-through
	// parent arguments
	NewFramer(WindowInterval) (WindowFramer, error)
	// Next returns the next WindowInterval frame, or an io.EOF error after the last row
	Next(*Context, WindowBuffer) (WindowInterval, error)
	// FirstIdx returns the current frame start index
	FirstIdx() int
	// LastIdx returns the last valid index in the current frame
	LastIdx() int
	// Interval returns the current frame as a WindowInterval
	Interval() (WindowInterval, error)
	// SlidingInterval returns three WindowIntervals: the current frame, dropped range since the
	// last frame, and added range since the last frame.
	// TODO: implement sliding window interface in framers, windowBlockIter, and aggregation functions
	//SlidingInterval(ctx Context) (WindowInterval, WindowInterval, WindowInterval)
}

// WindowFrame describe input bounds for an aggregation function
// execution. A frame will only have two non-null fields for the start
// and end bounds. A WindowFrame plan node is associated
// with an exec WindowFramer.
type WindowFrame interface {
	fmt.Stringer

	// NewFramer constructs an executable WindowFramer
	NewFramer(*WindowDefinition) (WindowFramer, error)
	// UnboundedFollowing returns whether a frame end is unbounded
	UnboundedFollowing() bool
	// UnboundedPreceding returns whether a frame start is unbounded
	UnboundedPreceding() bool
	// StartCurrentRow returns whether a frame start is CURRENT ROW
	StartCurrentRow() bool
	// EndCurrentRow returns whether a frame end is CURRENT ROW
	EndCurrentRow() bool
	// StartNFollowing returns a frame's start preceding Expression or nil
	StartNPreceding() Expression
	// StartNFollowing returns a frame's start following Expression or nil
	StartNFollowing() Expression
	// EndNPreceding returns whether a frame end preceding Expression or nil
	EndNPreceding() Expression
	// EndNPreceding returns whether a frame end following Expression or nil
	EndNFollowing() Expression
}

type AggregationBuffer interface {
	Disposable

	// Eval the given buffer.
	Eval(*Context) (interface{}, error)
	// Update the given buffer with the given row.
	Update(ctx *Context, row Row) error
}

// WindowAggregation implements a window aggregation expression. A WindowAggregation is similar to an Aggregation,
// except that it returns a result row for every input row, as opposed to as single for the entire result set. A
// WindowAggregation is expected to track its input rows in the order received, and to return the value for the row
// index given on demand.
type WindowAggregation interface {
	WindowAdaptableExpression
	// Window returns this expression's window
	Window() *WindowDefinition
	// WithWindow returns a version of this window aggregation with the window given
	WithWindow(window *WindowDefinition) (WindowAggregation, error)
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
	WithChildren(children ...Node) (Node, error)
	// CheckPrivileges passes the operations representative of this Node to the PrivilegedOperationChecker to determine
	// whether a user (contained in the context, along with their active roles) has the necessary privileges to execute
	// this node (and its children).
	CheckPrivileges(ctx *Context, opChecker PrivilegedOperationChecker) bool
}

type Node2 interface {
	Node

	// RowIter2 produces a row iterator from this node. The current row frame being
	// evaluated is provided, as well the context of the query.
	RowIter2(ctx *Context, f *RowFrame) (RowIter2, error)
}

// RowIterTypeSelector is implemented by top-level type-switch nodes that return either a Node or Node2 implementation.
type RowIterTypeSelector interface {
	RowIter
	IsNode2() bool
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
// acts as a black box.
type OpaqueNode interface {
	Node
	// Opaque reports whether the node is opaque or not.
	Opaque() bool
}

// Projector is a node that projects expressions for parent nodes to consume (i.e. GroupBy, Window, Project).
type Projector interface {
	// ProjectedExprs returns the list of expressions projected by this node.
	ProjectedExprs() []Expression
	// WithProjectedExprs returns a new Projector instance with the specified expressions set as its projected expressions.
	WithProjectedExprs(...Expression) (Projector, error)
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

// Databaseable is a node with a string reference to a database
type Databaseable interface {
	Database() string
}

// MultiDatabaser is a node that contains a reference to a database provider. This interface is intended for very
// specific nodes that must resolve databases during execution time rather than during analysis, such as block
// statements where the execution of a nested statement in the block may affect future statements within that same block.
type MultiDatabaser interface {
	// DatabaseProvider returns the current DatabaseProvider.
	DatabaseProvider() DatabaseProvider
	// WithDatabaseProvider returns a new node instance with the database provider replaced with the one given as parameter.
	WithDatabaseProvider(DatabaseProvider) (Node, error)
}

// SchemaTarget is a node that has a target schema that can be set during analysis. This is necessary because some
// schema objects (things that involve expressions, column references, etc.) can only be reified during analysis. The
// target schema is the schema of a table under a DDL operation, not the schema of rows returned by this node.
type SchemaTarget interface {
	// WithTargetSchema returns a copy of this node with the target schema set
	WithTargetSchema(Schema) (Node, error)
	// TargetSchema returns the target schema for this node
	TargetSchema() Schema
}

// PrimaryKeySchemaTarget is a node that has a primary key target schema that can be set
type PrimaryKeySchemaTarget interface {
	SchemaTarget
	WithPrimaryKeySchema(schema PrimaryKeySchema) (Node, error)
}

// TableFunction is a node that is generated by a function
type TableFunction interface {
	Node
	Expressioner
	Databaser

	// NewInstance returns a new instance of the table function
	NewInstance(ctx *Context, db Database, expressions []Expression) (Node, error)
	// FunctionName returns the name of this table function
	FunctionName() string
}

// Table represents the backend of a SQL table.
type Table interface {
	Nameable
	String() string
	Schema() Schema
	Collation() CollationID
	Partitions(*Context) (PartitionIter, error)
	PartitionRows(*Context, Partition) (RowIter, error)
}

type Table2 interface {
	Table

	PartitionRows2(ctx *Context, part Partition) (RowIter2, error)
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
	Filters() []Expression
	HandledFilters(filters []Expression) []Expression
	WithFilters(ctx *Context, filters []Expression) Table
}

// ProjectedTable is a table that can produce a specific RowIter
// that's more optimized given the columns that are projected.
type ProjectedTable interface {
	Table
	WithProjections(colNames []string) Table
	Projections() []string
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
	IndexConstraint_Primary
)

// IndexColumn is the column by which to add to an index.
type IndexColumn struct {
	Name string
	// Length represents the index prefix length. If zero, then no length was specified.
	Length int64
}

// IndexAddressable is a table that can be scanned through a primary index
type IndexAddressable interface {
	// IndexedAccess returns a table that can perform scans constrained to
	// an IndexLookup on the index given
	IndexedAccess(Index) IndexedTable
	// GetIndexes returns an array of this table's Indexes
	GetIndexes(ctx *Context) ([]Index, error)
}

type IndexAddressableTable interface {
	Table
	IndexAddressable
}

// IndexedTable is a table with an index chosen for range scans
type IndexedTable interface {
	Table
	// LookupPartitions returns partitions scanned by the given IndexLookup
	LookupPartitions(*Context, IndexLookup) (PartitionIter, error)
}

type ParallelizedIndexAddressableTable interface {
	IndexAddressableTable
	ShouldParallelizeAccess() bool
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

// ForeignKeyTable is a table that can declare its foreign key constraints, as well as be referenced.
type ForeignKeyTable interface {
	IndexAddressableTable
	// CreateIndexForForeignKey creates an index for this table, using the provided parameters. Indexes created through
	// this function are specifically ones generated for use with a foreign key. Returns an error if the index name
	// already exists, or an index on the same columns already exists.
	CreateIndexForForeignKey(ctx *Context, indexName string, using IndexUsing, constraint IndexConstraint, columns []IndexColumn) error

	// GetDeclaredForeignKeys returns the foreign key constraints that are declared by this table.
	GetDeclaredForeignKeys(ctx *Context) ([]ForeignKeyConstraint, error)
	// GetReferencedForeignKeys returns the foreign key constraints that are referenced by this table.
	GetReferencedForeignKeys(ctx *Context) ([]ForeignKeyConstraint, error)
	// AddForeignKey adds the given foreign key constraint to the table. Returns an error if the foreign key name
	// already exists on any other table within the database.
	AddForeignKey(ctx *Context, fk ForeignKeyConstraint) error
	// DropForeignKey removes a foreign key from the table.
	DropForeignKey(ctx *Context, fkName string) error
	// UpdateForeignKey updates the given foreign key constraint. May range from updated table names to setting the
	// IsResolved boolean.
	UpdateForeignKey(ctx *Context, fkName string, fk ForeignKeyConstraint) error
	// GetForeignKeyUpdater returns a ForeignKeyUpdater for this table.
	GetForeignKeyUpdater(ctx *Context) ForeignKeyUpdater
}

// ForeignKeyUpdater is a TableEditor that is addressable via IndexLookup.
type ForeignKeyUpdater interface {
	RowInserter
	RowUpdater
	RowDeleter
	IndexAddressable
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

// PrimaryKeyAlterableTable represents a table that supports primary key changes.
type PrimaryKeyAlterableTable interface {
	Table
	// CreatePrimaryKey creates a primary key for this table, using the provided parameters.
	// Returns an error if the new primary key set is not compatible with the current table data.
	CreatePrimaryKey(ctx *Context, columns []IndexColumn) error
	// DropPrimaryKey drops a primary key on a table. Returns an error if that table does not have a key.
	DropPrimaryKey(ctx *Context) error
}

type PrimaryKeyTable interface {
	// PrimaryKeySchema returns this table's PrimaryKeySchema
	PrimaryKeySchema() PrimaryKeySchema
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
	// GetNextAutoIncrementValue gets the next AUTO_INCREMENT value. In the case that a table with an autoincrement
	// column is passed in a row with the autoinc column failed, the next auto increment value must
	// update its internal state accordingly and use the insert val at runtime.
	// Implementations are responsible for updating their state to provide the correct values.
	GetNextAutoIncrementValue(ctx *Context, insertVal interface{}) (uint64, error)
	// AutoIncrementSetter returns an AutoIncrementSetter.
	AutoIncrementSetter(*Context) AutoIncrementSetter
}

var ErrNoAutoIncrementCol = fmt.Errorf("this table has no AUTO_INCREMENT columns")

// AutoIncrementSetter provides support for altering a table's
// AUTO_INCREMENT sequence, eg 'ALTER TABLE t AUTO_INCREMENT = 10;'
type AutoIncrementSetter interface {
	// SetAutoIncrementValue sets a new AUTO_INCREMENT value.
	SetAutoIncrementValue(*Context, uint64) error
	// Close finalizes the set operation, persisting the result.
	Closer
}

type Closer interface {
	Close(*Context) error
}

// RowReplacer is a combination of RowDeleter and RowInserter.
// TODO: We can't embed those interfaces because go 1.13 doesn't allow for overlapping interfaces (they both declare
// Close). Go 1.14 fixes this problem, but we aren't ready to drop support for 1.13 yet.
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
	// Closer finalizes the replace operation, persisting the result.
	Closer
}

// ReplaceableTable allows rows to be replaced through a Delete (if applicable) then Insert.
type ReplaceableTable interface {
	Table
	// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
	// for each row, followed by a call to Close() when all rows have been processed.
	Replacer(ctx *Context) RowReplacer
}

// UpdatableTable is a table that can process updates of existing rows via update statements.
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
	// Closer finalizes the update operation, persisting the result.
	Closer
}

// RewritableTable is an extension to Table that makes it simpler for integrators to adapt to schema changes that must
// rewrite every row of the table. In this case, rows are streamed from the existing table in the old schema,
// transformed / updated appropriately, and written with the new format.
type RewritableTable interface {
	Table
	AlterableTable

	// ShouldRewriteTable returns whether this table should be rewritten because of a schema change. The old and new
	// versions of the schema and modified column are provided. For some operations, one or both of |oldColumn| or
	// |newColumn| may be nil.
	// The engine may decide to rewrite tables regardless in some cases, such as when a new non-nullable column is added.
	ShouldRewriteTable(ctx *Context, oldSchema, newSchema PrimaryKeySchema, oldColumn, newColumn *Column) bool

	// RewriteInserter returns a RowInserter for the new schema. Rows from the current table, with the old schema, will
	// be streamed from the table and passed to this RowInserter. Implementor tables must still return rows in the
	// current schema until the rewrite operation completes. |Close| will be called on RowInserter when all rows have
	// been inserted.
	RewriteInserter(ctx *Context, oldSchema, newSchema PrimaryKeySchema, oldColumn, newColumn *Column) (RowInserter, error)
}

// DatabaseProvider is a collection of Database.
type DatabaseProvider interface {
	// Database gets a Database from the provider.
	Database(ctx *Context, name string) (Database, error)

	// HasDatabase checks if the Database exists in the provider.
	HasDatabase(ctx *Context, name string) bool

	// AllDatabases returns a slice of all Databases in the provider.
	AllDatabases(ctx *Context) []Database
}

type MutableDatabaseProvider interface {
	DatabaseProvider

	// CreateDatabase creates a database and adds it to the provider's collection.
	CreateDatabase(ctx *Context, name string) error

	// DropDatabase removes a database from the provider's collection.
	DropDatabase(ctx *Context, name string) error
}

type CollatedDatabaseProvider interface {
	MutableDatabaseProvider

	// CreateCollatedDatabase creates a collated database and adds it to the provider's collection.
	CreateCollatedDatabase(ctx *Context, name string, collation CollationID) error
}

// ExternalStoredProcedureProvider provides access to built-in stored procedures. These procedures are implemented
// as functions, instead of as SQL statements. The returned stored procedures cannot be modified or deleted.
type ExternalStoredProcedureProvider interface {
	// ExternalStoredProcedure returns the external stored procedure details for the procedure with the specified name
	// that is able to accept the specified number of parameters. If no matching external stored procedure is found,
	// nil, nil is returned. If an unexpected error is encountered, it is returned as the error parameter.
	ExternalStoredProcedure(ctx *Context, name string, numOfParams int) (*ExternalStoredProcedureDetails, error)
	// ExternalStoredProcedures returns a slice of all external stored procedure details with the specified name. External
	// stored procedures can overload the same name with different arguments, so this method enables a caller to see all
	// available variants with the specified name. If no matching external stored procedures are found, an
	// empty slice is returned, with a nil error. If an unexpected error is encountered, it is returned as the
	// error parameter.
	ExternalStoredProcedures(ctx *Context, name string) ([]ExternalStoredProcedureDetails, error)
}

// FunctionProvider is an extension of DatabaseProvider that allows custom functions to be provided
type FunctionProvider interface {
	// Function returns the function with the name provided, case-insensitive
	Function(ctx *Context, name string) (Function, error)
}

// TableFunctionProvider is an extension of DatabaseProvider that allows custom table functions to be provided
type TableFunctionProvider interface {
	// TableFunction returns the table function with the name provided, case-insensitive
	TableFunction(ctx *Context, name string) (TableFunction, error)
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

// CollatedDatabase is a Database that may store and update its collation.
type CollatedDatabase interface {
	Database

	// GetCollation returns this database's collation.
	GetCollation(ctx *Context) CollationID

	// SetCollation updates this database's collation.
	SetCollation(ctx *Context, collation CollationID) error
}

// UnresolvedTable is a Table that is either unresolved or deferred for until an asOf resolution
type UnresolvedTable interface {
	Nameable
	// Database returns the database name
	Database() string
	// WithAsOf returns a copy of this versioned table with its AsOf
	// field set to the given value. Analogous to WithChildren.
	WithAsOf(asOf Expression) (Node, error)
	//AsOf returns this table's asof expression.
	AsOf() Expression
}

type TransactionCharacteristic int

const (
	ReadWrite TransactionCharacteristic = iota
	ReadOnly
)

// Transaction is an opaque type implemented by an integrator to record necessary information at the start of a
// transaction. Active transactions will be recorded in the session.
type Transaction interface {
	fmt.Stringer
	IsReadOnly() bool
}

// TransactionDatabase is a Database that can BEGIN, ROLLBACK and COMMIT transactions, as well as create SAVEPOINTS and
// restore to them.
type TransactionDatabase interface {
	Database

	// StartTransaction starts a new transaction and returns it
	StartTransaction(ctx *Context, tCharacteristic TransactionCharacteristic) (Transaction, error)

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
	Name            string    // The name of this trigger. Trigger names in a database are unique.
	CreateStatement string    // The text of the statement to create this trigger.
	CreatedAt       time.Time // The time that the trigger was created.
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
	// CreateTable creates the table with the given name and schema. If a table with that name already exists, must return
	// sql.ErrTableAlreadyExists.
	CreateTable(ctx *Context, name string, schema PrimaryKeySchema, collation CollationID) error
}

// TemporaryTableCreator is a database that can create temporary tables that persist only as long as the session.
// Note that temporary tables with the same name as persisted tables take precedence in most SQL operations.
type TemporaryTableCreator interface {
	Database
	// CreateTemporaryTable creates the table with the given name and schema. If a temporary table with that name already exists, must
	// return sql.ErrTableAlreadyExists
	CreateTemporaryTable(ctx *Context, name string, schema PrimaryKeySchema, collation CollationID) error
}

// ViewDefinition is the named textual definition of a view
type ViewDefinition struct {
	Name           string
	TextDefinition string
}

// ViewDatabase is implemented by databases that persist view definitions
type ViewDatabase interface {
	// CreateView persists the definition a view with the name and select statement given. If a view with that name
	// already exists, should return ErrExistingView
	CreateView(ctx *Context, name string, selectStatement string) error

	// DropView deletes the view named from persistent storage. If the view doesn't exist, should return
	// ErrViewDoesNotExist
	DropView(ctx *Context, name string) error

	// GetView returns the textual definition of the view with the name given, or false if it doesn't exist.
	GetView(ctx *Context, viewName string) (string, bool, error)

	// AllViews returns the definitions of all views in the database
	AllViews(ctx *Context) ([]ViewDefinition, error)
}

// TableDropper should be implemented by databases that can drop tables.
type TableDropper interface {
	DropTable(ctx *Context, name string) error
}

// TableRenamer should be implemented by databases that can rename tables.
type TableRenamer interface {
	// RenameTable renames a table from oldName to newName as given. If a table with newName already exists, must return
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
	UpdatableTable

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

// ExternalStoredProcedureDetails are the details of an external stored procedure. Compared to standard stored
// procedures, external ones are considered "built-in", in that they're not created by the user, and may not be modified
// or deleted by a user. In addition, they're implemented as a function taking standard parameters, compared to stored
// procedures being implemented as expressions.
type ExternalStoredProcedureDetails struct {
	// Name is the name of the external stored procedure. If two external stored procedures share a name, then they're
	// considered overloaded. Standard stored procedures do not support overloading.
	Name string
	// Schema describes the row layout of the RowIter returned from Function.
	Schema Schema
	// Function is the implementation of the external stored procedure. All functions should have the following definition:
	// `func(*Context, <PARAMETERS>) (RowIter, error)`. The <PARAMETERS> may be any of the following types: `bool`,
	// `string`, `[]byte`, `int8`-`int64`, `uint8`-`uint64`, `float32`, `float64`, `time.Time`, or `Decimal`
	// (shopspring/decimal). The architecture-dependent types `int` and `uint` (without a number) are also supported.
	// It is valid to return a nil RowIter if there are no rows to be returned.
	//
	// Each parameter, by default, is an IN parameter. If the parameter type is a pointer, e.g. `*int32`, then it
	// becomes an INOUT parameter. There is no way to set a parameter as an OUT parameter.
	//
	// Values are converted to their nearest type before being passed in, following the conversion rules of their
	// related SQL types. The exceptions are `time.Time` (treated as a `DATETIME`), string (treated as a `LONGTEXT` with
	// the default collation) and Decimal (treated with a larger precision and scale). Take extra care when using decimal
	// for an INOUT parameter, to ensure that the returned value fits the original's precision and scale, else an error
	// will occur.
	//
	// As functions support overloading, each variant must have a completely unique function signature to prevent
	// ambiguity. Uniqueness is determined by the number of parameters. If two functions are returned that have the same
	// name and same number of parameters, then an error is thrown. If the last parameter is variadic, then the stored
	// procedure functions as though it has the integer-max number of parameters. When an exact match is not found for
	// overloaded functions, the largest function is used (which in this case will be the variadic function). Also, due
	// to the usage of the integer-max for the parameter count, only one variadic function is allowed per function name.
	// The type of the variadic parameter may not have a pointer type.
	Function interface{}
}

// FakeCreateProcedureStmt returns a parseable CREATE PROCEDURE statement for this external stored procedure, as some
// tools (such as Java's JDBC connector) require a valid statement in some situations.
func (espd ExternalStoredProcedureDetails) FakeCreateProcedureStmt() string {
	return fmt.Sprintf("CREATE PROCEDURE %s() SELECT 'External stored procedure';", espd.Name)
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

	// We can assume they have the same implementing type if this passes, so we have to check the parameters
	if a.Type() != b.Type() {
		return false
	}
	// Some types cannot be compared structurally as they contain non-comparable types (such as slices), so we handle
	// those separately.
	switch at := a.(type) {
	case enumType:
		aEnumType := at
		bEnumType := b.(enumType)
		if len(aEnumType.indexToVal) != len(bEnumType.indexToVal) {
			return false
		}
		for i := 0; i < len(aEnumType.indexToVal); i++ {
			if aEnumType.indexToVal[i] != bEnumType.indexToVal[i] {
				return false
			}
		}
		return aEnumType.collation == bEnumType.collation
	case setType:
		aSetType := at
		bSetType := b.(setType)
		if len(aSetType.bitToVal) != len(bSetType.bitToVal) {
			return false
		}
		for bit, aVal := range aSetType.bitToVal {
			if bVal, ok := bSetType.bitToVal[bit]; ok && aVal != bVal {
				return false
			}
		}
		return aSetType.collation == bSetType.collation
	case TupleType:
		if tupA, ok := a.(TupleType); ok {
			if tupB, ok := b.(TupleType); ok && len(tupA) == len(tupB) {
				for i := range tupA {
					if !TypesEqual(tupA[i], tupB[i]) {
						return false
					}
				}
				return true
			}
		}
		return false
	default:
		return a == b
	}
}
