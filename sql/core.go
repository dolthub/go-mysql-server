package sql

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"gopkg.in/src-d/go-errors.v1"
)

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

	//ErrUnexpectedRowLength is thrown when the obtained row has more columns than the schema
	ErrUnexpectedRowLength = errors.NewKind("expected %d values, got %d")

	// ErrInvalidChildrenNumber is returned when the WithChildren method of a
	// node or expression is called with an invalid number of arguments.
	ErrInvalidChildrenNumber = errors.NewKind("%T: invalid children number, got %d, expected %d")

	// ErrDeleteRowNotFound
	ErrDeleteRowNotFound = errors.NewKind("row was not found when attempting to delete").New()
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
	Eval(*Context, Row) (interface{}, error)
	// Children returns the children expressions of this expression.
	Children() []Expression
	// WithChildren returns a copy of the expression with children replaced.
	// It will return an error if the number of children is different than
	// the current number of children. They must be given in the same order
	// as they are returned by Children.
	WithChildren(...Expression) (Expression, error)
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

// Node is a node in the execution plan tree.
type Node interface {
	Resolvable
	fmt.Stringer
	// Schema of the node.
	Schema() Schema
	// Children nodes.
	Children() []Node
	// RowIter produces a row iterator from this node.
	RowIter(*Context) (RowIter, error)
	// WithChildren returns a copy of the node with children replaced.
	// It will return an error if the number of children is different than
	// the current number of children. They must be given in the same order
	// as they are returned by Children.
	WithChildren(...Node) (Node, error)
}

// OpaqueNode is a node that doesn't allow transformations to its children and
// acts a a black box.
type OpaqueNode interface {
	Node
	// Opaque reports whether the node is opaque or not.
	Opaque() bool
}

// AsyncNode is a node that can be executed asynchronously.
type AsyncNode interface {
	// IsAsync reports whether the node is async or not.
	IsAsync() bool
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
	io.Closer
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

//FilteredTable is a table that can produce a specific RowIter
// that's more optimized given the filters.
type FilteredTable interface {
	Table
	HandledFilters(filters []Expression) []Expression
	WithFilters(filters []Expression) Table
	Filters() []Expression
}

// ProjectedTable is a table that can produce a specific RowIter
// that's more optimized given the columns that are projected.
type ProjectedTable interface {
	Table
	WithProjection(colNames []string) Table
	Projection() []string
}

// IndexableTable represents a table that supports being indexed and
// receiving indexes to be able to speed up its execution.
type IndexableTable interface {
	Table
	WithIndexLookup(IndexLookup) Table
	IndexLookup() IndexLookup
	IndexKeyValues(*Context, []string) (PartitionIndexKeyValueIter, error)
}

// Inserter allow rows to be inserted in them.
type Inserter interface {
	// Insert the given row.
	Insert(*Context, Row) error
}

// Deleter allow rows to be deleted from tables.
type Deleter interface {
	// Delete the given row. Returns ErrDeleteRowNotFound if the row was not found.
	Delete(*Context, Row) error
}

// Replacer allows rows to be replaced through a Delete (if applicable) then Insert.
type Replacer interface {
	Deleter
	Inserter
}

// Updater allows rows to be updated.
type Updater interface {
	// Update the given row. Provides both the old and new rows.
	Update(ctx *Context, old Row, new Row) error
}

// Database represents the database.
type Database interface {
	Nameable
	// Tables returns the information of all tables.
	Tables() map[string]Table
}

// TableCreator should be implemented by databases that can create new tables.
type TableCreator interface {
	CreateTable(ctx *Context, name string, schema Schema) error
}

// TableDropper should be implemented by databases that can drop tables.
type TableDropper interface {
	DropTable(ctx *Context, name string) error
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

// EvaluateCondition evaluates a condition, which is an expression whose value
// will be coerced to boolean.
func EvaluateCondition(ctx *Context, cond Expression, row Row) (bool, error) {
	v, err := cond.Eval(ctx, row)
	if err != nil {
		return false, err
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
