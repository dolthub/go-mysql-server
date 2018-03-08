package sql

import (
	"gopkg.in/src-d/go-errors.v1"
)

// Nameable is something that has a name.
type Nameable interface {
	// Name returns the name.
	Name() string
}

// Resolvable is something that can be resolved or not.
type Resolvable interface {
	// Resolved returns whether the node is resolved.
	Resolved() bool
}

// Transformable is a node which can be transformed.
type Transformable interface {
	// TransformUp transforms all nodes and returns the result of this transformation.
	TransformUp(func(Node) Node) Node
	// TransformExpressionsUp transforms all expressions inside the node and all its
	// children and returns a node with the result of the transformations.
	TransformExpressionsUp(func(Expression) Expression) Node
}

// Expression is a combination of one or more SQL expressions.
type Expression interface {
	Resolvable
	// Type returns the expression type.
	Type() Type
	// Name returns the expression name.
	Name() string
	// IsNullable returns whether the expression can be null.
	IsNullable() bool
	// Eval evaluates the given row and returns a result.
	Eval(Session, Row) (interface{}, error)
	// TransformUp transforms the expression and all its children with the
	// given transform function.
	TransformUp(func(Expression) Expression) Expression
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
	Update(session Session, buffer, row Row) error
	// Merge merges a partial buffer into a global one.
	Merge(session Session, buffer, partial Row) error
}

// Node is a node in the execution plan tree.
type Node interface {
	Resolvable
	Transformable
	// Schema of the node.
	Schema() Schema
	// Children nodes.
	Children() []Node
	// RowIter produces a row iterator from this node.
	RowIter(Session) (RowIter, error)
}

// Table represents a SQL table.
type Table interface {
	Nameable
	Node
}

// Inserter allow rows to be inserted in them.
type Inserter interface {
	// Insert the given row.
	Insert(row Row) error
}

// Database represents the database.
type Database interface {
	Nameable
	// Tables returns the information of all tables.
	Tables() map[string]Table
}

// ErrInvalidType is thrown when there is an unexpected type at some part of
// the execution tree.
var ErrInvalidType = errors.NewKind("invalid type: %s")
