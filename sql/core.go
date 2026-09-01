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
	"context"
	"encoding/json"
	"fmt"
	"math"
	trace2 "runtime/trace"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql/values"
)

// Expression is a combination of one or more SQL expressions.
type Expression interface {
	Resolvable
	fmt.Stringer
	// Type returns the expression type.
	Type(ctx *Context) Type
	// IsNullable returns whether the expression can be null.
	IsNullable(ctx *Context) bool
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

// RowIterExpression is an Expression that returns a RowIter rather than a scalar, used to implement functions that
// return sets.
type RowIterExpression interface {
	Expression
	// EvalRowIter evaluates the expression, which must be a RowIter
	EvalRowIter(ctx *Context, r Row) (RowIter, error)
	// ReturnsRowIter returns whether this expression returns a RowIter
	ReturnsRowIter() bool
}

// ExpressionWithNodes is an expression that contains nodes as children.
type ExpressionWithNodes interface {
	Expression
	// NodeChildren returns all node children.
	NodeChildren() []Node
	// WithNodeChildren returns a copy of the expression with its node children replaced. It will return an error if the
	// number of children is different than the current number of children. They must be given in the same order as they
	// are returned by NodeChildren.
	WithNodeChildren(ctx *Context, children ...Node) (ExpressionWithNodes, error)
}

// NonDeterministicExpression allows a way for expressions to declare that they are non-deterministic, which will
// signal the engine to not cache their results when this would otherwise appear to be safe.
type NonDeterministicExpression interface {
	Expression
	// IsNonDeterministic returns whether this expression returns a non-deterministic result. An expression is
	// non-deterministic if it can return different results on subsequent evaluations.
	IsNonDeterministic() bool
}

// IsNullExpression indicates that this expression tests for IS NULL.
type IsNullExpression interface {
	Expression
	IsNullExpression() bool
}

// IsNotNullExpression indicates that this expression tests for IS NOT NULL. Note that in some cases in some
// database engines, such as records in Postgres, IS NOT NULL is not identical to NOT(IS NULL).
type IsNotNullExpression interface {
	Expression
	IsNotNullExpression() bool
}

// Node is a node in the execution plan tree.
type Node interface {
	Resolvable
	fmt.Stringer
	// Schema of the node.
	Schema(ctx *Context) Schema
	// Children nodes.
	Children() []Node
	// WithChildren returns a copy of the node with children replaced.
	// It will return an error if the number of children is different than
	// the current number of children. They must be given in the same order
	// as they are returned by Children.
	WithChildren(ctx *Context, children ...Node) (Node, error)
	// IsReadOnly returns whether the node is read-only.
	IsReadOnly() bool
}

// NodeExecBuilder converts a sql.Node tree into a RowIter.
type NodeExecBuilder interface {
	Build(ctx *Context, n Node, r Row) (RowIter, error)
}

// ExecSourceRel is a node that has no children and is directly row generating.
// See also |ExecBuilderNode| for nodes that want to control their own iterator generation but have children
// they need to build iterators for.
type ExecSourceRel interface {
	Node
	// RowIter returns a RowIter for this node
	RowIter(ctx *Context, r Row) (RowIter, error)
}

// ExecBuilderNode is a that generates its own RowIter given a builder.
type ExecBuilderNode interface {
	Node
	// BuildRowIter builds a RowIter for the node with the builder provided
	BuildRowIter(ctx *Context, b NodeExecBuilder, r Row) (RowIter, error)
}

// Nameable is something that has a name.
type Nameable interface {
	// Name returns the name.
	Name() string
}

// RenameableNode is a Node that can be renamed.
type RenameableNode interface {
	Nameable
	Node
	// WithName returns a copy of the node with the name changed.
	WithName(string) Node
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

// BinaryNode is a Node with two children
type BinaryNode interface {
	Left() Node
	Right() Node
}

// UnaryNode is a Node with one child.
type UnaryNode interface {
	Child() Node
}

// CommentedNode allows comments to be set and retrieved on it. Used primarily for join hint comments.
type CommentedNode interface {
	Node
	WithComment(string) Node
	Comment() string
}

// OpaqueNode is a node that doesn't allow transformations to its children and
// acts as a black box.
type OpaqueNode interface {
	Node
	// Opaque reports whether the node is opaque or not.
	Opaque() bool
}

func IsOpaque(n Node) bool {
	if o, ok := n.(OpaqueNode); ok {
		return o.Opaque()
	}
	return false
}

// Projector is a node that projects expressions for parent nodes to consume (i.e. GroupBy, Window, Project).
type Projector interface {
	// ProjectedExprs returns the list of expressions projected by this node.
	ProjectedExprs() []Expression
}

// Expressioner is a node that contains expressions.
type Expressioner interface {
	// Expressions returns the list of expressions contained by the node.
	Expressions() []Expression
	// WithExpressions returns a copy of the node with expressions replaced.
	// It will return an error if the number of expressions is different than
	// the current number of expressions. They must be given in the same order
	// as they are returned by Expressions.
	WithExpressions(ctx *Context, exprs ...Expression) (Node, error)
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
	WithPrimaryKeySchema(ctx *Context, schema PrimaryKeySchema) (Node, error)
}

// DynamicColumnsTable is a table with a schema that is variable depending
// on the tables in the database (information_schema.columns).
type DynamicColumnsTable interface {
	// AllColumns returns all columns that need to be resolved
	// for this particular table.
	AllColumns(*Context) (Schema, error)
	// WithDefaultsSchema returns a table with a fully resolved
	// schema for every column in AllColumns.
	WithDefaultsSchema(Schema) (Table, error)
	// HasDynamicColumns indicates that a type implements the
	// DynamicColumnsTable interface.
	HasDynamicColumns() bool
}

// PartitionCounter can return the number of partitions.
type PartitionCounter interface {
	// PartitionCount returns the number of partitions.
	PartitionCount(*Context) (int64, error)
}

// Closer is a node that can be closed.
type Closer interface {
	Close(*Context) error
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

// ConvertToBool converts a value to a boolean. nil is considered false.
// TODO: the logic here should be merged with types.Boolean.Convert()
func ConvertToBool(ctx *Context, v interface{}) (bool, error) {
	switch b := v.(type) {
	case []uint8:
		return ConvertToBool(ctx, string(b))
	case bool:
		return b, nil
	case int:
		return b != 0, nil
	case int64:
		return b != 0, nil
	case int32:
		return b != 0, nil
	case int16:
		return b != 0, nil
	case int8:
		return b != 0, nil
	case uint:
		return b != 0, nil
	case uint64:
		return b != 0, nil
	case uint32:
		return b != 0, nil
	case uint16:
		return b != 0, nil
	case uint8:
		return b != 0, nil
	case time.Duration:
		return b != 0, nil
	case time.Time:
		return b.UnixNano() != 0, nil
	case float32:
		return b != 0, nil
	case float64:
		return b != 0, nil
	case string:
		bFloat, err := strconv.ParseFloat(TrimStringToNumberPrefix(ctx, b, false), 64)
		if err != nil {
			return false, nil
		}
		return bFloat != 0, nil
	case *apd.Decimal:
		return !b.IsZero(), nil
	case nil:
		return false, fmt.Errorf("unable to cast nil to bool")
	default:
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}
}

const (
	// IntCutSet is the set of characters that should be trimmed from the beginning and end of a string
	//   when converting to a signed or unsigned integer
	IntCutSet = " \t"

	// NumericCutSet is the set of characters to trim from a string before converting it to a number.
	NumericCutSet = " \t\n\r"
)

var ErrVectorInvalidBinaryLength = errors.NewKind("cannot convert BINARY(%d) to vector, byte length must be a multiple of 4 bytes")

// DecodeVector decodes a byte slice that represents a vector. This is needed for distance functions.
func DecodeVector(buf []byte) ([]float32, error) {
	if len(buf)%int(values.Float32Size) != 0 {
		return nil, ErrVectorInvalidBinaryLength.New(len(buf))
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&buf[0])), len(buf)/int(values.Float32Size)), nil
}

// EncodeVector encodes a byte slice that represents a vector.
func EncodeVector(floats []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&floats[0])), len(floats)*int(values.Float32Size))
}

func ConvertToVector(ctx context.Context, v interface{}) ([]float32, error) {
	var err error
	v, err = UnwrapAny(ctx, v)
	if err != nil {
		return nil, err
	}
	switch b := v.(type) {
	case []float32:
		return b, nil
	case []byte:
		return DecodeVector(b)
	case string:
		var val interface{}
		err := json.Unmarshal([]byte(b), &val)
		if err != nil {
			return nil, fmt.Errorf("can't convert JSON to vector: %w", err)
		}
		return convertJsonInterfaceToVector(val)
	case JSONWrapper:
		val, err := b.ToInterface(ctx)
		if err != nil {
			return nil, err
		}
		return convertJsonInterfaceToVector(val)
	default:
		return nil, fmt.Errorf("unable to cast %#v of type %T to vector", v, v)
	}
}

func convertJsonInterfaceToVector(val interface{}) ([]float32, error) {
	array, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("can't convert JSON to vector; expected array, got %T", val)
	}
	res := make([]float32, len(array))
	for i, elem := range array {
		switch v := elem.(type) {
		case float32:
			res[i] = v
		case float64:
			if v > math.MaxFloat32 || v < -math.MaxFloat32 {
				return nil, fmt.Errorf("data cannot be converted to a valid vector: %v", v)
			}
			res[i] = float32(v)
		case int64:
			res[i] = float32(v)
		case int32:
			res[i] = float32(v)
		default:
			return nil, fmt.Errorf("can't convert JSON to vector; expected array of floats, but array contained %T", elem)
		}
	}
	return res, nil
}

// EvaluateCondition evaluates a condition, which is an expression whose value
// will be nil or coerced boolean.
func EvaluateCondition(ctx *Context, cond Expression, row Row) (interface{}, error) {
	defer trace2.StartRegion(ctx, "EvaluateCondition").End()

	v, err := cond.Eval(ctx, row)
	if err != nil {
		return false, err
	}
	if v == nil {
		return nil, nil
	}
	res, err := ConvertToBool(ctx, v)
	if err != nil {
		return nil, err
	}
	return res, nil
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

// DebugStringer is shared by implementors of Node and Expression, and is used for debugging the analyzer. It allows
// a node or expression to be printed in greater detail than its default String() representation.
type DebugStringer interface {
	// DebugString prints a debug string of the node in question.
	DebugString(ctx *Context) string
}

// DebugString returns a debug string for the Node or Expression given.
func DebugString(ctx *Context, nodeOrExpression interface{}) string {
	if ds, ok := nodeOrExpression.(DebugStringer); ok {
		return ds.DebugString(ctx)
	}
	if s, ok := nodeOrExpression.(fmt.Stringer); ok {
		return s.String()
	}
	if nodeOrExpression == nil {
		return ""
	}
	panic(fmt.Sprintf("Expected sql.DebugString or fmt.Stringer for %T", nodeOrExpression))
}

// ValueExpression is an experimental future interface alternative to Expression to provide faster access.
type ValueExpression interface {
	Expression
	// EvalValue evaluates the given row frame and returns a result.
	EvalValue(ctx *Context, row ValueRow) (Value, error)
	// IsValueExpression indicates whether this expression and all its children support ValueExpression.
	IsValueExpression(ctx *Context) bool
}

var _ SystemVariable = (*MysqlSystemVariable)(nil)

type NameableNode interface {
	Nameable
	Node
}

func (s *ImmutableStatusVarValue) Copy() StatusVarValue {
	ret := *s
	return &ret
}

// StoredProcParam is a Parameter for a Stored Procedure.
// Stored Procedures Parameters can be referenced from within other Stored Procedures, so we need to store them
// somewhere that is accessible between interpreter calls to the engine.
type StoredProcParam struct {
	Type       Type
	Value      any
	Reference  *StoredProcParam
	HasBeenSet bool
}

// SetValue saves val to the StoredProcParam, and set HasBeenSet to true.
func (s *StoredProcParam) SetValue(val any) {
	s.Value = val
	s.HasBeenSet = true
	if s.Reference != nil && s != s.Reference {
		s.Reference.SetValue(val)
	}
}

// OrderAndLimit stores the context of an ORDER BY ... LIMIT statement, and is used by index lookups and iterators.
type OrderAndLimit struct {
	OrderBy       Expression
	Limit         Expression
	Literal       Expression
	CalcFoundRows bool
}

func (v OrderAndLimit) DebugString(ctx *Context) string {
	if v.Limit != nil {
		return fmt.Sprintf("%v LIMIT %v", DebugString(ctx, v.OrderBy), DebugString(ctx, v.Limit))
	}
	return DebugString(ctx, v.OrderBy)
}

func (v OrderAndLimit) String() string {
	if v.Limit != nil {
		return fmt.Sprintf("%v LIMIT %v", v.OrderBy, v.Limit)
	}
	return v.OrderBy.String()
}
