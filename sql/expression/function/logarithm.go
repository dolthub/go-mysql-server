package function

import (
	"fmt"
	"math"
	"reflect"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// ErrInvalidArgumentForLogarithm is returned when an invalid argument value is passed to a
// logarithm function
var ErrInvalidArgumentForLogarithm = errors.NewKind("invalid argument value for logarithm: %v")

// NewLogBaseFunc returns LogBase creator function with a specific base.
func NewLogBaseFunc(base float64) func(e sql.Expression) sql.Expression {
	return func(e sql.Expression) sql.Expression {
		return NewLogBase(base, e)
	}
}

// LogBase is a function that returns the logarithm of a value with a specific base.
type LogBase struct {
	expression.UnaryExpression
	base float64
}

// NewLogBase creates a new LogBase expression.
func NewLogBase(base float64, e sql.Expression) sql.Expression {
	return &LogBase{UnaryExpression: expression.UnaryExpression{Child: e}, base: base}
}

func (l *LogBase) String() string {
	switch l.base {
	case float64(math.E):
		return fmt.Sprintf("ln(%s)", l.Child)
	case float64(10):
		return fmt.Sprintf("log10(%s)", l.Child)
	case float64(2):
		return fmt.Sprintf("log2(%s)", l.Child)
	default:
		return fmt.Sprintf("log(%v, %s)", l.base, l.Child)
	}
}

// WithChildren implements the Expression interface.
func (l *LogBase) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLogBase(l.base, children[0]), nil
}

// Type returns the resultant type of the function.
func (l *LogBase) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the sql.Expression interface.
func (l *LogBase) IsNullable() bool {
	return l.base == float64(1) || l.base <= float64(0) || l.Child.IsNullable()
}

// Eval implements the Expression interface.
func (l *LogBase) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	val, err := sql.Float64.Convert(v)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(v))
	}
	return computeLog(val.(float64), l.base)
}

// Log is a function that returns the natural logarithm of a value.
type Log struct {
	expression.BinaryExpression
}

// NewLog creates a new Log expression.
func NewLog(args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen == 0 || argLen > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("LOG", "1 or 2", argLen)
	}

	if argLen == 1 {
		return &Log{expression.BinaryExpression{Left: expression.NewLiteral(math.E, sql.Float64), Right: args[0]}}, nil
	} else {
		return &Log{expression.BinaryExpression{Left: args[0], Right: args[1]}}, nil
	}
}

func (l *Log) String() string {
	return fmt.Sprintf("log(%s, %s)", l.Left, l.Right)
}

// WithChildren implements the Expression interface.
func (l *Log) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLog(children...)
}

// Children implements the Expression interface.
func (l *Log) Children() []sql.Expression {
	return []sql.Expression{l.Left, l.Right}
}

// Type returns the resultant type of the function.
func (l *Log) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the Expression interface.
func (l *Log) IsNullable() bool {
	return l.Left.IsNullable() || l.Right.IsNullable()
}

// Eval implements the Expression interface.
func (l *Log) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	left, err := l.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	lhs, err := sql.Float64.Convert(left)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(left))
	}

	right, err := l.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	rhs, err := sql.Float64.Convert(right)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(right))
	}

	// rhs becomes value, lhs becomes base
	return computeLog(rhs.(float64), lhs.(float64))
}

func computeLog(v float64, base float64) (float64, error) {
	if v <= 0 {
		return float64(0), ErrInvalidArgumentForLogarithm.New(v)
	}
	if base == float64(1) || base <= float64(0) {
		return float64(0), ErrInvalidArgumentForLogarithm.New(base)
	}
	switch base {
	case float64(2):
		return math.Log2(v), nil
	case float64(10):
		return math.Log10(v), nil
	case math.E:
		return math.Log(v), nil
	default:
		// LOG(BASE,V) is equivalent to LOG(V) / LOG(BASE).
		return float64(math.Log(v) / math.Log(base)), nil
	}
}
