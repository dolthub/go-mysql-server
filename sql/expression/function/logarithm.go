package function

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"math"
	"reflect"
)

// ErrInvalidArgumentForLogarithm is returned when an invalid argument value is passed to a
// logarithm function
var ErrInvalidArgumentForLogarithm = errors.NewKind("invalid argument value for logarithm: %v")

// Ln is a function that returns the exponential logarithm of a value.
type Ln struct {
	expression.UnaryExpression
}

// NewLn creates a new Ln expression.
func NewLn(e sql.Expression) sql.Expression {
	return &Ln{expression.UnaryExpression{Child: e}}
}

func (l *Ln) String() string {
	return fmt.Sprintf("ln(%s)", l.Child)
}

// TransformUp implements the Expression interface.
func (l *Ln) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewLn(child))
}

// Type returns the resultant type of the function.
func (l *Ln) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the sql.Expression interface.
func (l *Ln) IsNullable() bool {
	return l.Child.IsNullable()
}

// Eval implements the Expression interface.
func (l *Ln) Eval(
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
	return computeLog(val.(float64), math.E)
}

// Log2 is a function that returns the binary logarithm of a value.
type Log2 struct {
	expression.UnaryExpression
}

// NewLog2 creates a new Log2 expression.
func NewLog2(e sql.Expression) sql.Expression {
	return &Log2{expression.UnaryExpression{Child: e}}
}

func (l *Log2) String() string {
	return fmt.Sprintf("log2(%s)", l.Child)
}

// TransformUp implements the Expression interface.
func (l *Log2) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewLog2(child))
}

// Type returns the resultant type of the function.
func (l *Log2) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the sql.Expression interface.
func (l *Log2) IsNullable() bool {
	return l.Child.IsNullable()
}

// Eval implements the Expression interface.
func (l *Log2) Eval(
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
	return computeLog(val.(float64), float64(2))
}

// Log10 is a function that returns the decimal logarithm of a value.
type Log10 struct {
	expression.UnaryExpression
}

// NewLog10 creates a new Log10 expression.
func NewLog10(e sql.Expression) sql.Expression {
	return &Log10{expression.UnaryExpression{Child: e}}
}

func (l *Log10) String() string {
	return fmt.Sprintf("log10(%s)", l.Child)
}

// TransformUp implements the Expression interface.
func (l *Log10) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewLog10(child))
}

// Type returns the resultant type of the function.
func (l *Log10) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the sql.Expression interface.
func (l *Log10) IsNullable() bool {
	return l.Child.IsNullable()
}

// Eval implements the Expression interface.
func (l *Log10) Eval(
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
	return computeLog(val.(float64), float64(10))
}

// Log is a function that returns the natural logarithm of a value.
type Log struct {
	expression.BinaryExpression
}

// NewLn creates a new Log expression.
func NewLog(args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen == 0 || argLen > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("1 or 2", argLen)
	}

	var right sql.Expression
	if len(args) == 2 {
		right = args[1]
	}

	return &Log{expression.BinaryExpression{Left: args[0], Right: right}}, nil
}

func (l *Log) String() string {
	if l.Right == nil {
		return fmt.Sprintf("log(%s)", l.Left)
	}

	return fmt.Sprintf("log(%s, %s)", l.Left, l.Right)
}

// TransformUp implements the Expression interface.
func (l *Log) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	var args = make([]sql.Expression, 2)
	arg, err := l.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}
	args[0] = arg

	args[1] = nil
	if l.Right != nil {
		arg, err := l.Right.TransformUp(f)
		if err != nil {
			return nil, err
		}
		args[1] = arg
	}

	expr, err := NewLog(args...)
	if err != nil {
		return nil, err
	}

	return f(expr)
}

// Children implements the Expression interface.
func (l *Log) Children() []sql.Expression {
	if l.Right == nil {
		return []sql.Expression{l.Left}
	}

	return []sql.Expression{l.Left, l.Right}
}

// Type returns the resultant type of the function.
func (l *Log) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the Expression interface.
func (l *Log) IsNullable() bool { return l.Left.IsNullable() }

// Resolved implements the Expression interface.
func (l *Log) Resolved() bool {
	return l.Left.Resolved() && (l.Right == nil || l.Right.Resolved())
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

	if l.Right != nil {
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
	} else {
		return computeLog(lhs.(float64), math.E)
	}
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
