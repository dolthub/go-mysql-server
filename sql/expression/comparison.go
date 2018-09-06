package expression

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/internal/regex"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Comparer implements a comparison expression.
type Comparer interface {
	sql.Expression
	Compare(ctx *sql.Context, row sql.Row) (int, error)
	Left() sql.Expression
	Right() sql.Expression
}

// ErrNilOperand ir returned if some or both of the comparison's operands is nil.
var ErrNilOperand = errors.NewKind("nil operand found in comparison")

type comparison struct {
	BinaryExpression
	compareType sql.Type
}

func newComparison(left, right sql.Expression) comparison {
	return comparison{BinaryExpression{left, right}, nil}
}

// Compare the two given values using the types of the expressions in the comparison.
// Since both types should be equal, it does not matter which type is used, but for
// reference, the left type is always used.
func (c *comparison) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	left, right, err := c.evalLeftAndRight(ctx, row)
	if err != nil {
		return 0, err
	}

	if left == nil || right == nil {
		return 0, ErrNilOperand.New()
	}

	if c.Left().Type() == c.Right().Type() {
		return c.Left().Type().Compare(left, right)
	}

	left, right, err = c.castLeftAndRight(left, right)
	if err != nil {
		return 0, err
	}

	return c.compareType.Compare(left, right)
}

func (c *comparison) evalLeftAndRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	left, err := c.Left().Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	right, err := c.Right().Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	return left, right, nil
}

func (c *comparison) castLeftAndRight(left, right interface{}) (interface{}, interface{}, error) {
	if sql.IsNumber(c.Left().Type()) || sql.IsNumber(c.Right().Type()) {
		if sql.IsDecimal(c.Left().Type()) || sql.IsDecimal(c.Right().Type()) {
			left, right, err := convertLeftAndRight(left, right, ConvertToDecimal)
			if err != nil {
				return nil, nil, err
			}

			c.compareType = sql.Float64
			return left, right, nil
		}

		if sql.IsSigned(c.Left().Type()) || sql.IsSigned(c.Right().Type()) {
			left, right, err := convertLeftAndRight(left, right, ConvertToSigned)
			if err != nil {
				return nil, nil, err
			}

			c.compareType = sql.Int64
			return left, right, nil
		}

		left, right, err := convertLeftAndRight(left, right, ConvertToUnsigned)
		if err != nil {
			return nil, nil, err
		}

		c.compareType = sql.Uint64
		return left, right, nil
	}

	left, right, err := convertLeftAndRight(left, right, ConvertToChar)
	if err != nil {
		return nil, nil, err
	}

	c.compareType = sql.Text
	return left, right, nil
}

func convertLeftAndRight(left, right interface{}, convertTo string) (interface{}, interface{}, error) {
	l, err := convertValue(left, convertTo)
	if err != nil {
		return nil, nil, err
	}

	r, err := convertValue(right, convertTo)
	if err != nil {
		return nil, nil, err
	}

	return l, r, nil
}

// Type implements the Expression interface.
func (*comparison) Type() sql.Type {
	return sql.Boolean
}

// Left implements Comparer interface
func (c *comparison) Left() sql.Expression { return c.BinaryExpression.Left }

// Right implements Comparer interface
func (c *comparison) Right() sql.Expression { return c.BinaryExpression.Right }

// Equals is a comparison that checks an expression is equal to another.
type Equals struct {
	comparison
}

// NewEquals returns a new Equals expression.
func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	return &Equals{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (e *Equals) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := e.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 0, nil
}

// TransformUp implements the Expression interface.
func (e *Equals) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := e.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := e.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewEquals(left, right))
}

func (e *Equals) String() string {
	return fmt.Sprintf("%s = %s", e.Left(), e.Right())
}

// Regexp is a comparison that checks an expression matches a regexp.
type Regexp struct {
	comparison
	r regex.Matcher
}

// NewRegexp creates a new Regexp expression.
func NewRegexp(left sql.Expression, right sql.Expression) *Regexp {
	return &Regexp{newComparison(left, right), nil}
}

// Eval implements the Expression interface.
func (re *Regexp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if sql.IsText(re.Left().Type()) && sql.IsText(re.Right().Type()) {
		return re.compareRegexp(ctx, row)
	}

	result, err := re.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 0, nil
}

func (re *Regexp) compareRegexp(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, right, err := re.evalLeftAndRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil || right == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err = sql.Text.Convert(right)
	if err != nil {
		return nil, err
	}

	if re.r == nil {
		re.r, err = regex.New(regex.Default(), right.(string))
		if err != nil {
			return false, err
		}
	}

	return re.r.Match(left.(string)), nil
}

// TransformUp implements the Expression interface.
func (re *Regexp) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := re.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := re.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewRegexp(left, right))
}

func (re *Regexp) String() string {
	return fmt.Sprintf("%s REGEXP %s", re.Left(), re.Right())
}

// GreaterThan is a comparison that checks an expression is greater than another.
type GreaterThan struct {
	comparison
}

// NewGreaterThan creates a new GreaterThan expression.
func NewGreaterThan(left sql.Expression, right sql.Expression) *GreaterThan {
	return &GreaterThan{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (gt *GreaterThan) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := gt.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 1, nil
}

// TransformUp implements the Expression interface.
func (gt *GreaterThan) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := gt.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := gt.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewGreaterThan(left, right))
}

func (gt *GreaterThan) String() string {
	return fmt.Sprintf("%s > %s", gt.Left(), gt.Right())
}

// LessThan is a comparison that checks an expression is less than another.
type LessThan struct {
	comparison
}

// NewLessThan creates a new LessThan expression.
func NewLessThan(left sql.Expression, right sql.Expression) *LessThan {
	return &LessThan{newComparison(left, right)}
}

// Eval implements the expression interface.
func (lt *LessThan) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := lt.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == -1, nil
}

// TransformUp implements the Expression interface.
func (lt *LessThan) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := lt.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := lt.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLessThan(left, right))
}

func (lt *LessThan) String() string {
	return fmt.Sprintf("%s < %s", lt.Left(), lt.Right())
}

// GreaterThanOrEqual is a comparison that checks an expression is greater or equal to
// another.
type GreaterThanOrEqual struct {
	comparison
}

// NewGreaterThanOrEqual creates a new GreaterThanOrEqual
func NewGreaterThanOrEqual(left sql.Expression, right sql.Expression) *GreaterThanOrEqual {
	return &GreaterThanOrEqual{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (gte *GreaterThanOrEqual) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := gte.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result > -1, nil
}

// TransformUp implements the Expression interface.
func (gte *GreaterThanOrEqual) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := gte.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := gte.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewGreaterThanOrEqual(left, right))
}

func (gte *GreaterThanOrEqual) String() string {
	return fmt.Sprintf("%s >= %s", gte.Left(), gte.Right())
}

// LessThanOrEqual is a comparison that checks an expression is equal or lower than
// another.
type LessThanOrEqual struct {
	comparison
}

// NewLessThanOrEqual creates a LessThanOrEqual expression.
func NewLessThanOrEqual(left sql.Expression, right sql.Expression) *LessThanOrEqual {
	return &LessThanOrEqual{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (lte *LessThanOrEqual) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := lte.Compare(ctx, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result < 1, nil
}

// TransformUp implements the Expression interface.
func (lte *LessThanOrEqual) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := lte.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := lte.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLessThanOrEqual(left, right))
}

func (lte *LessThanOrEqual) String() string {
	return fmt.Sprintf("%s <= %s", lte.Left(), lte.Right())
}

var (
	// ErrUnsupportedInOperand is returned when there is an invalid righthand
	// operand in an IN operator.
	ErrUnsupportedInOperand = errors.NewKind("right operand in IN operation must be tuple, but is %T")
	// ErrInvalidOperandColumns is returned when the columns in the left operand
	// and the elements of the right operand don't match.
	ErrInvalidOperandColumns = errors.NewKind("operand should have %d columns, but has %d")
)

// In is a comparison that checks an expression is inside a list of expressions.
type In struct {
	comparison
}

// NewIn creates a In expression.
func NewIn(left sql.Expression, right sql.Expression) *In {
	return &In{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (in *In) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left().Type()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, err
	}

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	// TODO: support subqueries
	switch right := in.Right().(type) {
	case Tuple:
		for _, el := range right {
			if sql.NumColumns(el.Type()) != leftElems {
				return nil, ErrInvalidOperandColumns.New(leftElems, sql.NumColumns(el.Type()))
			}
		}

		for _, el := range right {
			right, err := el.Eval(ctx, row)
			if err != nil {
				return nil, err
			}

			right, err = typ.Convert(right)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, right)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return true, nil
			}
		}

		return false, nil
	default:
		return nil, ErrUnsupportedInOperand.New(right)
	}
}

// TransformUp implements the Expression interface.
func (in *In) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := in.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := in.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewIn(left, right))
}

func (in *In) String() string {
	return fmt.Sprintf("%s IN %s", in.Left(), in.Right())
}

// Children implements the Expression interface.
func (in *In) Children() []sql.Expression {
	return []sql.Expression{in.Left(), in.Right()}
}

// NotIn is a comparison that checks an expression is not inside a list of expressions.
type NotIn struct {
	comparison
}

// NewNotIn creates a In expression.
func NewNotIn(left sql.Expression, right sql.Expression) *NotIn {
	return &NotIn{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (in *NotIn) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left().Type()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, err
	}

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	// TODO: support subqueries
	switch right := in.Right().(type) {
	case Tuple:
		for _, el := range right {
			if sql.NumColumns(el.Type()) != leftElems {
				return nil, ErrInvalidOperandColumns.New(leftElems, sql.NumColumns(el.Type()))
			}
		}

		for _, el := range right {
			right, err := el.Eval(ctx, row)
			if err != nil {
				return nil, err
			}

			right, err = typ.Convert(right)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, right)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return false, nil
			}
		}

		return true, nil
	default:
		return nil, ErrUnsupportedInOperand.New(right)
	}
}

// TransformUp implements the Expression interface.
func (in *NotIn) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := in.Left().TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := in.Right().TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewNotIn(left, right))
}

func (in *NotIn) String() string {
	return fmt.Sprintf("%s NOT IN %s", in.Left(), in.Right())
}

// Children implements the Expression interface.
func (in *NotIn) Children() []sql.Expression {
	return []sql.Expression{in.Left(), in.Right()}
}
