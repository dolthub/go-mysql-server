package expression

import (
	"fmt"
	"regexp"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Compararer implements a comparison expression.
type Comparer interface {
	sql.Expression
	Compare(session sql.Session, row sql.Row) (int, error)
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
func (c *comparison) Compare(session sql.Session, row sql.Row) (int, error) {
	left, right, err := c.evalLeftAndRight(session, row)
	if err != nil {
		return 0, err
	}

	if left == nil || right == nil {
		return 0, ErrNilOperand.New()
	}

	if c.Left().Type() == c.Right().Type() {
		return c.Left().Type().Compare(left, right), nil
	}

	left, right, err = c.castLeftAndRight(left, right)
	if err != nil {
		return 0, err
	}

	return c.compareType.Compare(left, right), nil
}

func (c *comparison) evalLeftAndRight(session sql.Session, row sql.Row) (interface{}, interface{}, error) {
	left, err := c.Left().Eval(session, row)
	if err != nil {
		return nil, nil, err
	}

	right, err := c.Right().Eval(session, row)
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
func (e *Equals) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	result, err := e.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 0, nil
}

// TransformUp implements the Expression interface.
func (e *Equals) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
}

// NewRegexp creates a new Regexp expression.
func NewRegexp(left sql.Expression, right sql.Expression) *Regexp {
	return &Regexp{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (re *Regexp) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	if sql.IsText(re.Left().Type()) && sql.IsText(re.Right().Type()) {
		return re.compareRegexp(session, row)
	}

	result, err := re.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 0, nil
}

func (re *Regexp) compareRegexp(session sql.Session, row sql.Row) (interface{}, error) {
	left, right, err := re.evalLeftAndRight(session, row)
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

	reg, err := regexp.Compile(right.(string))
	if err != nil {
		return false, err
	}

	return reg.MatchString(left.(string)), nil
}

// TransformUp implements the Expression interface.
func (re *Regexp) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
func (gt *GreaterThan) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	result, err := gt.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == 1, nil
}

// TransformUp implements the Expression interface.
func (gt *GreaterThan) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
func (lt *LessThan) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	result, err := lt.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result == -1, nil
}

// TransformUp implements the Expression interface.
func (lt *LessThan) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
func (gte *GreaterThanOrEqual) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	result, err := gte.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result > -1, nil
}

// TransformUp implements the Expression interface.
func (gte *GreaterThanOrEqual) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
func (lte *LessThanOrEqual) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	result, err := lte.Compare(session, row)
	if err != nil {
		if ErrNilOperand.Is(err) {
			return nil, nil
		}

		return nil, err
	}

	return result < 1, nil
}

// TransformUp implements the Expression interface.
func (lte *LessThanOrEqual) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
