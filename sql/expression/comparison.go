package expression

import (
	"fmt"
	"sync"

	"github.com/src-d/go-mysql-server/internal/regex"
	"github.com/src-d/go-mysql-server/sql"
	errors "gopkg.in/src-d/go-errors.v1"
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
			l, r, err := convertLeftAndRight(left, right, ConvertToDecimal)
			if err != nil {
				return nil, nil, err
			}

			c.compareType = sql.Float64
			return l, r, nil
		}

		if sql.IsSigned(c.Left().Type()) || sql.IsSigned(c.Right().Type()) {
			l, r, err := convertLeftAndRight(left, right, ConvertToSigned)
			if err != nil {
				return nil, nil, err
			}

			c.compareType = sql.Int64
			return l, r, nil
		}

		l, r, err := convertLeftAndRight(left, right, ConvertToUnsigned)
		if err != nil {
			return nil, nil, err
		}

		c.compareType = sql.Uint64
		return l, r, nil
	}

	left, right, err := convertLeftAndRight(left, right, ConvertToChar)
	if err != nil {
		return nil, nil, err
	}

	c.compareType = sql.LongText
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

// WithChildren implements the Expression interface.
func (e *Equals) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 2)
	}
	return NewEquals(children[0], children[1]), nil
}

func (e *Equals) String() string {
	return fmt.Sprintf("%s = %s", e.Left(), e.Right())
}

// Regexp is a comparison that checks an expression matches a regexp.
type Regexp struct {
	comparison
	pool   *sync.Pool
	cached bool
}

// NewRegexp creates a new Regexp expression.
func NewRegexp(left sql.Expression, right sql.Expression) *Regexp {
	var cached = true
	sql.Inspect(right, func(e sql.Expression) bool {
		if _, ok := e.(*GetField); ok {
			cached = false
		}
		return true
	})

	return &Regexp{
		comparison: newComparison(left, right),
		pool:       nil,
		cached:     cached,
	}
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
	left, err := re.Left().Eval(ctx, row)
	if err != nil || left == nil {
		return nil, err
	}
	left, err = sql.LongText.Convert(left)
	if err != nil {
		return nil, err
	}

	var (
		matcher  regex.Matcher
		disposer regex.Disposer
		right    interface{}
	)
	// eval right and convert to text
	if !re.cached || re.pool == nil {
		right, err = re.Right().Eval(ctx, row)
		if err != nil || right == nil {
			return nil, err
		}
		right, err = sql.LongText.Convert(right)
		if err != nil {
			return nil, err
		}
	}
	// for non-cached regex every time create a new matcher
	if !re.cached {
		matcher, disposer, err = regex.New(regex.Default(), right.(string))
	} else {
		if re.pool == nil {
			re.pool = &sync.Pool{
				New: func() interface{} {
					r, _, e := regex.New(regex.Default(), right.(string))
					if e != nil {
						err = e
						return nil
					}
					return r
				},
			}
		}
		if obj := re.pool.Get(); obj != nil {
			matcher = obj.(regex.Matcher)
		}
	}
	if matcher == nil {
		return nil, err
	}

	ok := matcher.Match(left.(string))

	if !re.cached {
		disposer.Dispose()
	} else if re.pool != nil {
		re.pool.Put(matcher)
	}
	return ok, nil
}

// WithChildren implements the Expression interface.
func (re *Regexp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(re, len(children), 2)
	}
	return NewRegexp(children[0], children[1]), nil
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

// WithChildren implements the Expression interface.
func (gt *GreaterThan) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(gt, len(children), 2)
	}
	return NewGreaterThan(children[0], children[1]), nil
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

// WithChildren implements the Expression interface.
func (lt *LessThan) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(lt, len(children), 2)
	}
	return NewLessThan(children[0], children[1]), nil
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

// WithChildren implements the Expression interface.
func (gte *GreaterThanOrEqual) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(gte, len(children), 2)
	}
	return NewGreaterThanOrEqual(children[0], children[1]), nil
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

// WithChildren implements the Expression interface.
func (lte *LessThanOrEqual) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(lte, len(children), 2)
	}
	return NewLessThanOrEqual(children[0], children[1]), nil
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
	typ := in.Left().Type().Promote()
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
	case *Subquery:
		if leftElems > 1 {
			return nil, ErrInvalidOperandColumns.New(leftElems, 1)
		}

		typ := right.Type()
		values, err := right.EvalMultiple(ctx)
		if err != nil {
			return nil, err
		}

		for _, val := range values {
			val, err = typ.Convert(val)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, val)
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

// WithChildren implements the Expression interface.
func (in *In) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewIn(children[0], children[1]), nil
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
	typ := in.Left().Type().Promote()
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
	case *Subquery:
		if leftElems > 1 {
			return nil, ErrInvalidOperandColumns.New(leftElems, 1)
		}

		typ := right.Type()
		values, err := right.EvalMultiple(ctx)
		if err != nil {
			return nil, err
		}

		for _, val := range values {
			val, err = typ.Convert(val)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, val)
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

// WithChildren implements the Expression interface.
func (in *NotIn) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewNotIn(children[0], children[1]), nil
}

func (in *NotIn) String() string {
	return fmt.Sprintf("%s NOT IN %s", in.Left(), in.Right())
}

// Children implements the Expression interface.
func (in *NotIn) Children() []sql.Expression {
	return []sql.Expression{in.Left(), in.Right()}
}
