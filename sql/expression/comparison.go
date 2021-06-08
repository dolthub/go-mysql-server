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

package expression

import (
	"fmt"
	"sync"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/regex"
	"github.com/dolthub/go-mysql-server/sql"
)

var ErrInvalidRegexp = errors.NewKind("Invalid regular expression: %s")

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
}

func newComparison(left, right sql.Expression) comparison {
	return comparison{BinaryExpression{left, right}}
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

	if sql.TypesEqual(c.Left().Type(), c.Right().Type()) {
		return c.Left().Type().Compare(left, right)
	}

	var compareType sql.Type
	left, right, compareType, err = c.castLeftAndRight(left, right)
	if err != nil {
		return 0, err
	}

	return compareType.Compare(left, right)
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

func (c *comparison) castLeftAndRight(left, right interface{}) (interface{}, interface{}, sql.Type, error) {
	leftType := c.Left().Type()
	rightType := c.Right().Type()
	if sql.IsTuple(leftType) && sql.IsTuple(rightType) {
		return left, right, c.Left().Type(), nil
	}

	if sql.IsNumber(leftType) || sql.IsNumber(rightType) {
		if sql.IsDecimal(leftType) || sql.IsDecimal(rightType) {
			//TODO: We need to set to the actual DECIMAL type
			l, r, err := convertLeftAndRight(left, right, ConvertToDecimal)
			if err != nil {
				return nil, nil, nil, err
			}

			if sql.IsDecimal(leftType) {
				return l, r, leftType, nil
			} else {
				return l, r, rightType, nil
			}
		}

		if sql.IsFloat(leftType) || sql.IsFloat(rightType) {
			l, r, err := convertLeftAndRight(left, right, ConvertToDouble)
			if err != nil {
				return nil, nil, nil, err
			}

			return l, r, sql.Float64, nil
		}

		if sql.IsSigned(leftType) || sql.IsSigned(rightType) {
			l, r, err := convertLeftAndRight(left, right, ConvertToSigned)
			if err != nil {
				return nil, nil, nil, err
			}

			return l, r, sql.Int64, nil
		}

		l, r, err := convertLeftAndRight(left, right, ConvertToUnsigned)
		if err != nil {
			return nil, nil, nil, err
		}

		return l, r, sql.Uint64, nil
	}

	if sql.IsTime(leftType) || sql.IsTime(rightType) {
		l, r, err := convertLeftAndRight(left, right, ConvertToDatetime)
		if err != nil {
			return nil, nil, nil, err
		}

		return l, r, sql.Datetime, nil
	}

	left, right, err := convertLeftAndRight(left, right, ConvertToChar)
	if err != nil {
		return nil, nil, nil, err
	}

	return left, right, sql.LongText, nil
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
func (e *Equals) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 2)
	}
	return NewEquals(children[0], children[1]), nil
}

func (e *Equals) String() string {
	return fmt.Sprintf("(%s = %s)", e.Left(), e.Right())
}

func (e *Equals) DebugString() string {
	return fmt.Sprintf("(%s = %s)", sql.DebugString(e.Left()), sql.DebugString(e.Right()))
}

// NullSafeEquals is a comparison that checks an expression is equal to
// another, where NULLs do not coalesce to NULL and two NULLs compare equal to
// each other.
type NullSafeEquals struct {
	comparison
}

// NewNullSafeEquals returns a new NullSafeEquals expression.
func NewNullSafeEquals(left sql.Expression, right sql.Expression) *NullSafeEquals {
	return &NullSafeEquals{newComparison(left, right)}
}

// Type implements the Expression interface.
func (e *NullSafeEquals) Type() sql.Type {
	return sql.Int8
}

func (e *NullSafeEquals) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	left, right, err := e.evalLeftAndRight(ctx, row)
	if err != nil {
		return 0, err
	}

	if left == nil && right == nil {
		return 0, nil
	} else if left == nil {
		return 1, nil
	} else if right == nil {
		return -1, nil
	}

	if sql.TypesEqual(e.Left().Type(), e.Right().Type()) {
		return e.Left().Type().Compare(left, right)
	}

	var compareType sql.Type
	left, right, compareType, err = e.castLeftAndRight(left, right)
	if err != nil {
		return 0, err
	}

	return compareType.Compare(left, right)
}

// Eval implements the Expression interface.
func (e *NullSafeEquals) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	result, err := e.Compare(ctx, row)
	if err != nil {
		return nil, err
	}

	if result == 0 {
		return 1, nil
	}
	return 0, nil
}

// WithChildren implements the Expression interface.
func (e *NullSafeEquals) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 2)
	}
	return NewNullSafeEquals(children[0], children[1]), nil
}

func (e *NullSafeEquals) String() string {
	return fmt.Sprintf("(%s <=> %s)", e.Left(), e.Right())
}

func (e *NullSafeEquals) DebugString() string {
	return fmt.Sprintf("(%s <=> %s)", sql.DebugString(e.Left()), sql.DebugString(e.Right()))
}

// Regexp is a comparison that checks an expression matches a regexp.
type Regexp struct {
	comparison
	pool   *sync.Pool
	cached bool
	once   sync.Once
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
		once:       sync.Once{},
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

type matcherErrTuple struct {
	matcher regex.DisposableMatcher
	err     error
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

	var matcher regex.DisposableMatcher

	if !re.cached {
		right, rerr := re.evalRight(ctx, row)
		if rerr != nil || right == nil {
			return right, rerr
		}
		matcher, err = regex.NewDisposableMatcher(regex.Default(), *right)
	} else {
		re.once.Do(func() {
			right, err := re.evalRight(ctx, row)
			re.pool = &sync.Pool{
				New: func() interface{} {
					if err != nil || right == nil {
						return matcherErrTuple{nil, err}
					}
					m, e := regex.NewDisposableMatcher(regex.Default(), *right)
					return matcherErrTuple{m, e}
				},
			}
		})
		met := re.pool.Get().(matcherErrTuple)
		matcher, err = met.matcher, met.err
	}

	if err != nil {
		return nil, ErrInvalidRegexp.New(err.Error())
	} else if matcher == nil {
		return nil, nil
	}

	ok := matcher.Match(left.(string))

	if !re.cached {
		matcher.Dispose()
	} else {
		re.pool.Put(matcherErrTuple{matcher, nil})
	}
	return ok, nil
}

func (re *Regexp) evalRight(ctx *sql.Context, row sql.Row) (*string, error) {
	right, err := re.Right().Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if right == nil {
		return nil, nil
	}
	right, err = sql.LongText.Convert(right)
	if err != nil {
		return nil, err
	}
	s := right.(string)
	return &s, nil
}

// WithChildren implements the Expression interface.
func (re *Regexp) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(re, len(children), 2)
	}
	return NewRegexp(children[0], children[1]), nil
}

func (re *Regexp) String() string {
	return fmt.Sprintf("(%s REGEXP %s)", re.Left(), re.Right())
}

func (re *Regexp) DebugString() string {
	return fmt.Sprintf("(%s REGEXP %s)", sql.DebugString(re.Left()), sql.DebugString(re.Right()))
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
func (gt *GreaterThan) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(gt, len(children), 2)
	}
	return NewGreaterThan(children[0], children[1]), nil
}

func (gt *GreaterThan) String() string {
	return fmt.Sprintf("(%s > %s)", gt.Left(), gt.Right())
}

func (gt *GreaterThan) DebugString() string {
	return fmt.Sprintf("(%s > %s)", sql.DebugString(gt.Left()), sql.DebugString(gt.Right()))
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
func (lt *LessThan) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(lt, len(children), 2)
	}
	return NewLessThan(children[0], children[1]), nil
}

func (lt *LessThan) String() string {
	return fmt.Sprintf("(%s < %s)", lt.Left(), lt.Right())
}

func (lt *LessThan) DebugString() string {
	return fmt.Sprintf("(%s < %s)", sql.DebugString(lt.Left()), sql.DebugString(lt.Right()))
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
func (gte *GreaterThanOrEqual) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(gte, len(children), 2)
	}
	return NewGreaterThanOrEqual(children[0], children[1]), nil
}

func (gte *GreaterThanOrEqual) String() string {
	return fmt.Sprintf("(%s >= %s)", gte.Left(), gte.Right())
}

func (gte *GreaterThanOrEqual) DebugString() string {
	return fmt.Sprintf("(%s >= %s)", sql.DebugString(gte.Left()), sql.DebugString(gte.Right()))
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
func (lte *LessThanOrEqual) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(lte, len(children), 2)
	}
	return NewLessThanOrEqual(children[0], children[1]), nil
}

func (lte *LessThanOrEqual) String() string {
	return fmt.Sprintf("(%s <= %s)", lte.Left(), lte.Right())
}

func (lte *LessThanOrEqual) DebugString() string {
	return fmt.Sprintf("(%s <= %s)", sql.DebugString(lte.Left()), sql.DebugString(lte.Right()))
}

var (
	// ErrUnsupportedInOperand is returned when there is an invalid righthand
	// operand in an IN operator.
	ErrUnsupportedInOperand = errors.NewKind("right operand in IN operation must be tuple, but is %T")
	// ErrInvalidOperandColumns is returned when the columns in the left operand
	// and the elements of the right operand don't match.
	ErrInvalidOperandColumns = errors.NewKind("operand should have %d columns, but has %d")
)
