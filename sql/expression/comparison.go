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
	"github.com/dolthub/go-mysql-server/sql/types"
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

// ContainsImpreciseComparison searches an expression tree for comparison
// expressions that require a conversion or type promotion.
// This utility helps determine if filter predicates can be pushed down.
func ContainsImpreciseComparison(e sql.Expression) bool {
	var imprecise bool
	sql.Inspect(e, func(expr sql.Expression) bool {
		if cmp, ok := expr.(Comparer); ok {
			left, right := cmp.Left().Type(), cmp.Right().Type()

			// integer comparisons are exact
			if types.IsInteger(left) && types.IsInteger(right) {
				return true
			}

			// comparisons with type conversions are sometimes imprecise
			if !left.Equals(right) {
				imprecise = true
				return false
			}
		}
		return true
	})
	return imprecise
}

type comparison struct {
	BinaryExpression
}

func newComparison(left, right sql.Expression) comparison {
	return comparison{BinaryExpression{left, right}}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *comparison) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	leftCollation, leftCoercibility := sql.GetCoercibility(ctx, c.Left())
	rightCollation, rightCoercibility := sql.GetCoercibility(ctx, c.Right())
	return sql.ResolveCoercibility(leftCollation, leftCoercibility, rightCollation, rightCoercibility)
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

	if types.TypesEqual(c.Left().Type(), c.Right().Type()) {
		return c.Left().Type().Compare(left, right)
	}

	// ENUM, SET, and TIME must be excluded when doing comparisons, as they're too restrictive to use as a comparison
	// base.
	//
	// The best overall method would be to assign type priority. For example, INT would have a higher priority than
	// TINYINT. This could then be combined with the origin of the value (table column, procedure param, etc.) to
	// determine the best type for any comparison (tie-breakers can be simple rules such as the current left preference).
	var compareType sql.Type
	collationPreference := sql.Collation_Default
	switch c.Left().(type) {
	case *GetField, *UserVar, *SystemVar, *ProcedureParam:
		compareType = c.Left().Type()
		if twc, ok := compareType.(sql.TypeWithCollation); ok {
			collationPreference = twc.Collation()
		}
	default:
		switch c.Right().(type) {
		case *GetField, *UserVar, *SystemVar, *ProcedureParam:
			compareType = c.Right().Type()
			if twc, ok := compareType.(sql.TypeWithCollation); ok {
				collationPreference = twc.Collation()
			}
		}
	}
	if compareType != nil {
		_, isEnum := compareType.(sql.EnumType)
		_, isSet := compareType.(sql.SetType)
		_, isTime := compareType.(types.TimeType)
		if !isEnum && !isSet && !isTime {
			compareType = nil
		}
	}
	if compareType == nil {
		left, right, compareType, err = c.castLeftAndRight(left, right)
		if err != nil {
			return 0, err
		}
	}
	if types.IsTextOnly(compareType) {
		collationPreference, _ = c.CollationCoercibility(ctx)
		if err != nil {
			return 0, err
		}
		stringCompareType := compareType.(sql.StringType)
		compareType = types.MustCreateString(stringCompareType.Type(), stringCompareType.Length(), collationPreference)
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
	if types.IsTuple(leftType) && types.IsTuple(rightType) {
		return left, right, c.Left().Type(), nil
	}

	if types.IsTime(leftType) || types.IsTime(rightType) {
		l, r, err := convertLeftAndRight(left, right, ConvertToDatetime)
		if err != nil {
			return nil, nil, nil, err
		}

		return l, r, types.DatetimeMaxPrecision, nil
	}

	if types.IsBinaryType(leftType) || types.IsBinaryType(rightType) {
		l, r, err := convertLeftAndRight(left, right, ConvertToBinary)
		if err != nil {
			return nil, nil, nil, err
		}
		return l, r, types.LongBlob, nil
	}

	if types.IsNumber(leftType) || types.IsNumber(rightType) {
		if types.IsDecimal(leftType) || types.IsDecimal(rightType) {
			//TODO: We need to set to the actual DECIMAL type
			l, r, err := convertLeftAndRight(left, right, ConvertToDecimal)
			if err != nil {
				return nil, nil, nil, err
			}

			if types.IsDecimal(leftType) {
				return l, r, leftType, nil
			} else {
				return l, r, rightType, nil
			}
		}

		if types.IsFloat(leftType) || types.IsFloat(rightType) {
			l, r, err := convertLeftAndRight(left, right, ConvertToDouble)
			if err != nil {
				return nil, nil, nil, err
			}

			return l, r, types.Float64, nil
		}

		if types.IsSigned(leftType) && types.IsSigned(rightType) {
			l, r, err := convertLeftAndRight(left, right, ConvertToSigned)
			if err != nil {
				return nil, nil, nil, err
			}

			return l, r, types.Int64, nil
		}

		if types.IsUnsigned(leftType) && types.IsUnsigned(rightType) {
			l, r, err := convertLeftAndRight(left, right, ConvertToUnsigned)
			if err != nil {
				return nil, nil, nil, err
			}

			return l, r, types.Uint64, nil
		}

		l, r, err := convertLeftAndRight(left, right, ConvertToDouble)
		if err != nil {
			return nil, nil, nil, err
		}

		return l, r, types.Float64, nil
	}

	left, right, err := convertLeftAndRight(left, right, ConvertToChar)
	if err != nil {
		return nil, nil, nil, err
	}

	return left, right, types.LongText, nil
}

func convertLeftAndRight(left, right interface{}, convertTo string) (interface{}, interface{}, error) {
	l, err := convertValue(left, convertTo, nil, 0, 0)
	if err != nil {
		return nil, nil, err
	}

	r, err := convertValue(right, convertTo, nil, 0, 0)
	if err != nil {
		return nil, nil, err
	}

	return l, r, nil
}

// Type implements the Expression interface.
func (*comparison) Type() sql.Type {
	return types.Boolean
}

// Left implements Comparer interface
func (c *comparison) Left() sql.Expression { return c.BinaryExpression.Left }

// Right implements Comparer interface
func (c *comparison) Right() sql.Expression { return c.BinaryExpression.Right }

// Equals is a comparison that checks an expression is equal to another.
type Equals struct {
	comparison
}

var _ sql.Expression = (*Equals)(nil)
var _ sql.CollationCoercible = (*Equals)(nil)

// NewEquals returns a new Equals expression.
func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	return &Equals{newComparison(left, right)}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (e *Equals) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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
	if e == nil {
		return ""
	}
	return fmt.Sprintf("(%s = %s)", e.Left(), e.Right())
}

func (e *Equals) DebugString() string {
	if e == nil {
		return ""
	}
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Eq")
	children := []string{sql.DebugString(e.Left()), sql.DebugString(e.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// NullSafeEquals is a comparison that checks an expression is equal to
// another, where NULLs do not coalesce to NULL and two NULLs compare equal to
// each other.
type NullSafeEquals struct {
	comparison
}

var _ sql.Expression = (*NullSafeEquals)(nil)
var _ sql.CollationCoercible = (*NullSafeEquals)(nil)

// NewNullSafeEquals returns a new NullSafeEquals expression.
func NewNullSafeEquals(left sql.Expression, right sql.Expression) *NullSafeEquals {
	return &NullSafeEquals{newComparison(left, right)}
}

// Type implements the Expression interface.
func (e *NullSafeEquals) Type() sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (e *NullSafeEquals) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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

	if types.TypesEqual(e.Left().Type(), e.Right().Type()) {
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

	return result == 0, nil
}

// WithChildren implements the Expression interface.
func (e *NullSafeEquals) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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

var _ sql.Expression = (*Regexp)(nil)
var _ sql.CollationCoercible = (*Regexp)(nil)

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
	if types.IsText(re.Right().Type()) {
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
	left, _, err = types.LongText.Convert(left)
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
	right, _, err = types.LongText.Convert(right)
	if err != nil {
		return nil, err
	}
	s := right.(string)
	return &s, nil
}

// WithChildren implements the Expression interface.
func (re *Regexp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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

var _ sql.Expression = (*GreaterThan)(nil)
var _ sql.CollationCoercible = (*GreaterThan)(nil)

// NewGreaterThan creates a new GreaterThan expression.
func NewGreaterThan(left sql.Expression, right sql.Expression) *GreaterThan {
	return &GreaterThan{newComparison(left, right)}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (gt *GreaterThan) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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
	return fmt.Sprintf("(%s > %s)", gt.Left(), gt.Right())
}

func (gt *GreaterThan) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GreaterThan")
	children := []string{sql.DebugString(gt.Left()), sql.DebugString(gt.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// LessThan is a comparison that checks an expression is less than another.
type LessThan struct {
	comparison
}

var _ sql.Expression = (*LessThan)(nil)
var _ sql.CollationCoercible = (*LessThan)(nil)

// NewLessThan creates a new LessThan expression.
func NewLessThan(left sql.Expression, right sql.Expression) *LessThan {
	return &LessThan{newComparison(left, right)}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (lt *LessThan) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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
	return fmt.Sprintf("(%s < %s)", lt.Left(), lt.Right())
}

func (lt *LessThan) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("LessThan")
	children := []string{sql.DebugString(lt.Left()), sql.DebugString(lt.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// GreaterThanOrEqual is a comparison that checks an expression is greater or equal to
// another.
type GreaterThanOrEqual struct {
	comparison
}

var _ sql.Expression = (*GreaterThanOrEqual)(nil)
var _ sql.CollationCoercible = (*GreaterThanOrEqual)(nil)

// NewGreaterThanOrEqual creates a new GreaterThanOrEqual
func NewGreaterThanOrEqual(left sql.Expression, right sql.Expression) *GreaterThanOrEqual {
	return &GreaterThanOrEqual{newComparison(left, right)}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (gte *GreaterThanOrEqual) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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
	return fmt.Sprintf("(%s >= %s)", gte.Left(), gte.Right())
}

func (gte *GreaterThanOrEqual) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GreaterThanOrEqual")
	children := []string{sql.DebugString(gte.Left()), sql.DebugString(gte.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// LessThanOrEqual is a comparison that checks an expression is equal or lower than
// another.
type LessThanOrEqual struct {
	comparison
}

var _ sql.Expression = (*LessThanOrEqual)(nil)
var _ sql.CollationCoercible = (*LessThanOrEqual)(nil)

// NewLessThanOrEqual creates a LessThanOrEqual expression.
func NewLessThanOrEqual(left sql.Expression, right sql.Expression) *LessThanOrEqual {
	return &LessThanOrEqual{newComparison(left, right)}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (lte *LessThanOrEqual) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
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
	return fmt.Sprintf("(%s <= %s)", lte.Left(), lte.Right())
}

func (lte *LessThanOrEqual) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("LessThanOrEqual")
	children := []string{sql.DebugString(lte.Left()), sql.DebugString(lte.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

var (
	// ErrUnsupportedInOperand is returned when there is an invalid righthand
	// operand in an IN operator.
	ErrUnsupportedInOperand = errors.NewKind("right operand in IN operation must be tuple, but is %T")
)
