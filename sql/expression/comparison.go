package expression

import (
	"fmt"
	"regexp"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Compararer implements a comparison expression.
type Comparer interface {
	sql.Expression
	IsComparison() bool
	Left() sql.Expression
	Right() sql.Expression
	SetLeft(sql.Expression)
	SetRight(sql.Expression)
}

type comparison struct {
	BinaryExpression
}

func newComparison(left, right sql.Expression) comparison {
	return comparison{BinaryExpression{left, right}}
}

// Compare the two given values using the types of the expressions in the comparison.
// Since both types should be equal, it does not matter which type is used, but for
// reference, the left type is always used.
func (c *comparison) Compare(a, b interface{}) int {
	return c.BinaryExpression.Left.Type().Compare(a, b)
}

// Type implements the Expression interface.
func (*comparison) Type() sql.Type {
	return sql.Boolean
}

// IsComparison implements Comaparer interface
func (*comparison) IsComparison() bool { return true }

// Left implements Comaparer interface
func (c *comparison) Left() sql.Expression { return c.BinaryExpression.Left }

// Right implements Comaparer interface
func (c *comparison) Right() sql.Expression { return c.BinaryExpression.Right }

// Left implements Comaparer interface
func (c *comparison) SetLeft(left sql.Expression) { c.BinaryExpression.Left = left }

// Right implements Comaparer interface
func (c *comparison) SetRight(right sql.Expression) { c.BinaryExpression.Right = right }

// Equals is a comparison that checks an expression is equal to another.
type Equals struct {
	comparison
}

// NewEquals returns a new Equals expression.
func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	return &Equals{newComparison(left, right)}
}

// Eval implements the Expression interface.
func (e Equals) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	a, err := e.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	b, err := e.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return e.Compare(a, b) == 0, nil
}

// TransformUp implements the Expression interface.
func (e *Equals) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := e.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := e.BinaryExpression.Right.TransformUp(f)
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
func (re Regexp) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	l, err := re.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}
	r, err := re.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if l == nil || r == nil {
		return nil, nil
	}

	sl, okl := l.(string)
	sr, okr := r.(string)

	if !okl || !okr {
		return re.Compare(l, r) == 0, nil
	}

	reg, err := regexp.Compile(sr)
	if err != nil {
		return false, err
	}

	return reg.MatchString(sl), nil
}

// TransformUp implements the Expression interface.
func (re *Regexp) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := re.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := re.BinaryExpression.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewRegexp(left, right))
}

func (re Regexp) String() string {
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
func (gt GreaterThan) Eval(
	session sql.Session,
	row sql.Row,
) (interface{}, error) {
	a, err := gt.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	b, err := gt.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return gt.Compare(a, b) == 1, nil
}

// TransformUp implements the Expression interface.
func (gt *GreaterThan) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := gt.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := gt.BinaryExpression.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewGreaterThan(left, right))
}

func (gt GreaterThan) String() string {
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
func (lt LessThan) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	a, err := lt.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	b, err := lt.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return lt.Compare(a, b) == -1, nil
}

// TransformUp implements the Expression interface.
func (lt *LessThan) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := lt.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := lt.BinaryExpression.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLessThan(left, right))
}

func (lt LessThan) String() string {
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
func (gte GreaterThanOrEqual) Eval(
	session sql.Session,
	row sql.Row,
) (interface{}, error) {
	a, err := gte.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	b, err := gte.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return gte.Compare(a, b) > -1, nil
}

// TransformUp implements the Expression interface.
func (gte *GreaterThanOrEqual) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := gte.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := gte.BinaryExpression.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewGreaterThanOrEqual(left, right))
}

func (gte GreaterThanOrEqual) String() string {
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
func (lte LessThanOrEqual) Eval(
	session sql.Session,
	row sql.Row,
) (interface{}, error) {
	a, err := lte.BinaryExpression.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	b, err := lte.BinaryExpression.Right.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return lte.Compare(a, b) < 1, nil
}

// TransformUp implements the Expression interface.
func (lte *LessThanOrEqual) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := lte.BinaryExpression.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := lte.BinaryExpression.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLessThanOrEqual(left, right))
}

func (lte LessThanOrEqual) String() string {
	return fmt.Sprintf("%s <= %s", lte.Left(), lte.Right())
}
