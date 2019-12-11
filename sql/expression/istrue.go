package expression

import (
	"errors"
	"github.com/src-d/go-mysql-server/sql"
)

// IsTrue is an expression that checks if an expression is true.
type IsTrue struct {
	UnaryExpression
	invert bool
}

const IsTrueStr = "IS TRUE"
const IsFalseStr = "IS FALSE"

// NewIsTrue creates a new IsTrue expression.
func NewIsTrue(child sql.Expression) *IsTrue {
	return &IsTrue{UnaryExpression: UnaryExpression{child}}
}

// NewIsFalse creates a new IsTrue expression with its boolean sense inverted (IsFalse, effectively).
func NewIsFalse(child sql.Expression) *IsTrue {
	return &IsTrue{UnaryExpression: UnaryExpression{child}, invert: true}
}

// Type implements the Expression interface.
func (*IsTrue) Type() sql.Type {
	return sql.Boolean
}

// IsNullable implements the Expression interface.
func (*IsTrue) IsNullable() bool {
	return false
}

// Eval implements the Expression interface.
func (e *IsTrue) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	var boolVal interface{}
	if v == nil {
		return false, nil
	} else {
		boolVal, err = sql.BooleanParse(v)
		if err != nil {
			return nil, err
		}
		boolVal = sql.BooleanConcrete(boolVal)
	}

	if e.invert {
		return !boolVal.(bool), nil
	}
	return boolVal, nil
}

func (e *IsTrue) String() string {
	isStr := IsTrueStr
	if e.invert {
		isStr = IsFalseStr
	}
	return e.Child.String() + " " + isStr
}

// WithChildren implements the Expression interface.
func (e *IsTrue) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, errors.New("incorrect number of children")
	}

	if e.invert {
		return NewIsFalse(children[0]), nil
	}
	return NewIsTrue(children[0]), nil
}
