package expression

import "github.com/src-d/go-mysql-server/sql"

// IsNull is an expression that checks if an expression is null.
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
		boolVal = false
	} else {
		boolVal, err = sql.Boolean.Convert(v)
		if err != nil {
			return nil, err
		}
	}

	if e.invert {
		return !boolVal.(bool), nil
	}
	return boolVal, nil
}

func (e IsTrue) String() string {
	isStr := IsTrueStr
	if e.invert {
		isStr = IsFalseStr
	}
	return e.Child.String() + " " + isStr
}

// TransformUp implements the Expression interface.
func (e *IsTrue) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	if e.invert {
		return f(NewIsFalse(child))
	} else {
		return f(NewIsTrue(child))
	}
}

