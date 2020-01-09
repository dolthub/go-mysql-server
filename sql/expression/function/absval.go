package function

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// AbsVal is a function that takes the absolute value of a number
type AbsVal struct {
	expression.UnaryExpression
}

// NewAbsVal creates a new AbsVal expression.
func NewAbsVal(e sql.Expression) sql.Expression {
	return &AbsVal{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (t *AbsVal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Fucking Golang
	switch x := val.(type) {
	case int:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case int64:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case int32:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case int16:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case int8:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case uint:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case uint64:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case uint32:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case uint16:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case uint8:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case float64:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	case float32:
		if x < 0 {
			return -x, nil
		} else {
			return x, nil
		}
	}

	return nil, nil
}

// String implements the Stringer interface.
func (t *AbsVal) String() string {
	return fmt.Sprintf("ABS(%s)", t.Child)
}

// IsNullable implements the Expression interface.
func (t *AbsVal) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *AbsVal) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewAbsVal(children[0]), nil
}

// Type implements the Expression interface.
func (t *AbsVal) Type() sql.Type {
	return nil
}
