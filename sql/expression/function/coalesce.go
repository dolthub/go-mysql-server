package function

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Coalesce returns the first non-NULL value in the list, or NULL if there are no non-NULL values.
type Coalesce struct {
	args []sql.Expression
}

// NewCoalesce creates a new Coalesce sql.Expression.
func NewCoalesce(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("1 or more", 0)
	}

	return &Coalesce{args}, nil
}

// Type implements the sql.Expression interface.
// The return type of Type() is the aggregated type of the argument types.
func (c *Coalesce) Type() sql.Type {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		t := arg.Type()
		if t == nil {
			continue
		}
		return t
	}

	return nil
}

// IsNullable implements the sql.Expression interface.
// Returns true if all arguments are nil
// or of the first non-nil argument is nullable, otherwise false.
func (c *Coalesce) IsNullable() bool {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		return arg.IsNullable()
	}
	return true
}

func (c *Coalesce) String() string {
	var args = make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("coalesce(%s)", strings.Join(args, ", "))
}

// TransformUp implements the sql.Expression interface.
func (c *Coalesce) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var (
		args = make([]sql.Expression, len(c.args))
		err  error
	)

	for i, arg := range c.args {
		if arg != nil {
			arg, err = arg.TransformUp(fn)
			if err != nil {
				return nil, err
			}
		}
		args[i] = arg
	}

	expr, err := NewCoalesce(args...)
	if err != nil {
		return nil, err
	}

	return fn(expr)
}

// Resolved implements the sql.Expression interface.
// The function checks if first non-nil argument is resolved.
func (c *Coalesce) Resolved() bool {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Expression interface.
func (c *Coalesce) Children() []sql.Expression { return c.args }

// Eval implements the sql.Expression interface.
// The function evaluates the first non-nil argument. If the value is nil,
// then we keep going, otherwise we return the first non-nil value.
func (c *Coalesce) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}

		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			continue
		}

		return val, nil
	}

	return nil, nil
}
