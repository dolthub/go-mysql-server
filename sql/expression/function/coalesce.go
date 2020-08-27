package function

import (
	"fmt"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Coalesce returns the first non-NULL value in the list, or NULL if there are no non-NULL values.
type Coalesce struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Coalesce)(nil)

// NewCoalesce creates a new Coalesce sql.Expression.
func NewCoalesce(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("COALESCE", "1 or more", 0)
	}

	return &Coalesce{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (c *Coalesce) FunctionName() string {
	return "coalesce"
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

// WithChildren implements the Expression interface.
func (*Coalesce) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewCoalesce(children...)
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
