package function

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	errors "gopkg.in/src-d/go-errors.v1"
)

// Concat joins several strings together.
type Concat struct {
	args []sql.Expression
}

// ErrConcatArrayWithOthers is returned when there are more than 1 argument in
// concat and any of them is an array.
var ErrConcatArrayWithOthers = errors.NewKind("can't concat a string array with any other elements")

// NewConcat creates a new Concat UDF.
func NewConcat(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("CONCAT", "1 or more", 0)
	}

	for _, arg := range args {
		// Don't perform this check until it's resolved. Otherwise we
		// can't get the type for sure.
		if !arg.Resolved() {
			continue
		}

		if len(args) > 1 && sql.IsArray(arg.Type()) {
			return nil, ErrConcatArrayWithOthers.New()
		}

		if sql.IsTuple(arg.Type()) {
			return nil, sql.ErrInvalidType.New("tuple")
		}
	}

	return &Concat{args}, nil
}

// Type implements the Expression interface.
func (f *Concat) Type() sql.Type { return sql.Text }

// IsNullable implements the Expression interface.
func (f *Concat) IsNullable() bool {
	for _, arg := range f.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *Concat) String() string {
	var args = make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("concat(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (*Concat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewConcat(children...)
}

// Resolved implements the Expression interface.
func (f *Concat) Resolved() bool {
	for _, arg := range f.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *Concat) Children() []sql.Expression { return f.args }

// Eval implements the Expression interface.
func (f *Concat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var parts []string

	for _, arg := range f.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, nil
		}

		if sql.IsArray(arg.Type()) {
			val, err = sql.CreateArray(sql.Text).Convert(val)
			if err != nil {
				return nil, err
			}

			for _, v := range val.([]interface{}) {
				parts = append(parts, v.(string))
			}
		} else {
			val, err = sql.Text.Convert(val)
			if err != nil {
				return nil, err
			}

			parts = append(parts, val.(string))
		}
	}

	return strings.Join(parts, ""), nil
}
