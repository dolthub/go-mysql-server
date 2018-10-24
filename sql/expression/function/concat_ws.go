package function

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ConcatWithSeparator joins several strings together. The first argument is
// the separator for the rest of the arguments. The separator is added between
// the strings to be concatenated. The separator can be a string, as can the
// rest of the arguments. If the separator is NULL, the result is NULL.
type ConcatWithSeparator struct {
	args []sql.Expression
}

// NewConcatWithSeparator creates a new NewConcatWithSeparator UDF.
func NewConcatWithSeparator(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("1 or more", 0)
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

	return &ConcatWithSeparator{args}, nil
}

// Type implements the Expression interface.
func (f *ConcatWithSeparator) Type() sql.Type { return sql.Text }

// IsNullable implements the Expression interface.
func (f *ConcatWithSeparator) IsNullable() bool {
	for _, arg := range f.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *ConcatWithSeparator) String() string {
	var args = make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("concat_ws(%s)", strings.Join(args, ", "))
}

// TransformUp implements the Expression interface.
func (f *ConcatWithSeparator) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var args = make([]sql.Expression, len(f.args))
	for i, arg := range f.args {
		arg, err := arg.TransformUp(fn)
		if err != nil {
			return nil, err
		}
		args[i] = arg
	}

	expr, err := NewConcatWithSeparator(args...)
	if err != nil {
		return nil, err
	}

	return fn(expr)
}

// Resolved implements the Expression interface.
func (f *ConcatWithSeparator) Resolved() bool {
	for _, arg := range f.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *ConcatWithSeparator) Children() []sql.Expression { return f.args }

// Eval implements the Expression interface.
func (f *ConcatWithSeparator) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var parts []string

	for i, arg := range f.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil && i == 0 {
			return nil, nil
		}

		if val == nil {
			continue
		}

		if sql.IsArray(arg.Type()) {
			val, err = sql.Array(sql.Text).Convert(val)
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

	return strings.Join(parts[1:], parts[0]), nil
}
