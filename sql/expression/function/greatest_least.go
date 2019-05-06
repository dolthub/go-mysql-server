package function

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Greatest returns the argument with the greatest numerical or string value. It allows for
// numeric (ints anf floats) and string arguments and will return the used type
// when all arguments are of the same type or floats if there are numerically
// convertible strings or integers mixed with floats. When ints or floats
// are mixed with non numerically convertible strings, those are ignored.
type Greatest struct {
	Args []sql.Expression
}

// ErrUnsupportedType is returned when an argument to Greatest or Latest is not numeric or string
var ErrUnsupportedType = errors.NewKind("unsupported type for greatest/latest argument: %T")

// NewGreatest creates a new Greatest UDF
func NewGreatest(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("GREATEST", "1 or more", 0)
	}

	for _, arg := range args {
		if sql.IsTuple(arg.Type()) {
			return nil, sql.ErrInvalidType.New("tuple")
		}
	}

	return &Greatest{args}, nil
}

// Type implements the Expression interface.
func (f *Greatest) Type() sql.Type { return sql.Float64 }

// IsNullable implements the Expression interface.
func (f *Greatest) IsNullable() bool {
	for _, arg := range f.Args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *Greatest) String() string {
	var args = make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("greatest(%s)", strings.Join(args, ", "))
}

// TransformUp implements the Expression interface.
func (f *Greatest) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var args = make([]sql.Expression, len(f.Args))
	for i, arg := range f.Args {
		a, err := arg.TransformUp(fn)
		if err != nil {
			return nil, err
		}
		args[i] = a
	}

	expr, err := NewGreatest(args...)
	if err != nil {
		return nil, err
	}

	return fn(expr)
}

// Resolved implements the Expression interface.
func (f *Greatest) Resolved() bool {
	for _, arg := range f.Args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *Greatest) Children() []sql.Expression { return f.Args }

// Eval implements the Expression interface.
func (f *Greatest) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	greatestNum := 0.0
	greatestString := ""
	allString := true
	allInt := true

	for i, arg := range f.Args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, nil
		}

		switch t := val.(type) {
		case int, int32, int64:
			switch x:= t.(type) {
			case int:
				t = int64(x)
			case int32:
				t = int64(x)
			}
			allString = false
			ival := t.(int64)
			if i == 0 || ival > int64(greatestNum) {
				greatestNum = float64(ival)
			}
		case float32, float64:
			if x, ok := t.(float32); ok {
				t = float64(x)
			}

			allString = false
			allInt = false
			fval := t.(float64)
			if i == 0 || fval > greatestNum {
				greatestNum = fval
			}

		case string:
			if allString && (i == 0 || t > greatestString) {
				greatestString = t
			}

			fval, err := strconv.ParseFloat(t, 64)
			if err != nil {
				// MySQL just ignores non numerically convertible string arguments
				// when mixed with numeric ones
				continue
			}

			allInt = false
			if i == 0 || fval > greatestNum {
				greatestNum = fval
			}
		default:
			return nil, ErrUnsupportedType.New(t)
		}

	}

	if allInt {
		return int64(greatestNum), nil
	} else if allString {
		return greatestString, nil
	}

	// float64
	return greatestNum, nil
}

// Least returns the argument with the least numerical or string value. It allows for
// numeric (ints anf floats) and string arguments and will return the used type
// when all arguments are of the same type or floats if there are numerically
// convertible strings or integers mixed with floats. When ints or floats
// are mixed with non numerically convertible strings, those are ignored.
type Least struct {
	Args []sql.Expression
}

// NewLeast creates a new Least UDF
func NewLeast(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("LEAST", "1 or more", 0)
	}

	for _, arg := range args {
		if len(args) > 1 && sql.IsArray(arg.Type()) {
			return nil, ErrConcatArrayWithOthers.New()
		}

		if sql.IsTuple(arg.Type()) {
			return nil, sql.ErrInvalidType.New("tuple")
		}
	}

	return &Least{args}, nil
}

// Type implements the Expression interface.
func (f *Least) Type() sql.Type { return sql.Float64 }

// IsNullable implements the Expression interface.
func (f *Least) IsNullable() bool {
	for _, arg := range f.Args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *Least) String() string {
	var args = make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("least(%s)", strings.Join(args, ", "))
}

// TransformUp implements the Expression interface.
func (f *Least) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var args = make([]sql.Expression, len(f.Args))
	for i, arg := range f.Args {
		a, err := arg.TransformUp(fn)
		if err != nil {
			return nil, err
		}
		args[i] = a
	}

	expr, err := NewLeast(args...)
	if err != nil {
		return nil, err
	}

	return fn(expr)
}

// Resolved implements the Expression interface.
func (f *Least) Resolved() bool {
	for _, arg := range f.Args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *Least) Children() []sql.Expression { return f.Args }

// Eval implements the Expression interface.
func (f *Least) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	leastNum := 0.0
	leastString := ""
	allString := true
	allInt := true

	for i, arg := range f.Args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, nil
		}

		switch t := val.(type) {
		case int, int32, int64:
			switch x:= t.(type) {
			case int:
				t = int64(x)
			case int32:
				t = int64(x)
			}
			allString = false
			ival := t.(int64)
			if i == 0 || ival < int64(leastNum) {
				leastNum = float64(ival)
			}
		case float32, float64:
			if x, ok := t.(float32); ok {
				t = float64(x)
			}

			allString = false
			allInt = false
			fval := t.(float64)
			if i == 0 || fval < leastNum {
				leastNum = fval
			}

		case string:
			if allString && (i == 0 || t < leastString) {
				leastString = t
			}

			fval, err := strconv.ParseFloat(t, 64)
			if err != nil {
				// MySQL just ignores non numerically convertible string arguments
				// when mixed with numeric ones
				continue
			}

			allInt = false
			if i == 0 || fval < leastNum {
				leastNum = fval
			}
		default:
			return nil, ErrUnsupportedType.New(t)
		}

	}

	if allInt {
		return int64(leastNum), nil
	} else if allString {
		return leastString, nil
	}

	// float
	return leastNum, nil
}
