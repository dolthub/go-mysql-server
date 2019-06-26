package function

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)

var ErrUintOverflow = errors.NewKind(
	"Unsigned integer too big to fit on signed integer")

// compEval is used to implement Greatest/Least Eval() using a comparison function
func compEval(
	returnType sql.Type,
	args []sql.Expression,
	ctx *sql.Context,
	row sql.Row,
	cmp compareFn,
) (interface{}, error) {

	if returnType == sql.Null {
		return nil, nil
	}

	var selectedNum float64
	var selectedString string

	for i, arg := range args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		switch t := val.(type) {
		case int, int8, int16, int32, int64, uint,
			uint8, uint16, uint32, uint64:
			switch x := t.(type) {
			case int:
				t = int64(x)
			case int8:
				t = int64(x)
			case int16:
				t = int64(x)
			case int32:
				t = int64(x)
			case uint:
				i := int64(x)
				if i < 0 {
					return nil, ErrUintOverflow.New()
				}
				t = i
			case uint64:
				i := int64(x)
				if i < 0 {
					return nil, ErrUintOverflow.New()
				}
				t = i
			case uint8:
				t = int64(x)
			case uint16:
				t = int64(x)
			case uint32:
				t = int64(x)
			}
			ival := t.(int64)
			if i == 0 || cmp(ival, int64(selectedNum)) {
				selectedNum = float64(ival)
			}
		case float32, float64:
			if x, ok := t.(float32); ok {
				t = float64(x)
			}

			fval := t.(float64)
			if i == 0 || cmp(fval, float64(selectedNum)) {
				selectedNum = fval
			}

		case string:
			if returnType == sql.Text && (i == 0 || cmp(t, selectedString)) {
				selectedString = t
			}

			fval, err := strconv.ParseFloat(t, 64)
			if err != nil {
				// MySQL just ignores non numerically convertible string arguments
				// when mixed with numeric ones
				continue
			}

			if i == 0 || cmp(fval, selectedNum) {
				selectedNum = fval
			}
		default:
			return nil, ErrUnsupportedType.New(t)
		}

	}

	switch returnType {
	case sql.Int64:
		return int64(selectedNum), nil
	case sql.Text:
		return selectedString, nil
	}

	// sql.Float64
	return float64(selectedNum), nil
}

// compRetType is used to determine the type from args based on the rules described for
// Greatest/Least
func compRetType(args ...sql.Expression) (sql.Type, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("LEAST", "1 or more", 0)
	}

	allString := true
	allInt := true

	for _, arg := range args {
		argType := arg.Type()
		if sql.IsTuple(argType) {
			return nil, sql.ErrInvalidType.New("tuple")
		} else if sql.IsNumber(argType) {
			allString = false
			if sql.IsDecimal(argType) {
				allString = false
				allInt = false
			}
		} else if sql.IsText(argType) {
			allInt = false
		} else if argType == sql.Null {
			// When a Null is present the return will always de Null
			return sql.Null, nil
		} else {
			return nil, ErrUnsupportedType.New(argType)
		}
	}

	if allString {
		return sql.Text, nil
	} else if allInt {
		return sql.Int64, nil
	}

	return sql.Float64, nil
}

// Greatest returns the argument with the greatest numerical or string value. It allows for
// numeric (ints anf floats) and string arguments and will return the used type
// when all arguments are of the same type or floats if there are numerically
// convertible strings or integers mixed with floats. When ints or floats
// are mixed with non numerically convertible strings, those are ignored.
type Greatest struct {
	Args       []sql.Expression
	returnType sql.Type
}

// ErrUnsupportedType is returned when an argument to Greatest or Latest is not numeric or string
var ErrUnsupportedType = errors.NewKind("unsupported type for greatest/latest argument: %T")

// NewGreatest creates a new Greatest UDF
func NewGreatest(args ...sql.Expression) (sql.Expression, error) {
	retType, err := compRetType(args...)
	if err != nil {
		return nil, err
	}

	return &Greatest{args, retType}, nil
}

// Type implements the Expression interface.
func (f *Greatest) Type() sql.Type { return f.returnType }

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

// WithChildren implements the Expression interface.
func (f *Greatest) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewGreatest(children...)
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

type compareFn func(interface{}, interface{}) bool

func greaterThan(a, b interface{}) bool {
	switch i := a.(type) {
	case int64:
		return i > b.(int64)
	case float64:
		return i > b.(float64)
	case string:
		return i > b.(string)
	}
	panic("Implementation error on greaterThan")
}

func lessThan(a, b interface{}) bool {
	switch i := a.(type) {
	case int64:
		return i < b.(int64)
	case float64:
		return i < b.(float64)
	case string:
		return i < b.(string)
	}
	panic("Implementation error on lessThan")
}

// Eval implements the Expression interface.
func (f *Greatest) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return compEval(f.returnType, f.Args, ctx, row, greaterThan)
}

// Least returns the argument with the least numerical or string value. It allows for
// numeric (ints anf floats) and string arguments and will return the used type
// when all arguments are of the same type or floats if there are numerically
// convertible strings or integers mixed with floats. When ints or floats
// are mixed with non numerically convertible strings, those are ignored.
type Least struct {
	Args       []sql.Expression
	returnType sql.Type
}

// NewLeast creates a new Least UDF
func NewLeast(args ...sql.Expression) (sql.Expression, error) {
	retType, err := compRetType(args...)
	if err != nil {
		return nil, err
	}

	return &Least{args, retType}, nil
}

// Type implements the Expression interface.
func (f *Least) Type() sql.Type { return f.returnType }

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

// WithChildren implements the Expression interface.
func (f *Least) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLeast(children...)
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
	return compEval(f.returnType, f.Args, ctx, row, lessThan)
}
