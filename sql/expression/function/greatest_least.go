// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
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
	var selectedTime time.Time

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
			if sql.IsTextOnly(returnType) && (i == 0 || cmp(t, selectedString)) {
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
		case time.Time:
			// Since we deviate from MySQL with int -> time handling, we only set the selectedTime variable
			if i == 0 || cmp(t, selectedTime) {
				selectedTime = t
			}
		case nil:
			return nil, nil
		default:
			return nil, ErrUnsupportedType.New(t)
		}

	}

	switch returnType {
	case sql.Int64:
		return int64(selectedNum), nil
	case sql.LongText:
		return selectedString, nil
	case sql.Datetime:
		return selectedTime, nil
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
	allDatetime := true

	for _, arg := range args {
		argType := arg.Type()
		if sql.IsTuple(argType) {
			return nil, sql.ErrInvalidType.New("tuple")
		} else if sql.IsNumber(argType) {
			allString = false
			allDatetime = false
			if sql.IsFloat(argType) {
				allString = false
				allInt = false
			}
		} else if sql.IsText(argType) {
			allInt = false
			allDatetime = false
		} else if sql.IsTime(argType) {
			allString = false
			allInt = false
		} else if argType == sql.Null {
			// When a Null is present the return will always be Null
			return sql.Null, nil
		} else {
			return nil, ErrUnsupportedType.New(argType)
		}
	}

	if allString {
		return sql.LongText, nil
	} else if allInt {
		return sql.Int64, nil
	} else if allDatetime {
		return sql.Datetime, nil
	} else {
		return sql.Float64, nil
	}
}

// Greatest returns the argument with the greatest numerical or string value. It allows for
// numeric (ints and floats) and string arguments and will return the used type
// when all arguments are of the same type or floats if there are numerically
// convertible strings or integers mixed with floats. When ints or floats
// are mixed with non numerically convertible strings, those are ignored.
type Greatest struct {
	Args       []sql.Expression
	returnType sql.Type
}

var _ sql.FunctionExpression = (*Greatest)(nil)

// ErrUnsupportedType is returned when an argument to Greatest or Latest is not numeric or string
var ErrUnsupportedType = errors.NewKind("unsupported type for greatest/least argument: %T")

// NewGreatest creates a new Greatest UDF
func NewGreatest(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &Greatest{Args: args}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Greatest) FunctionName() string {
	return "greatest"
}

// Type implements the Expression interface.
func (f *Greatest) Type() sql.Type {
	if f.returnType != nil {
		return f.returnType
	}
	return f.Args[0].Type()
}

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
func (f *Greatest) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewGreatest(ctx, children...)
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
	case time.Time:
		return i.After(b.(time.Time))
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
	case time.Time:
		return i.Before(b.(time.Time))
	}
	panic("Implementation error on lessThan")
}

// Eval implements the Expression interface.
func (f *Greatest) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if f.returnType == nil {
		retType, err := compRetType(f.Args...)
		if err != nil {
			return nil, err
		}
		f.returnType = retType
	}

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

var _ sql.FunctionExpression = (*Least)(nil)

// NewLeast creates a new Least UDF
func NewLeast(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &Least{Args: args}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Least) FunctionName() string {
	return "least"
}

// Type implements the Expression interface.
func (f *Least) Type() sql.Type {
	if f.returnType != nil {
		return f.returnType
	}
	return f.Args[0].Type()
}

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
func (f *Least) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewLeast(ctx, children...)
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
	if f.returnType == nil {
		retType, err := compRetType(f.Args...)
		if err != nil {
			return nil, err
		}
		f.returnType = retType
	}

	return compEval(f.returnType, f.Args, ctx, row, lessThan)
}
