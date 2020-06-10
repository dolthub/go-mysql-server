// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/shopspring/decimal"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
)

// Rand returns a random float 0 <= x < 1. If it has an argument, that argument will be used to seed the random number
// generator, effectively turning it into a hash on that value.
type Rand struct {
	Child sql.Expression
}

var _ sql.Expression = (*Rand)(nil)


// NewRand creates a new Rand expression.
func NewRand(exprs ...sql.Expression) (sql.Expression, error) {
	if len(exprs) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("rand", "0 or 1", len(exprs))
	}
	if len(exprs) > 0 {
		return &Rand{Child: exprs[0]}, nil
	}
	return &Rand{}, nil
}

// Type implements the Expression interface.
func (r *Rand) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the Expression interface
func (r *Rand) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface
func (r *Rand) Resolved() bool {
	return r.Child == nil || r.Child.Resolved()
}

func (r *Rand) String() string {
	if r.Child != nil {
		return fmt.Sprintf("RAND(%s)", r.Child)
	}
	return fmt.Sprintf("RAND()")
}

// WithChildren implements the Expression interface.
func (r *Rand) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	if len(children) == 0 {
		return r, nil
	}

	return NewRand(children[0])
}

// Children implements the Expression interface
func (r *Rand) Children() []sql.Expression {
	if r.Child == nil {
		return nil
	}
	return []sql.Expression{r.Child}
}

// Eval implements the Expression interface.
func (r *Rand) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if r.Child == nil {
		return rand.Float64(), nil
	}

	// For child expressions, the mysql semantics are to seed the PRNG with an int64 value of the expression given. For
	// non-numeric types, the seed will always be 0, which means that rand() will always return the same result for all
	// non-numeric seed arguments.
	e, err := r.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	var seed int64
	if sql.IsNumber(r.Child.Type()) {
		e, err = sql.Int64.Convert(e)
		if err == nil {
			seed = e.(int64)
		}
	}

	return rand.New(rand.NewSource(seed)).Float64(), nil
}

// UintRegex matches unsigned ints
var UintRegex, _ = regexp.Compile("^[1-9][0-9]*$")

// IntRegex matches ints
var IntRegex, _ = regexp.Compile("^-?[1-9][0-9]*$")

// UnaryMathFloatFuncLogic is an interface for sql function logic that takes a single float64 argument
type UnaryMathFloatFuncLogic interface {
	// EvalFloat contains sql function logic for a function that takes a single float64 as an argument
	EvalFloat(float64) (interface{}, error)
}

// UnaryMathFuncLogic is an interface for a sql function logic that takes a single number argument
type UnaryMathFuncLogic interface {
	// UnaryMathFloatFuncLogic is an embedded interface that handles the case where the number arumend passed is a float
	UnaryMathFloatFuncLogic
	// EvalUint handles the case where the number argument passed is a uint
	EvalUint(uint64) (interface{}, error)
	// EvalInt handles the case where the number argument passed is an int
	EvalInt(int64) (interface{}, error)
	// EvalDecimal handles the case where the number argument passed is a decimal
	EvalDecimal(decimal.Decimal) (interface{}, error)
}

// UnaryMathFloatFuncWrapper wraps logic that handles floats and provides methods for all number types
type UnaryMathFloatFuncWrapper struct {
	FlLogic UnaryMathFloatFuncLogic
}

// WrapUnaryMathFloatFuncLogic takes a UnaryMathFloatFuncLogic and wraps it returning a UnaryMathFuncLogic implementation
// which uses the float implementation for all calls
func WrapUnaryMathFloatFuncLogic(logic UnaryMathFloatFuncLogic) UnaryMathFuncLogic {
	return UnaryMathFloatFuncWrapper{logic}
}

// EvalFloat handles the case where the number argument passed is a float
func (wr UnaryMathFloatFuncWrapper) EvalFloat(n float64) (interface{}, error) {
	return wr.FlLogic.EvalFloat(n)
}

// EvalUint handles the case where the number argument passed is a uint
func (wr UnaryMathFloatFuncWrapper) EvalUint(n uint64) (interface{}, error) {
	return wr.FlLogic.EvalFloat(float64(n))
}

// EvalInt handles the case where the number argument passed is an int
func (wr UnaryMathFloatFuncWrapper) EvalInt(n int64) (interface{}, error) {
	return wr.FlLogic.EvalFloat(float64(n))
}

// EvalDecimal handles the case where the number argument passed is a decimal
func (wr UnaryMathFloatFuncWrapper) EvalDecimal(dec decimal.Decimal) (interface{}, error) {
	n, _ := dec.Float64()
	return wr.FlLogic.EvalFloat(n)
}

// UnaryMathFunc is a sql function that takes a single number argument and returns a value
type UnaryMathFunc struct {
	expression.UnaryExpression
	// Name is the name of the function
	Name  string
	// Logic contains the logic being executed when the function is called
	Logic UnaryMathFuncLogic
}

// NewUnaryMathFunc returns a function which is called to create a sql.Expression representing the function and its
// argemunts
func NewUnaryMathFunc(name string, logic UnaryMathFuncLogic) sql.Function1 {
	fn := func(e sql.Expression) sql.Expression {
		return &UnaryMathFunc{expression.UnaryExpression{Child: e}, name, logic}
	}

	return sql.Function1{Name: name, Fn: fn}
}

// Eval implements the Expression interface.
func (mf *UnaryMathFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := mf.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Fucking Golang
	switch x := val.(type) {
	case uint8:
		return mf.Logic.EvalUint(uint64(x))
	case uint16:
		return mf.Logic.EvalUint(uint64(x))
	case uint32:
		return mf.Logic.EvalUint(uint64(x))
	case uint64:
		return mf.Logic.EvalUint(x)
	case uint:
		return mf.Logic.EvalUint(uint64(x))
	case int8:
		return mf.Logic.EvalInt(int64(x))
	case int16:
		return mf.Logic.EvalInt(int64(x))
	case int32:
		return mf.Logic.EvalInt(int64(x))
	case int64:
		return mf.Logic.EvalInt(x)
	case int:
		return mf.Logic.EvalInt(int64(x))
	case float64:
		return mf.Logic.EvalFloat(x)
	case float32:
		return mf.Logic.EvalFloat(float64(x))
	case decimal.Decimal:
		return mf.Logic.EvalDecimal(x)
	case string:
		if x == "0" || UintRegex.MatchString(x) {
			n, err := strconv.ParseUint(x, 10, 64)

			if err != nil {
				return nil, err
			}

			return mf.Logic.EvalUint(n)
		} else if IntRegex.MatchString(x) {
			n, err := strconv.ParseInt(x, 10, 64)

			if err != nil {
				return nil, err
			}

			return mf.Logic.EvalInt(n)
		} else {
			n, err := strconv.ParseFloat(x, 64)

			if err != nil {
				return nil, err
			}

			return mf.Logic.EvalFloat(n)
		}
	}

	return nil, nil
}

// String implements the Stringer interface.
func (mf *UnaryMathFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(mf.Name), mf.Child.String())
}

// IsNullable implements the Expression interface.
func (mf *UnaryMathFunc) IsNullable() bool {
	return mf.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (mf *UnaryMathFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(mf, len(children), 1)
	}

	return &UnaryMathFunc{expression.UnaryExpression{Child:children[0]}, mf.Name, mf.Logic}, nil
}

// Type implements the Expression interface.
func (mf *UnaryMathFunc) Type() sql.Type {
	return mf.Child.Type()
}

// AbsFuncLogic is a UnaryMathFuncLogic implementation that returns the absolute value of a number
type AbsFuncLogic struct{}

// EvalUint handles the case where the number argument passed is a uint
func (fl AbsFuncLogic) EvalUint(n uint64) (interface{}, error) {
	return n, nil
}

// EvalInt handles the case where the number argument passed is an int
func (fl AbsFuncLogic) EvalInt(n int64) (interface{}, error) {
	if n < 0 {
		return -n, nil
	}

	return n, nil
}

// EvalFloat handles the case where the number argument passed is a float
func (fl AbsFuncLogic) EvalFloat(n float64) (interface{}, error) {
	if n < 0 {
		return -n, nil
	}

	return n, nil
}

// EvalDecimal handles the case where the number argument passed is a decimal
func (fl AbsFuncLogic) EvalDecimal(dec decimal.Decimal) (interface{}, error) {
	return dec.Abs(), nil
}

// SinFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the sin of a value
type SinFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl SinFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Sin(n), nil
}


// CosFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the cos of a value
type CosFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl CosFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Cos(n), nil
}


// TanFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the tan of a value
type TanFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl TanFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Tan(n), nil
}


// ASinFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the asin of a value
type ASinFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl ASinFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Asin(n), nil
}


// ACosFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the acos of a value
type ACosFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl ACosFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Acos(n), nil
}


// ATanFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the atan of a value
type ATanFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl ATanFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return math.Atan(n), nil
}


// CotFuncLogic is a UnaryMathFloatFuncWrapper implementation which returns the cot of a value
type CotFuncLogic struct{}

// EvalFloat handles the case where the number argument passed is a float
func (fl CotFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return 1.0 / math.Tan(n), nil
}


// DegreesFuncLogic is a UnaryMathFloatFuncWrapper implementation which converts radians to degrees
type DegreesFuncLogic struct {}

// EvalFloat handles the case where the number argument passed is a float
func (fl DegreesFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return (n * 180.0) / math.Pi, nil
}

// RadiansFuncLogic is a UnaryMathFloatFuncWrapper implementation which converts degrees to radians
type RadiansFuncLogic struct {}

// EvalFloat handles the case where the number argument passed is a float
func (fl RadiansFuncLogic) EvalFloat(n float64) (interface{}, error) {
	return (n * math.Pi) / 180.0, nil
}
