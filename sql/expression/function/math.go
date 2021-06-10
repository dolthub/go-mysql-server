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
	"hash/crc32"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

// Rand returns a random float 0 <= x < 1. If it has an argument, that argument will be used to seed the random number
// generator, effectively turning it into a hash on that value.
type Rand struct {
	Child sql.Expression
}

var _ sql.Expression = (*Rand)(nil)
var _ sql.NonDeterministicExpression = (*Rand)(nil)
var _ sql.FunctionExpression = (*Rand)(nil)

// NewRand creates a new Rand expression.
func NewRand(ctx *sql.Context, exprs ...sql.Expression) (sql.Expression, error) {
	if len(exprs) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("rand", "0 or 1", len(exprs))
	}
	if len(exprs) > 0 {
		return &Rand{Child: exprs[0]}, nil
	}
	return &Rand{}, nil
}

// FunctionName implements sql.FunctionExpression
func (r *Rand) FunctionName() string {
	return "rand"
}

// Type implements sql.Expression.
func (r *Rand) Type() sql.Type {
	return sql.Float64
}

// IsNonDeterministic implements sql.NonDeterministicExpression
func (r *Rand) IsNonDeterministic() bool {
	return r.Child == nil
}

// IsNullable implements sql.Expression
func (r *Rand) IsNullable() bool {
	return false
}

// Resolved implements sql.Expression
func (r *Rand) Resolved() bool {
	return r.Child == nil || r.Child.Resolved()
}

func (r *Rand) String() string {
	if r.Child != nil {
		return fmt.Sprintf("RAND(%s)", r.Child)
	}
	return fmt.Sprintf("RAND()")
}

// WithChildren implements sql.Expression.
func (r *Rand) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	if len(children) == 0 {
		return r, nil
	}

	return NewRand(ctx, children[0])
}

// Children implements sql.Expression
func (r *Rand) Children() []sql.Expression {
	if r.Child == nil {
		return nil
	}
	return []sql.Expression{r.Child}
}

// Eval implements sql.Expression.
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

// Sin is the SIN function
type Sin struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Sin)(nil)

// NewSin returns a new SIN function expression
func NewSin(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Sin{NewUnaryFunc(arg, "SIN", sql.Float64)}
}

// Eval implements sql.Expression
func (s *Sin) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := s.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Sin(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (s *Sin) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSin(ctx, children[0]), nil
}

type Cos struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Cos)(nil)

// NewCos returns a new COS function expression
func NewCos(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Cos{NewUnaryFunc(arg, "COS", sql.Float64)}
}

// Eval implements sql.Expression
func (s *Cos) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := s.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Cos(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (c *Cos) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCos(ctx, children[0]), nil
}

type Tan struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Tan)(nil)

// NewTan returns a new TAN function expression
func NewTan(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Tan{NewUnaryFunc(arg, "TAN", sql.Float64)}
}

// Eval implements sql.Expression
func (t *Tan) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := t.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Tan(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (t *Tan) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewTan(ctx, children[0]), nil
}

type Asin struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Asin)(nil)

// NewAsin returns a new ASIN function expression
func NewAsin(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Asin{NewUnaryFunc(arg, "ASIN", sql.Float64)}
}

// Eval implements sql.Expression
func (a *Asin) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := a.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Asin(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (a *Asin) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAsin(ctx, children[0]), nil
}

type Acos struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Acos)(nil)

// NewAcos returns a new ACOS function expression
func NewAcos(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Acos{NewUnaryFunc(arg, "ACOS", sql.Float64)}
}

// Eval implements sql.Expression
func (a *Acos) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := a.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Acos(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (a *Acos) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAcos(ctx, children[0]), nil
}

type Atan struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Atan)(nil)

// NewAtan returns a new ATAN function expression
func NewAtan(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Atan{NewUnaryFunc(arg, "ATAN", sql.Float64)}
}

// Eval implements sql.Expression
func (a *Atan) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := a.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Atan(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (a *Atan) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAtan(ctx, children[0]), nil
}

type Cot struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Cot)(nil)

// NewCot returns a new COT function expression
func NewCot(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Cot{NewUnaryFunc(arg, "COT", sql.Float64)}
}

// Eval implements sql.Expression
func (c *Cot) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := c.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return 1.0 / math.Tan(n.(float64)), nil
}

// WithChildren implements sql.Expression
func (c *Cot) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCot(ctx, children[0]), nil
}

type Degrees struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Degrees)(nil)

// NewDegrees returns a new DEGREES function expression
func NewDegrees(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Degrees{NewUnaryFunc(arg, "DEGREES", sql.Float64)}
}

// Eval implements sql.Expression
func (d *Degrees) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return (n.(float64) * 180.0) / math.Pi, nil
}

// WithChildren implements sql.Expression
func (d *Degrees) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDegrees(ctx, children[0]), nil
}

type Radians struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Radians)(nil)

// NewRadians returns a new RADIANS function expression
func NewRadians(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Radians{NewUnaryFunc(arg, "RADIANS", sql.Float64)}
}

// Eval implements sql.Expression
func (r *Radians) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := r.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Float64.Convert(val)
	if err != nil {
		return nil, err
	}

	return (n.(float64) * math.Pi) / 180.0, nil
}

// WithChildren implements sql.Expression
func (r *Radians) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return NewRadians(ctx, children[0]), nil
}

type Crc32 struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Crc32)(nil)

// NewCrc32 returns a new CRC32 function expression
func NewCrc32(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Crc32{NewUnaryFunc(arg, "CRC32", sql.Uint32)}
}

// Eval implements sql.Expression
func (c *Crc32) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := c.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	var bytes []byte
	switch val := arg.(type) {
	case string:
		bytes = []byte(val)
	case int8, int16, int32, int64, int:
		val, err := sql.Int64.Convert(arg)

		if err != nil {
			return nil, err
		}

		bytes = []byte(strconv.FormatInt(val.(int64), 10))
	case uint8, uint16, uint32, uint64, uint:
		val, err := sql.Uint64.Convert(arg)

		if err != nil {
			return nil, err
		}

		bytes = []byte(strconv.FormatUint(val.(uint64), 10))
	case float32:
		s := floatToString(float64(val))
		bytes = []byte(s)
	case float64:
		s := floatToString(val)
		bytes = []byte(s)
	case bool:
		if val {
			bytes = []byte{1}
		} else {
			bytes = []byte{0}
		}
	default:
		return nil, ErrInvalidArgument.New("crc32", fmt.Sprint(arg))
	}

	return crc32.ChecksumIEEE(bytes), nil
}

// WithChildren implements sql.Expression
func (c *Crc32) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCrc32(ctx, children[0]), nil
}

func floatToString(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 32)
	idx := strings.IndexRune(s, '.')

	if idx == -1 {
		s += ".0"
	}

	return s
}

type Sign struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Sign)(nil)

// NewSign returns a new SIGN function expression
func NewSign(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Sign{NewUnaryFunc(arg, "SIGN", sql.Int8)}
}

var negativeSignRegex = regexp.MustCompile(`^-[0-9]*\.?[0-9]*[1-9]`)
var positiveSignRegex = regexp.MustCompile(`^+?[0-9]*\.?[0-9]*[1-9]`)

// Eval implements sql.Expression
func (s *Sign) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := s.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	switch typedVal := arg.(type) {
	case int8, int16, int32, int64, float64, float32, int, decimal.Decimal:
		val, err := sql.Int64.Convert(arg)

		if err != nil {
			return nil, err
		}

		n := val.(int64)
		if n == 0 {
			return int8(0), nil
		} else if n < 0 {
			return int8(-1), nil
		}

		return int8(1), nil

	case uint8, uint16, uint32, uint64, uint:
		val, err := sql.Uint64.Convert(arg)

		if err != nil {
			return nil, err
		}

		n := val.(uint64)
		if n == 0 {
			return int8(0), nil
		}

		return int8(1), nil

	case bool:
		if typedVal {
			return int8(1), nil
		}

		return int8(0), nil

	case time.Time:
		return int8(1), nil

	case string:
		if negativeSignRegex.MatchString(typedVal) {
			return int8(-1), nil
		} else if positiveSignRegex.MatchString(typedVal) {
			return int8(1), nil
		}

		return int8(0), nil
	}

	return int8(0), nil
}

// WithChildren implements sql.Expression
func (s *Sign) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSign(ctx, children[0]), nil
}
