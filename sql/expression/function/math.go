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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/liquidata-inc/go-mysql-server/sql"
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
func NewRand(exprs ...sql.Expression) (sql.Expression, error) {
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
func (r *Rand) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	if len(children) == 0 {
		return r, nil
	}

	return NewRand(children[0])
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

// SinFunc implements the sin function logic
func SinFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Sin(n.(float64)), nil
}

// CosFunc implements the cos function logic
func CosFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Cos(n.(float64)), nil
}

// TanFunc implements the tan function logic
func TanFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Tan(n.(float64)), nil
}

// ASinFunc implements the asin function logic
func ASinFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Asin(n.(float64)), nil
}

// ACosFunc implements the acos function logic
func ACosFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Acos(n.(float64)), nil
}

// ATanFunc implements the atan function logic
func ATanFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Atan(n.(float64)), nil
}

// CotFunc implements the cot function logic
func CotFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return 1.0 / math.Tan(n.(float64)), nil
}

// DegreesFunc implements the degrees function logic
func DegreesFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return (n.(float64) * 180.0) / math.Pi, nil
}

// RadiansFunc implements the radians function logic
func RadiansFunc(_ *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return (n.(float64) * math.Pi) / 180.0, nil
}

func asBytes(arg interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := binary.Write(buf, binary.LittleEndian, arg)

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func floatToString(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 32)
	idx := strings.IndexRune(s, '.')

	if idx == -1 {
		s += ".0"
	}

	return s
}

// Crc32Func implement the sql crc32 function logic
func Crc32Func(_ *sql.Context, arg interface{}) (interface{}, error) {
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

var negativeSignRegex = regexp.MustCompile(`^-[0-9]*\.?[0-9]*[1-9]`)
var positiveSignRegex = regexp.MustCompile(`^+?[0-9]*\.?[0-9]*[1-9]`)

func SignFunc(_ *sql.Context, arg interface{}) (interface{}, error) {
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
