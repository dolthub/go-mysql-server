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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"unsafe"
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

// SinFunc implements the sin function logic
func SinFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Sin(n.(float64)), nil
}

// CosFunc implements the cos function logic
func CosFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Cos(n.(float64)), nil
}

// TanFunc implements the tan function logic
func TanFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Tan(n.(float64)), nil
}

// ASinFunc implements the asin function logic
func ASinFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Asin(n.(float64)), nil
}

// ACosFunc implements the acos function logic
func ACosFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Acos(n.(float64)), nil
}

// ATanFunc implements the atan function logic
func ATanFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return math.Atan(n.(float64)), nil
}

// CotFunc implements the cot function logic
func CotFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return 1.0 / math.Tan(n.(float64)), nil
}

// DegreesFunc implements the degrees function logic
func DegreesFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
	n, err := sql.Float64.Convert(val)

	if err != nil {
		return nil, err
	}

	return (n.(float64) * 180.0) / math.Pi, nil
}

// RadiansFunc implements the radians function logic
func RadiansFunc(ctx *sql.Context, val interface{}) (interface{}, error) {
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
func Crc32Func(ctx *sql.Context, arg interface{}) (interface{}, error) {
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

func hexChar(b byte) byte {
	 if b > 9 {
		return b - 10 + byte('A')
	 }

	 return b + byte('0')
}

// MySQL expects the 64 bit 2s compliment representation for negative integer values. Typical methods for converting a
// number to a string don't handle negative integer values in this way (strconv.FormatInt and fmt.Sprintf for example).
func hexForNegativeInt64(n int64) string {
	// get a pointer to the int64s memory
	mem := (*[8]byte)(unsafe.Pointer(&n))
	// make a copy of the data that I can manipulate
	bytes := *mem
	// reverse the order for printing
	for i := 0; i < 4; i++ {
		bytes[i], bytes[7-i] = bytes[7-i], bytes[i]
	}
	// print the hex encoded bytes
	return fmt.Sprintf("%X", bytes)
}

func hexForFloat(f float64) (string, error) {
	if f < 0 {
		f -= 0.5
		n := int64(f)
		return hexForNegativeInt64(n), nil
	}

	f += 0.5
	n := uint64(f)
	return fmt.Sprintf("%X", n), nil
}

func HexFunc(ctx *sql.Context, arg interface{}) (interface{}, error) {
	switch val := arg.(type) {
	case string:
		buf := make([]byte, 0, 2*len(val))
		for _, c := range val {
			high := byte(c / 16)
			low := byte(c % 16)

			buf = append(buf, hexChar(high))
			buf = append(buf, hexChar(low))
		}

		return string(buf), nil

	case uint8, uint16, uint32, uint, int, int8, int16, int32, int64:
		n, err := sql.Int64.Convert(arg)

		if err != nil {
			return nil, err
		}

		a := n.(int64)
		if a < 0 {
			return hexForNegativeInt64(a), nil
		} else {
			return fmt.Sprintf("%X", a), nil
		}

	case uint64:
		return fmt.Sprintf("%X", val), nil

	case float32:
		return hexForFloat(float64(val))

	case float64:
		return hexForFloat(val)

	case bool:
		if val {
			return "1", nil
		}

		return "0", nil

	default:
		return nil, ErrInvalidArgument.New("crc32", fmt.Sprint(arg))
	}
}