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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

// Ascii implements the sql function "ascii" which returns the numeric value of the leftmost character
type Ascii struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Ascii)(nil)

func NewAscii(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Ascii{NewUnaryFunc(arg, "ASCII", sql.Uint8)}
}

// Eval implements the sql.Expression interface
func (a *Ascii) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := a.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	switch x := val.(type) {
	case bool:
		if x {
			val = 1
		} else {
			val = 0
		}

	case time.Time:
		val = x.Year()
	}

	x, err := sql.Text.Convert(val)

	if err != nil {
		return nil, err
	}

	s := x.(string)
	return s[0], nil
}

// WithChildren implements the sql.Expression interface
func (a *Ascii) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAscii(ctx, children[0]), nil
}

// Hex implements the sql function "hex" which returns the hexadecimal representation of the string or numeric value
type Hex struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Hex)(nil)

func NewHex(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Hex{NewUnaryFunc(arg, "HEX", sql.Text)}
}

// Eval implements the sql.Expression interface
func (h *Hex) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := h.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	switch val := arg.(type) {
	case string:
		return hexForString(val), nil

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

	case decimal.Decimal:
		f, _ := val.Float64()
		return hexForFloat(f)

	case bool:
		if val {
			return "1", nil
		}

		return "0", nil

	case time.Time:
		s, err := formatDate("%Y-%m-%d %H:%i:%s", val)

		if err != nil {
			return nil, err
		}

		s += fractionOfSecString(val)

		return hexForString(s), nil

	default:
		return nil, ErrInvalidArgument.New("crc32", fmt.Sprint(arg))
	}
}

// WithChildren implements the sql.Expression interface
func (h *Hex) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewHex(ctx, children[0]), nil
}

func hexChar(b byte) byte {
	if b > 9 {
		return b - 10 + byte('A')
	}

	return b + byte('0')
}

// MySQL expects the 64 bit 2s complement representation for negative integer values. Typical methods for converting a
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

func hexForString(val string) string {
	buf := make([]byte, 0, 2*len(val))
	// Do not change this to range, as range iterates over runes and not bytes
	for i := 0; i < len(val); i++ {
		c := val[i]
		high := c / 16
		low := c % 16

		buf = append(buf, hexChar(high))
		buf = append(buf, hexChar(low))
	}
	return string(buf)
}

// Unhex implements the sql function "unhex" which returns the integer representation of a hexadecimal string
type Unhex struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Unhex)(nil)

func NewUnhex(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Unhex{NewUnaryFunc(arg, "UNHEX", sql.LongBlob)}
}

// Eval implements the sql.Expression interface
func (h *Unhex) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := h.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	val, err := sql.LongBlob.Convert(arg)

	if err != nil {
		return nil, err
	}

	s := val.(string)
	if len(s)%2 != 0 {
		s = "0" + s
	}

	s = strings.ToUpper(s)
	for _, c := range s {
		if c < '0' || c > '9' && c < 'A' || c > 'F' {
			return nil, nil
		}
	}

	res, err := hex.DecodeString(s)

	if err != nil {
		return nil, err
	}

	return string(res), nil
}

// WithChildren implements the sql.Expression interface
func (h *Unhex) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewUnhex(ctx, children[0]), nil
}

// MySQL expects the 64 bit 2s complement representation for negative integer values. Typical methods for converting a
// number to a string don't handle negative integer values in this way (strconv.FormatInt and fmt.Sprintf for example).
func binForNegativeInt64(n int64) string {
	// get a pointer to the int64s memory
	mem := (*[8]byte)(unsafe.Pointer(&n))
	// make a copy of the data that I can manipulate
	bytes := *mem

	s := ""
	for i := 7; i >= 0; i-- {
		s += strconv.FormatInt(int64(bytes[i]), 2)
	}

	return s
}

// Bin implements the sql function "bin" which returns the binary representation of a number
type Bin struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Bin)(nil)

func NewBin(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Bin{NewUnaryFunc(arg, "BIN", sql.Text)}
}

// Eval implements the sql.Expression interface
func (h *Bin) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := h.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	switch val := arg.(type) {
	case time.Time:
		return strconv.FormatUint(uint64(val.Year()), 2), nil
	case uint64:
		return strconv.FormatUint(val, 2), nil

	default:
		n, err := sql.Int64.Convert(arg)

		if err != nil {
			return "0", nil
		}

		if n.(int64) < 0 {
			return binForNegativeInt64(n.(int64)), nil
		} else {
			return strconv.FormatInt(n.(int64), 2), nil
		}
	}
}

// WithChildren implements the sql.Expression interface
func (h *Bin) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewBin(ctx, children[0]), nil
}

// Bitlength implements the sql function "bit_length" which returns the data length of the argument in bits
type Bitlength struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*Bitlength)(nil)

func NewBitlength(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Bitlength{NewUnaryFunc(arg, "BIT_LENGTH", sql.Int32)}
}

// Eval implements the sql.Expression interface
func (h *Bitlength) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := h.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	switch val := arg.(type) {
	case uint8, int8, bool:
		return 8, nil
	case uint16, int16:
		return 16, nil
	case int, uint, uint32, int32, float32:
		return 32, nil
	case uint64, int64, float64:
		return 64, nil
	case string:
		return 8 * len([]byte(val)), nil
	case time.Time:
		return 128, nil
	}

	return nil, ErrInvalidArgument.New("bit_length", fmt.Sprint(arg))
}

// WithChildren implements the sql.Expression interface
func (h *Bitlength) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewBitlength(ctx, children[0]), nil
}
