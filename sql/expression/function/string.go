package function

import (
	"encoding/hex"
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// AsciiFunc implements the sql function "ascii" which returns the numeric value of the leftmost character
func AsciiFunc(_ *sql.Context, val interface{}) (interface{}, error) {
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

// HexFunc implements the sql function "hex" which returns the hexidecimal representation of the string or numeric value
func HexFunc(_ *sql.Context, arg interface{}) (interface{}, error) {
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

func hexForString(val string) string {
	buf := make([]byte, 0, 2*len(val))
	for _, c := range val {
		high := byte(c / 16)
		low := byte(c % 16)

		buf = append(buf, hexChar(high))
		buf = append(buf, hexChar(low))
	}
	return string(buf)
}

func UnhexFunc(_ *sql.Context, arg interface{}) (interface{}, error) {
	val, err := sql.Text.Convert(arg)

	if err != nil {
		return nil, err
	}

	s := val.(string)
	if len(s) % 2 != 0 {
		return nil, nil
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

// BinFunc implements the sql function "bin" which returns the binary representation of a number
func BinFunc(_ *sql.Context, arg interface{}) (interface{}, error) {
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

// BitLengthFunc implements the sql function "bit_length" which returns the length of the argument in bits
func BitLengthFunc(_ *sql.Context, arg interface{}) (interface{}, error) {
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
		return 8*len([]byte(val)), nil
	case time.Time:
		return 128, nil
	}

	return nil, ErrInvalidArgument.New("bit_length", fmt.Sprint(arg))
}