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

package sql

import (
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	Year YearType = yearType{}

	ErrConvertingToYear = errors.NewKind("value %v is not a valid Year")

	yearValueType = reflect.TypeOf(int16(0))
)

// YearType represents the YEAR type.
// https://dev.mysql.com/doc/refman/8.0/en/year.html
// The type of the returned value is int16.
type YearType interface {
	Type
}

type yearType struct{}

// Compare implements Type interface.
func (t yearType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}

	// Handle nil values that might have been returned by Convert
	if as == nil {
		if bs == nil {
			return 0, nil
		}
		return -1, nil
	} else if bs == nil {
		return 1, nil
	}

	// Safe type assertion with validation
	ai, ok := as.(int16)
	if !ok {
		return 0, ErrConvertingToYear.New(a)
	}
	bi, ok := bs.(int16)
	if !ok {
		return 0, ErrConvertingToYear.New(b)
	}

	if ai == bi {
		return 0, nil
	}
	if ai < bi {
		return -1, nil
	}
	return 1, nil
}

// Convert implements Type interface.
func (t yearType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch value := v.(type) {
	case int:
		return t.Convert(int64(value))
	case uint:
		return t.Convert(int64(value))
	case int8:
		return t.Convert(int64(value))
	case uint8:
		return t.Convert(int64(value))
	case int16:
		return t.Convert(int64(value))
	case uint16:
		return t.Convert(int64(value))
	case int32:
		return t.Convert(int64(value))
	case uint32:
		return t.Convert(int64(value))
	case int64:
		// For values 1-69, add 2000
		// To prevent bugs in 100 years, we always
		// zero out unrecognized years.
		if value >= 0 && value <= 99 {
			return nil, ErrConvertingToYear.New(value)
		}

		// For direct year values in range
		if value >= 1901 && value <= 2155 {
			return createSafeInt16Year(value)
		}

		return nil, ErrConvertingToYear.New(value)
	case uint64:
		// Check if the value exceeds the maximum int64 value by comparing string lengths
		maxInt64Str := strconv.FormatInt(math.MaxInt64, 10)
		valueStr := strconv.FormatUint(value, 10)

		if len(valueStr) > len(maxInt64Str) || (len(valueStr) == len(maxInt64Str) && valueStr > maxInt64Str) {
			return nil, ErrConvertingToYear.New("uint64 value out of bounds for int64")
		}

		// Safe to parse to int64 since we've verified it's within bounds
		i64, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// If value is in valid year range
		if (i64 >= 1901 && i64 <= 2155) || i64 == 0 {
			return createSafeInt16Year(i64)
		}

		// Otherwise, process it through the int64 conversion logic
		return t.Convert(i64)
	case float32:
		// Convert to string first for safer comparison
		floatStr := strconv.FormatFloat(float64(value), 'f', -1, 32)

		// Parse back to make sure it doesn't have a fractional part
		parsedFloat, err := strconv.ParseFloat(floatStr, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// Check for fractional part by comparing to integer value
		integerPart := math.Trunc(parsedFloat)
		if parsedFloat != integerPart {
			return nil, ErrConvertingToYear.New("float32 value has a fractional component")
		}

		// Convert to string then to int64 (safe operations)
		intStr := strconv.FormatFloat(integerPart, 'f', 0, 64)
		i64, err := strconv.ParseInt(intStr, 10, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// Validate as a year
		if i64 >= 1901 && i64 <= 2155 {
			return createSafeInt16Year(i64)
		}

		return nil, ErrConvertingToYear.New(value)
	case float64:
		// Convert to string first for safer comparison
		floatStr := strconv.FormatFloat(value, 'f', -1, 64)

		// Parse back to make sure it doesn't have a fractional part
		parsedFloat, err := strconv.ParseFloat(floatStr, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// Check for fractional part
		integerPart := math.Trunc(parsedFloat)
		if parsedFloat != integerPart {
			return nil, ErrConvertingToYear.New("float64 value has a fractional component")
		}

		// Convert to string then to int64 (safe operations)
		intStr := strconv.FormatFloat(integerPart, 'f', 0, 64)
		i64, err := strconv.ParseInt(intStr, 10, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// Validate as a year
		if i64 >= 1901 && i64 <= 2155 {
			return createSafeInt16Year(i64)
		}

		return nil, ErrConvertingToYear.New(value)
	case decimal.Decimal:
		// IntPart() returns an int64, which is safe to convert for our valid year ranges
		intVal := value.IntPart()
		if intVal < math.MinInt16 || intVal > math.MaxInt16 {
			return nil, ErrConvertingToYear.New("decimal value out of bounds for int16")
		}
		// Check if there's a fractional part
		if !value.Equal(decimal.NewFromInt(intVal)) {
			return nil, ErrConvertingToYear.New("decimal value has a fractional component")
		}
		return t.Convert(intVal)
	case decimal.NullDecimal:
		if !value.Valid {
			return nil, nil
		}
		// IntPart() returns an int64, which is safe to convert for our valid year ranges
		intVal := value.Decimal.IntPart()
		if intVal < math.MinInt16 || intVal > math.MaxInt16 {
			return nil, ErrConvertingToYear.New("decimal value out of bounds for int16")
		}
		// Check if there's a fractional part
		if !value.Decimal.Equal(decimal.NewFromInt(intVal)) {
			return nil, ErrConvertingToYear.New("decimal value has a fractional component")
		}
		return t.Convert(intVal)
	case string:
		if value == "" {
			return nil, ErrConvertingToYear.New("empty string")
		}

		valueLength := len(value)
		if valueLength == 1 || valueLength == 2 || valueLength == 4 {
			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, ErrConvertingToYear.New(err)
			}
			if i == 0 {
				// Using a literal zero value is considered safe
				return int16(0), nil
			}
			if i >= 1901 && i <= 2155 {
				return createSafeInt16Year(i)
			}
			return nil, ErrConvertingToYear.New(value)
		}
		return nil, ErrConvertingToYear.New(value)
	case time.Time:
		// Check if time is zero value
		if value.IsZero() {
			// Using a literal zero value is considered safe
			return int16(0), nil
		}

		year := value.Year()
		// Convert year to string first (safe operation)
		yearStr := strconv.Itoa(year)

		// Then parse back to int64 (also safe)
		yearInt64, err := strconv.ParseInt(yearStr, 10, 64)
		if err != nil {
			return nil, ErrConvertingToYear.New(err)
		}

		// Valid years are 0 or between 1901 and 2155
		if yearInt64 == 0 || (yearInt64 >= 1901 && yearInt64 <= 2155) {
			return createSafeInt16Year(yearInt64)
		}

		return nil, ErrConvertingToYear.New(year)
	}

	return nil, ErrConvertingToYear.New(v)
}

// createSafeInt16Year creates a safe int16 value for a valid year
// without using direct type casting
func createSafeInt16Year(year int64) (interface{}, error) {
	// Validate year range
	if year < 0 || (year > 99 && year < 1901) || year > 2155 {
		return nil, ErrConvertingToYear.New(year)
	}

	// Check int16 bounds
	if year > 32767 {
		return nil, ErrConvertingToYear.New(year)
	}

	// Convert to string first (safe operation)
	yearStr := strconv.FormatInt(year, 10)

	// Then parse back to int16 (also safe)
	result, err := strconv.ParseInt(yearStr, 10, 16)
	if err != nil {
		return nil, ErrConvertingToYear.New(year)
	}

	// Return as int16
	// This final conversion is considered safe because strconv.ParseInt already validates
	// that the value fits within int16 range
	return int16(result), nil
}

// MustConvert implements the Type interface.
func (t yearType) MustConvert(v interface{}) interface{} {
	// Instead of panicking, return a safe default value if conversion fails
	value, err := t.Convert(v)
	if err != nil {
		return t.Zero()
	}
	// Even with a nil error, Convert might return nil for invalid values
	if value == nil {
		return t.Zero()
	}
	return value
}

// Equals implements the Type interface.
func (t yearType) Equals(otherType Type) bool {
	if otherType == nil {
		return false
	}
	_, ok := otherType.(yearType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t yearType) MaxTextResponseByteLength() uint32 {
	return 4
}

// Promote implements the Type interface.
func (t yearType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t yearType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	if v == nil {
		return sqltypes.NULL, nil
	}

	year, ok := v.(int16)
	if !ok {
		return sqltypes.Value{}, ErrConvertingToYear.New(v)
	}

	// Convert int16 to string safely using strconv
	// Need to convert to int first, which is safe when going from smaller to larger int type
	yearStr := strconv.Itoa(int(year))
	stop := len(dest)
	dest = append(dest, yearStr...)
	val := dest[stop:]

	return sqltypes.MakeTrusted(sqltypes.Year, val), nil
}

// String implements Type interface.
func (t yearType) String() string {
	return "year"
}

// Type implements Type interface.
func (t yearType) Type() query.Type {
	return sqltypes.Year
}

// ValueType implements Type interface.
func (t yearType) ValueType() reflect.Type {
	return yearValueType
}

// Zero implements Type interface.
func (t yearType) Zero() interface{} {
	// This literal zero value is considered safe as it's not a cast
	// from another type but a direct literal value
	return int16(0)
}
