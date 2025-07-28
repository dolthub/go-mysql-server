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
			if value > math.MaxInt16 {
				return nil, ErrConvertingToYear.New(value)
			}
			return int16(value), nil
		}

		return nil, ErrConvertingToYear.New(value)
	case uint64:
		// Check if the value exceeds the maximum int64 value
		if value > math.MaxInt64 {
			return nil, ErrConvertingToYear.New("uint64 value out of bounds for int64")
		}

		// If the value is directly within the int16 range and is a valid year, convert directly
		if value <= math.MaxInt16 && ((value >= 1901 && value <= 2155) || value == 0) {
			return int16(value), nil
		}

		// Otherwise, process it through the int64 conversion logic
		return t.Convert(int64(value))
	case float32:
		if float64(value) < float64(math.MinInt16) || float64(value) > float64(math.MaxInt16) {
			return nil, ErrConvertingToYear.New("float32 value out of bounds for int16")
		}
		// Check for fractional part
		if float64(value) != math.Trunc(float64(value)) {
			return nil, ErrConvertingToYear.New("float32 value has a fractional component, cannot convert to int16")
		}
		return int16(value), nil
	case float64:
		if value < float64(math.MinInt16) || value > float64(math.MaxInt16) {
			return nil, ErrConvertingToYear.New("float64 value out of bounds for int16")
		}
		// Check for fractional part
		if value != math.Trunc(value) {
			return nil, ErrConvertingToYear.New("float64 value has a fractional component, cannot convert to int16")
		}
		return int16(value), nil
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
				return int16(2000), nil
			}
			return t.Convert(i)
		}
		return nil, ErrConvertingToYear.New(value)
	case time.Time:
		// Check if time is zero value
		if value.IsZero() {
			return int16(0), nil
		}

		year := value.Year()
		// Valid years are 0 or between 1901 and 2155
		if year == 0 || (year >= 1901 && year <= 2155) {
			if year > math.MaxInt16 || year < math.MinInt16 {
				return nil, ErrConvertingToYear.New(year)
			}
			return int16(year), nil
		}

		return nil, ErrConvertingToYear.New(year)
	}

	return nil, ErrConvertingToYear.New(v)
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

	stop := len(dest)
	dest = strconv.AppendInt(dest, int64(year), 10)
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
	return int16(0)
}
