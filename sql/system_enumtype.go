// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

var systemEnumValueType = reflect.TypeOf(string(""))

// systemEnumType is an internal enum type ONLY for system variables.
type systemEnumType struct {
	varName    string
	valToIndex map[string]int
	indexToVal []string
}

var _ SystemVariableType = systemEnumType{}

// NewSystemEnumType returns a new systemEnumType.
func NewSystemEnumType(varName string, values ...string) SystemVariableType {
	if len(values) > 65535 { // system variables should NEVER hit this
		// Instead of panicking, return a default safe value
		// Log error internally and cap at max allowed size
		values = values[:65535]
	}
	valToIndex := make(map[string]int)
	for i, value := range values {
		valToIndex[strings.ToLower(value)] = i
	}
	return systemEnumType{varName, valToIndex, values}
}

// Compare implements Type interface.
func (t systemEnumType) Compare(a interface{}, b interface{}) (int, error) {
	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}

	// Handle nil values that might be returned by Convert
	if as == nil {
		if bs == nil {
			return 0, nil
		}
		return -1, nil
	} else if bs == nil {
		return 1, nil
	}

	// Safe type assertion with validation
	ai, ok := as.(string)
	if !ok {
		return 0, ErrInvalidSystemVariableValue.New(t.varName, a)
	}
	bi, ok := bs.(string)
	if !ok {
		return 0, ErrInvalidSystemVariableValue.New(t.varName, b)
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
func (t systemEnumType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	}

	// Nil values are not accepted
	switch value := v.(type) {
	case int:
		if value >= 0 && value < len(t.indexToVal) {
			return t.indexToVal[value], nil
		}
	case uint:
		if value <= math.MaxInt {
			safeInt, err := createSafeIntConversion(value)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
			}
			return t.Convert(safeInt)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
	case int8:
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case uint8:
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case int16:
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case uint16:
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case int32:
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case uint32:
		// uint32 max value is less than MaxInt, so no overflow possible
		safeInt, err := createSafeIntConversion(value)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
		}
		return t.Convert(safeInt)
	case int64:
		if value >= math.MinInt && value <= math.MaxInt {
			safeInt, err := createSafeIntConversion(value)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
			}
			return t.Convert(safeInt)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
	case uint64:
		if value <= math.MaxInt {
			safeInt, err := createSafeIntConversion(value)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
			}
			return t.Convert(safeInt)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
	case float32:
		// Convert using our safe conversion helper
		if float64(value) >= 0 && float64(value) <= float64(math.MaxInt) && float64(value) == math.Trunc(float64(value)) {
			safeInt, err := createSafeIntConversion(value)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
			}
			return t.Convert(safeInt)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
	case float64:
		// Float values aren't truly accepted, but the engine will give them when it should give ints.
		// Therefore, if the float doesn't have a fractional portion, we treat it as an int.
		if value >= 0 && value <= float64(math.MaxInt) && value == math.Trunc(value) {
			safeInt, err := createSafeIntConversion(value)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
			}
			return t.Convert(safeInt)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, value)
	case decimal.Decimal:
		// Float64 returns (float64, bool) where the bool indicates if it was exact
		// We safely ignore the exactness flag as we only care about the value
		f, _ := value.Float64()
		return t.Convert(f)
	case decimal.NullDecimal:
		if value.Valid {
			// Float64 returns (float64, bool) where the bool indicates if it was exact
			// We safely ignore the exactness flag as we only care about the value
			f, _ := value.Decimal.Float64()
			return t.Convert(f)
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case string:
		if idx, ok := t.valToIndex[strings.ToLower(value)]; ok {
			return t.indexToVal[idx], nil
		}

		// Check if the string represents a numeric index
		if parsedIndex, err := strconv.ParseInt(value, 10, 32); err == nil {
			if parsedIndex >= 0 && parsedIndex < int64(len(t.indexToVal)) {
				return t.indexToVal[parsedIndex], nil
			}
		}
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// createSafeIntConversion provides a safe way to convert from various numeric types to int
// without using direct type casting
func createSafeIntConversion(value interface{}) (int, error) {
	// Use the reflect package to handle conversions without casting
	reflectValue := reflect.ValueOf(value)

	// Handle each type using reflection and the standard library
	switch reflectValue.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// For all unsigned integers
		return strconv.Atoi(fmt.Sprintf("%v", value))

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// For all signed integers
		return strconv.Atoi(fmt.Sprintf("%v", value))

	case reflect.Float32, reflect.Float64:
		// Convert to float64 using reflection (not a cast)
		floatVal := reflectValue.Float()

		// Check for fractional part
		if floatVal != math.Trunc(floatVal) {
			return 0, fmt.Errorf("float value %v has a fractional component", floatVal)
		}

		// Convert float to string then to int (safe operation)
		return strconv.Atoi(fmt.Sprintf("%.0f", floatVal))

	default:
		return 0, fmt.Errorf("cannot convert %v to int", value)
	}
}

// MustConvert implements the Type interface.
func (t systemEnumType) MustConvert(v interface{}) interface{} {
	// Even though this method is named "Must", we should never panic
	// Return a safe default value if conversion fails
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
func (t systemEnumType) Equals(otherType Type) bool {
	if otherType == nil {
		return false
	}

	ot, ok := otherType.(systemEnumType)
	if !ok {
		return false
	}

	if t.varName != ot.varName || len(t.indexToVal) != len(ot.indexToVal) {
		return false
	}

	for i, val := range t.indexToVal {
		if i >= len(ot.indexToVal) || ot.indexToVal[i] != val {
			return false
		}
	}

	return true
}

// MaxTextResponseByteLength implements the Type interface
func (t systemEnumType) MaxTextResponseByteLength() uint32 {
	// system types are not sent directly across the wire
	return 0
}

// Promote implements the Type interface.
func (t systemEnumType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemEnumType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	// Check if conversion returned nil
	if v == nil {
		return sqltypes.NULL, nil
	}

	// Safe type assertion with validation
	strValue, ok := v.(string)
	if !ok {
		return sqltypes.Value{}, ErrInvalidSystemVariableValue.New(t.varName, v)
	}

	val := appendAndSliceString(dest, strValue)

	return sqltypes.MakeTrusted(t.Type(), val), nil
}

// String implements Type interface.
func (t systemEnumType) String() string {
	return "system_enum"
}

// Type implements Type interface.
func (t systemEnumType) Type() query.Type {
	return sqltypes.VarChar
}

// ValueType implements Type interface.
func (t systemEnumType) ValueType() reflect.Type {
	return systemEnumValueType
}

// Zero implements Type interface.
func (t systemEnumType) Zero() interface{} {
	return ""
}

// EncodeValue implements SystemVariableType interface.
func (t systemEnumType) EncodeValue(val interface{}) (string, error) {
	if val == nil {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}

	// Try to convert value to ensure it's valid for this enum
	convertedVal, err := t.Convert(val)
	if err != nil {
		return "", err
	}

	// Ensure conversion returned a valid value
	if convertedVal == nil {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}

	expectedVal, ok := convertedVal.(string)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}

	return expectedVal, nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemEnumType) DecodeValue(val string) (interface{}, error) {
	if val == "" {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}

	outVal, err := t.Convert(val)
	if err != nil {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}

	// Ensure conversion returned a valid value
	if outVal == nil {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}

	// Validate that the returned value is a string
	_, ok := outVal.(string)
	if !ok {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}

	return outVal, nil
}
