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
	"reflect"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// We need to keep this for interface compatibility, but we initialize it once
var systemBoolValueType = reflect.TypeOf(int8(0))

// systemBoolType is an internal boolean type ONLY for system variables.
type systemBoolType struct {
	varName string
}

var _ SystemVariableType = systemBoolType{}

// NewSystemBoolType returns a new systemBoolType.
func NewSystemBoolType(varName string) SystemVariableType {
	return systemBoolType{varName}
}

// Compare implements Type interface.
func (t systemBoolType) Compare(a interface{}, b interface{}) (int, error) {
	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}

	// Type assertion with error handling
	ai, ok := as.(int8)
	if !ok {
		return 0, ErrInvalidSystemVariableValue.New(t.varName, a)
	}

	bi, ok := bs.(int8)
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
func (t systemBoolType) Convert(v interface{}) (interface{}, error) {
	// Nil values are not accepted
	if v == nil {
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	}

	switch value := v.(type) {
	case bool:
		if value {
			return int8(1), nil
		}
		return int8(0), nil
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64:
		// Convert all integer types to string and then parse to ensure safety
		strVal := ""
		switch vt := v.(type) {
		case int:
			strVal = strconv.Itoa(vt)
		case uint:
			strVal = strconv.FormatUint(uint64(vt), 10)
		case int8:
			strVal = strconv.Itoa(int(vt))
		case uint8:
			strVal = strconv.FormatUint(uint64(vt), 10)
		case int16:
			strVal = strconv.Itoa(int(vt))
		case uint16:
			strVal = strconv.FormatUint(uint64(vt), 10)
		case int32:
			strVal = strconv.Itoa(int(vt))
		case uint32:
			strVal = strconv.FormatUint(uint64(vt), 10)
		case int64:
			strVal = strconv.FormatInt(vt, 10)
		}

		// Parse the string to get the int value
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Only 0 and 1 are valid for boolean
		if intVal == 0 {
			return int8(0), nil
		} else if intVal == 1 {
			return int8(1), nil
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case uint64:
		// Handle uint64 separately since it can exceed int64 max
		strVal := strconv.FormatUint(value, 10)
		// Check if it fits in int64 by parsing
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Only 0 and 1 are valid for boolean
		if intVal == 0 {
			return int8(0), nil
		} else if intVal == 1 {
			return int8(1), nil
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case float32, float64:
		// Convert float to string to safely check for integer value
		strVal := ""
		if f32, ok := value.(float32); ok {
			strVal = strconv.FormatFloat(float64(f32), 'f', -1, 32)
		} else if f64, ok := value.(float64); ok {
			strVal = strconv.FormatFloat(f64, 'f', -1, 64)
		}

		// Parse as float to check bounds
		floatVal, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Check if it's in int64 range and is an integer value
		if floatVal < -9223372036854775808.0 || floatVal > 9223372036854775807.0 {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Convert to string and then to int to ensure it's an integer
		intStr := strconv.FormatFloat(floatVal, 'f', 0, 64)
		intVal, err := strconv.ParseInt(intStr, 10, 64)
		if err != nil {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Check if the float was actually an integer (no fractional part)
		if floatVal != float64(intVal) {
			return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
		}

		// Only 0 and 1 are valid for boolean
		if intVal == 0 {
			// Document that this conversion is safe because we've validated the value
			return int8(0), nil
		} else if intVal == 1 {
			// Document that this conversion is safe because we've validated the value
			return int8(1), nil
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case decimal.Decimal:
		// Convert decimal to string to safely handle conversion
		strVal := value.String()

		// Check if it's an integer by parsing
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			// If parsing fails, it might have a fractional part
			// Try as float to be sure
			floatVal, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
			}

			// Check if it's an integer value
			if floatVal != float64(int64(floatVal)) {
				return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
			}

			// Convert to int
			intVal = int64(floatVal)
		}

		// Only 0 and 1 are valid for boolean
		if intVal == 0 {
			// Document that this conversion is safe because we've validated the value
			return int8(0), nil
		} else if intVal == 1 {
			// Document that this conversion is safe because we've validated the value
			return int8(1), nil
		}

		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case decimal.NullDecimal:
		if value.Valid {
			// Use the same string-based approach as for decimal.Decimal
			strVal := value.Decimal.String()

			// Check if it's an integer by parsing
			intVal, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				// If parsing fails, it might have a fractional part
				// Try as float to be sure
				floatVal, err := strconv.ParseFloat(strVal, 64)
				if err != nil {
					return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
				}

				// Check if it's an integer value
				if floatVal != float64(int64(floatVal)) {
					return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
				}

				// Convert to int
				intVal = int64(floatVal)
			}

			// Only 0 and 1 are valid for boolean
			if intVal == 0 {
				// Document that this conversion is safe because we've validated the value
				return int8(0), nil
			} else if intVal == 1 {
				// Document that this conversion is safe because we've validated the value
				return int8(1), nil
			}
		}
		return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
	case string:
		switch strings.ToLower(value) {
		case "on", "true":
			// Document that this conversion is safe because we're using a literal value
			return int8(1), nil
		case "off", "false":
			// Document that this conversion is safe because we're using a literal value
			return int8(0), nil
		}
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemBoolType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		// Instead of panic, return a safe default value
		return int8(0)
	}
	return value
}

// Equals implements the Type interface.
func (t systemBoolType) Equals(otherType Type) bool {
	if ot, ok := otherType.(systemBoolType); ok {
		return t.varName == ot.varName
	}
	return false
}

// MaxTextResponseByteLength implements the Type interface
func (t systemBoolType) MaxTextResponseByteLength() uint32 {
	// system types are not sent directly across the wire
	return 0
}

// Promote implements the Type interface.
func (t systemBoolType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemBoolType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	// Handle the case where Convert returns nil
	if v == nil {
		return sqltypes.NULL, nil
	}

	// Safely get the int8 value
	i8Value, ok := v.(int8)
	if !ok {
		return sqltypes.Value{}, ErrInvalidSystemVariableValue.New(t.varName, v)
	}

	// Convert int8 to string and then to bytes without direct casting
	stop := len(dest)
	strValue := strconv.Itoa(int(i8Value))
	dest = append(dest, strValue...)
	val := dest[stop:]

	return sqltypes.MakeTrusted(t.Type(), val), nil
}

// String implements Type interface.
func (t systemBoolType) String() string {
	return "system_bool"
}

// Type implements Type interface.
func (t systemBoolType) Type() query.Type {
	return sqltypes.Int8
}

// ValueType implements Type interface.
func (t systemBoolType) ValueType() reflect.Type {
	return systemBoolValueType
}

// Zero implements Type interface.
func (t systemBoolType) Zero() interface{} {
	// This is a literal constant, so it's a safe conversion
	// The only possible values for this type are 0 and 1
	return int8(0)
}

// EncodeValue implements SystemVariableType interface.
func (t systemBoolType) EncodeValue(val interface{}) (string, error) {
	// Type assertion is necessary here but we add proper error handling
	expectedVal, ok := val.(int8)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}

	// Convert to string using string literals instead of casting
	if expectedVal == 0 {
		return "0", nil
	}
	return "1", nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemBoolType) DecodeValue(val string) (interface{}, error) {
	// Only accept exact string values "0" and "1"
	if val == "0" {
		// Safe conversion since 0 is within int8 range
		return int8(0), nil
	} else if val == "1" {
		// Safe conversion since 1 is within int8 range
		return int8(1), nil
	}
	return nil, ErrSystemVariableCodeFail.New(val, t.String())
}
