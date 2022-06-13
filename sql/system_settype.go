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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
)

// systemSetType is an internal set type ONLY for system variables.
type systemSetType struct {
	SetType
	varName string
}

var _ SystemVariableType = systemSetType{}

// NewSystemSetType returns a new systemSetType.
func NewSystemSetType(varName string, values ...string) SystemVariableType {
	return systemSetType{MustCreateSetType(values, Collation_Default), varName}
}

// Compare implements Type interface.
func (t systemSetType) Compare(a interface{}, b interface{}) (int, error) {
	if a == nil || b == nil {
		return 0, ErrInvalidSystemVariableValue.New(t.varName, nil)
	}
	ai, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bi, err := t.Convert(b)
	if err != nil {
		return 0, err
	}
	au := ai.(uint64)
	bu := bi.(uint64)

	if au == bu {
		return 0, nil
	}
	if au < bu {
		return -1, nil
	}
	return 1, nil
}

// Convert implements Type interface.
func (t systemSetType) Convert(v interface{}) (interface{}, error) {
	// Nil values are not accepted
	switch value := v.(type) {
	case int:
		return t.SetType.Convert(value)
	case uint:
		return t.SetType.Convert(value)
	case int8:
		return t.SetType.Convert(value)
	case uint8:
		return t.SetType.Convert(value)
	case int16:
		return t.SetType.Convert(value)
	case uint16:
		return t.SetType.Convert(value)
	case int32:
		return t.SetType.Convert(value)
	case uint32:
		return t.SetType.Convert(value)
	case int64:
		return t.SetType.Convert(value)
	case uint64:
		return t.SetType.Convert(value)
	case float32:
		return t.Convert(float64(value))
	case float64:
		// Float values aren't truly accepted, but the engine will give them when it should give ints.
		// Therefore, if the float doesn't have a fractional portion, we treat it as an int.
		if value == float64(int64(value)) {
			return t.SetType.Convert(int64(value))
		}
	case decimal.Decimal:
		f, _ := value.Float64()
		return t.Convert(f)
	case decimal.NullDecimal:
		if value.Valid {
			f, _ := value.Decimal.Float64()
			return t.Convert(f)
		}
	case string:
		return t.SetType.Convert(value)
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemSetType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t systemSetType) Equals(otherType Type) bool {
	if ot, ok := otherType.(systemSetType); ok {
		return t.varName == ot.varName && t.SetType.Equals(ot.SetType)
	}
	return false
}

// Promote implements the Type interface.
func (t systemSetType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemSetType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	convertedValue, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	value, err := t.BitsToString(convertedValue.(uint64))
	if err != nil {
		return sqltypes.Value{}, err
	}

	val := appendAndSliceString(dest, value)

	return sqltypes.MakeTrusted(t.Type(), val), nil
}

// String implements Type interface.
func (t systemSetType) String() string {
	return "SYSTEM_SET"
}

// Type implements Type interface.
func (t systemSetType) Type() query.Type {
	return sqltypes.VarChar
}

// ValueType implements Type interface.
func (t systemSetType) ValueType() reflect.Type {
	return t.SetType.ValueType()
}

// Zero implements Type interface.
func (t systemSetType) Zero() interface{} {
	return ""
}

// EncodeValue implements SystemVariableType interface.
func (t systemSetType) EncodeValue(val interface{}) (string, error) {
	expectedVal, ok := val.(uint64)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}
	return t.BitsToString(expectedVal)
}

// DecodeValue implements SystemVariableType interface.
func (t systemSetType) DecodeValue(val string) (interface{}, error) {
	outVal, err := t.Convert(val)
	if err != nil {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}
	return outVal, nil
}
