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
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

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
		panic(varName + " somehow has more than 65535 values")
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
	ai := as.(string)
	bi := bs.(string)

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
	// Nil values are not accepted
	switch value := v.(type) {
	case int:
		if value >= 0 && value < len(t.indexToVal) {
			return t.indexToVal[value], nil
		}
	case uint:
		return t.Convert(int(value))
	case int8:
		return t.Convert(int(value))
	case uint8:
		return t.Convert(int(value))
	case int16:
		return t.Convert(int(value))
	case uint16:
		return t.Convert(int(value))
	case int32:
		return t.Convert(int(value))
	case uint32:
		return t.Convert(int(value))
	case int64:
		return t.Convert(int(value))
	case uint64:
		return t.Convert(int(value))
	case float32:
		return t.Convert(float64(value))
	case float64:
		// Float values aren't truly accepted, but the engine will give them when it should give ints.
		// Therefore, if the float doesn't have a fractional portion, we treat it as an int.
		if value == float64(int(value)) {
			return t.Convert(int(value))
		}
	case string:
		if idx, ok := t.valToIndex[strings.ToLower(value)]; ok {
			return t.indexToVal[idx], nil
		}
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemEnumType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t systemEnumType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemEnumType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(t.Type(), []byte(v.(string))), nil
}

// String implements Type interface.
func (t systemEnumType) String() string {
	return "SYSTEM_ENUM"
}

// Type implements Type interface.
func (t systemEnumType) Type() query.Type {
	return sqltypes.VarChar
}

// Zero implements Type interface.
func (t systemEnumType) Zero() interface{} {
	return ""
}

// EncodeValue implements SystemVariableType interface.
func (t systemEnumType) EncodeValue(val interface{}) (string, error) {
	expectedVal, ok := val.(string)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}
	return expectedVal, nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemEnumType) DecodeValue(val string) (interface{}, error) {
	outVal, err := t.Convert(val)
	if err != nil {
		return nil, ErrSystemVariableCodeFail.New(val, t.String())
	}
	return outVal, nil
}
