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
	"strconv"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// systemDoubleType is an internal double type ONLY for system variables.
type systemDoubleType struct {
	varName    string
	lowerbound float64
	upperbound float64
}

var _ SystemVariableType = systemDoubleType{}

// NewSystemDoubleType returns a new systemDoubleType.
func NewSystemDoubleType(varName string, lowerbound, upperbound float64) SystemVariableType {
	return systemDoubleType{varName, lowerbound, upperbound}
}

// Compare implements Type interface.
func (t systemDoubleType) Compare(a interface{}, b interface{}) (int, error) {
	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}
	ai := as.(float64)
	bi := bs.(float64)

	if ai == bi {
		return 0, nil
	}
	if ai < bi {
		return -1, nil
	}
	return 1, nil
}

// Convert implements Type interface.
func (t systemDoubleType) Convert(v interface{}) (interface{}, error) {
	// String nor nil values are accepted
	switch value := v.(type) {
	case int:
		return t.Convert(float64(value))
	case uint:
		return t.Convert(float64(value))
	case int8:
		return t.Convert(float64(value))
	case uint8:
		return t.Convert(float64(value))
	case int16:
		return t.Convert(float64(value))
	case uint16:
		return t.Convert(float64(value))
	case int32:
		return t.Convert(float64(value))
	case uint32:
		return t.Convert(float64(value))
	case int64:
		return t.Convert(float64(value))
	case uint64:
		return t.Convert(float64(value))
	case float32:
		return t.Convert(float64(value))
	case float64:
		if value >= t.lowerbound && value <= t.upperbound {
			return value, nil
		}
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemDoubleType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t systemDoubleType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemDoubleType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(t.Type(), strconv.AppendFloat(nil, v.(float64), 'f', -1, 64)), nil
}

// String implements Type interface.
func (t systemDoubleType) String() string {
	return "SYSTEM_DOUBLE"
}

// Type implements Type interface.
func (t systemDoubleType) Type() query.Type {
	return sqltypes.Float64
}

// Zero implements Type interface.
func (t systemDoubleType) Zero() interface{} {
	return float64(0)
}

// EncodeValue implements SystemVariableType interface.
func (t systemDoubleType) EncodeValue(val interface{}) (string, error) {
	expectedVal, ok := val.(float64)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}
	return strconv.FormatFloat(expectedVal, 'f', -1, 64), nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemDoubleType) DecodeValue(val string) (interface{}, error) {
	parsedVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if parsedVal >= t.lowerbound && parsedVal <= t.upperbound {
		return parsedVal, nil
	}
	return nil, ErrSystemVariableCodeFail.New(val, t.String())
}
