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

// systemIntType is an internal integer type ONLY for system variables.
type systemIntType struct {
	varName     string
	lowerbound  int64
	upperbound  int64
	negativeOne bool
}

var _ SystemVariableType = systemIntType{}

// NewSystemIntType returns a new systemIntType.
func NewSystemIntType(varName string, lowerbound, upperbound int64, negativeOne bool) SystemVariableType {
	return systemIntType{varName, lowerbound, upperbound, negativeOne}
}

// Compare implements Type interface.
func (t systemIntType) Compare(a interface{}, b interface{}) (int, error) {
	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}
	ai := as.(int64)
	bi := bs.(int64)

	if ai == bi {
		return 0, nil
	}
	if ai < bi {
		return -1, nil
	}
	return 1, nil
}

// Convert implements Type interface.
func (t systemIntType) Convert(v interface{}) (interface{}, error) {
	// String nor nil values are accepted
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
		if value >= t.lowerbound && value <= t.upperbound {
			return value, nil
		}
		if t.negativeOne && value == -1 {
			return value, nil
		}
	case uint64:
		return t.Convert(int64(value))
	case float32:
		return t.Convert(float64(value))
	case float64:
		// Float values aren't truly accepted, but the engine will give them when it should give ints.
		// Therefore, if the float doesn't have a fractional portion, we treat it as an int.
		if value == float64(int64(value)) {
			return t.Convert(int64(value))
		}
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemIntType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t systemIntType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemIntType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(t.Type(), strconv.AppendInt(nil, v.(int64), 10)), nil
}

// String implements Type interface.
func (t systemIntType) String() string {
	return "SYSTEM_INT"
}

// Type implements Type interface.
func (t systemIntType) Type() query.Type {
	return sqltypes.Int64
}

// Zero implements Type interface.
func (t systemIntType) Zero() interface{} {
	return int64(0)
}

// EncodeValue implements SystemVariableType interface.
func (t systemIntType) EncodeValue(val interface{}) (string, error) {
	expectedVal, ok := val.(int64)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}
	return strconv.FormatInt(expectedVal, 10), nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemIntType) DecodeValue(val string) (interface{}, error) {
	parsedVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil, err
	}
	if parsedVal >= t.lowerbound && parsedVal <= t.upperbound {
		return parsedVal, nil
	}
	return nil, ErrSystemVariableCodeFail.New(val, t.String())
}
