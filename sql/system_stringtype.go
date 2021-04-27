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
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// systemStringType is an internal string type ONLY for system variables.
type systemStringType struct {
	varName string
}

var _ SystemVariableType = systemStringType{}

// NewSystemStringType returns a new systemStringType.
func NewSystemStringType(varName string) SystemVariableType {
	return systemStringType{varName}
}

// Compare implements Type interface.
func (t systemStringType) Compare(a interface{}, b interface{}) (int, error) {
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
func (t systemStringType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return "", nil
	}
	if value, ok := v.(string); ok {
		return value, nil
	}

	return nil, ErrInvalidSystemVariableValue.New(t.varName, v)
}

// MustConvert implements the Type interface.
func (t systemStringType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t systemStringType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t systemStringType) SQL(v interface{}) (sqltypes.Value, error) {
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
func (t systemStringType) String() string {
	return "SYSTEM_STRING"
}

// Type implements Type interface.
func (t systemStringType) Type() query.Type {
	return sqltypes.VarChar
}

// Zero implements Type interface.
func (t systemStringType) Zero() interface{} {
	return ""
}

// EncodeValue implements SystemVariableType interface.
func (t systemStringType) EncodeValue(val interface{}) (string, error) {
	expectedVal, ok := val.(string)
	if !ok {
		return "", ErrSystemVariableCodeFail.New(val, t.String())
	}
	return expectedVal, nil
}

// DecodeValue implements SystemVariableType interface.
func (t systemStringType) DecodeValue(val string) (interface{}, error) {
	return val, nil
}
