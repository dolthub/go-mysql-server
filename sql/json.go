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
	"encoding/json"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

var ErrConvertingToJSON = errors.NewKind("value %v is not valid JSON")

var JSON JsonType = jsonType{}

type JsonType interface {
	Type
}

type jsonType struct{}

// Compare implements Type interface.
func (t jsonType) Compare(a interface{}, b interface{}) (int, error) {
	var err error
	if a, err = t.Convert(a); err != nil {
		return 0, err
	}
	if b, err = t.Convert(b); err != nil {
		return 0, err
	}
	// todo: making a context here is expensive
	return a.(JSONValue).Compare(NewEmptyContext(), b.(JSONValue))
}

// Convert implements Type interface.
func (t jsonType) Convert(v interface{}) (doc interface{}, err error) {
	switch v := v.(type) {
	case JSONValue:
		return v, nil
	case []byte:
		err = json.Unmarshal(v, &doc)
	case string:
		err = json.Unmarshal([]byte(v), &doc)
	default:
		// if |v| can be marshalled, it contains
		// a valid JSON document representation
		if _, err = json.Marshal(v); err == nil {
			return JSONDocument{Val: v}, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return JSONDocument{Val: doc}, nil
}

// Promote implements the Type interface.
func (t jsonType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t jsonType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	js, ok := v.(JSONValue)
	if !ok {
		return sqltypes.NULL, nil
	}

	// todo: making a context here is expensive
	s, err := js.ToString(NewEmptyContext())
	if err != nil {
		return sqltypes.NULL, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(s)), nil
}

// String implements Type interface.
func (t jsonType) String() string {
	return "JSON"
}

// Type implements Type interface.
func (t jsonType) Type() query.Type {
	return sqltypes.TypeJSON
}

// Zero implements Type interface.
func (t jsonType) Zero() interface{} {
	// JSON Null
	return nil
}

// Copy implements deep copy
func Copy(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch v.(type) {
	case map[string]interface{}:
		m := v.(map[string]interface{})
		newMap := make(map[string]interface{})
		for k, value := range m {
			newMap[k] = Copy(value)
		}
		return newMap
	case []interface{}:
		arr := v.([]interface{})
		newArray := make([]interface{}, len(arr))
		for i, doc := range arr {
			newArray[i] = Copy(doc)
		}
		return newArray
	case bool:
		newBool := v.(bool)
		return newBool
	case string:
		newString := v.(string)
		return newString
	case float64:
		newFloat64 := v.(float64)
		return newFloat64
	case float32:
		newFloat32 := v.(float32)
		return newFloat32
	case int:
		newInt := v.(int)
		return newInt
	case int8:
		newInt8 := v.(int8)
		return newInt8
	case int16:
		newInt16 := v.(int16)
		return newInt16
	case int32:
		newInt32 := v.(int32)
		return newInt32
	case int64:
		newInt := v.(int)
		return newInt
	case uint:
		newUInt := v.(uint)
		return newUInt
	case uint8:
		newUInt8 := v.(uint8)
		return newUInt8
	case uint16:
		newUInt16 := v.(uint16)
		return newUInt16
	case uint32:
		newUInt32 := v.(uint32)
		return newUInt32
	case uint64:
		newUInt64 := v.(uint64)
		return newUInt64
	default:
		return nil
	}
}
