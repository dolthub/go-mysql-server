// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"encoding/json"
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	jsonValueType = reflect.TypeOf((*JSONValue)(nil)).Elem()

	MaxJsonFieldByteLength = int64(1024) * int64(1024) * int64(1024)
)

var JSON sql.Type = JsonType{}

type JsonType struct{}

// Compare implements Type interface.
func (t JsonType) Compare(a interface{}, b interface{}) (int, error) {
	var err error
	if a, err = t.Convert(a); err != nil {
		return 0, err
	}
	if b, err = t.Convert(b); err != nil {
		return 0, err
	}
	// todo: making a context here is expensive
	return a.(JSONValue).Compare(sql.NewEmptyContext(), b.(JSONValue))
}

// Convert implements Type interface.
func (t JsonType) Convert(v interface{}) (doc interface{}, err error) {
	switch v := v.(type) {
	case JSONValue:
		return v, nil
	case []byte:
		if int64(len(v)) > MaxJsonFieldByteLength {
			return nil, ErrLengthTooLarge.New(len(v), MaxJsonFieldByteLength)
		}
		err = json.Unmarshal(v, &doc)
		if err != nil {
			return nil, sql.ErrInvalidJson.New(err.Error())
		}
	case string:
		charsetMaxLength := sql.Collation_Default.CharacterSet().MaxLength()
		length := int64(len(v)) * charsetMaxLength
		if length > MaxJsonFieldByteLength {
			return nil, ErrLengthTooLarge.New(length, MaxJsonFieldByteLength)
		}
		err = json.Unmarshal([]byte(v), &doc)
		if err != nil {
			return nil, sql.ErrInvalidJson.New(err.Error())
		}
	default:
		// if |v| can be marshalled, it contains
		// a valid JSON document representation
		if b, berr := json.Marshal(v); berr == nil {
			if int64(len(b)) > MaxJsonFieldByteLength {
				return nil, ErrLengthTooLarge.New(len(b), MaxJsonFieldByteLength)
			}
			err = json.Unmarshal(b, &doc)
			if err != nil {
				return nil, sql.ErrInvalidJson.New(err.Error())
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return JSONDocument{Val: doc}, nil
}

// Equals implements the Type interface.
func (t JsonType) Equals(otherType sql.Type) bool {
	_, ok := otherType.(JsonType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t JsonType) MaxTextResponseByteLength() uint32 {
	return uint32(MaxJsonFieldByteLength*sql.Collation_Default.CharacterSet().MaxLength()) - 1
}

// Promote implements the Type interface.
func (t JsonType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t JsonType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	// Convert to jsonType
	jsVal, err := t.Convert(v)
	if err != nil {
		return sqltypes.NULL, err
	}
	js := jsVal.(JSONValue)

	s, err := js.ToString(ctx)
	if err != nil {
		return sqltypes.NULL, err
	}

	val := AppendAndSliceString(dest, s)

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, val), nil
}

// String implements Type interface.
func (t JsonType) String() string {
	return "json"
}

// Type implements Type interface.
func (t JsonType) Type() query.Type {
	return sqltypes.TypeJSON
}

// ValueType implements Type interface.
func (t JsonType) ValueType() reflect.Type {
	return jsonValueType
}

// Zero implements Type interface.
func (t JsonType) Zero() interface{} {
	// MySQL throws an error for INSERT IGNORE, UPDATE IGNORE, etc. when bad json is encountered:
	// ERROR 3140 (22032): Invalid JSON text: "Invalid value." at position 0 in value for column 'table.column'.
	return nil
}

// DeepCopyJson implements deep copy of JSON document
func DeepCopyJson(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch v.(type) {
	case map[string]interface{}:
		m := v.(map[string]interface{})
		newMap := make(map[string]interface{})
		for k, value := range m {
			newMap[k] = DeepCopyJson(value)
		}
		return newMap
	case []interface{}:
		arr := v.([]interface{})
		newArray := make([]interface{}, len(arr))
		for i, doc := range arr {
			newArray[i] = DeepCopyJson(doc)
		}
		return newArray
	case bool, string, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return v
	default:
		return nil
	}
}

func MustJSON(s string) JSONDocument {
	var doc interface{}
	if err := json.Unmarshal([]byte(s), &doc); err != nil {
		panic(err)
	}
	return JSONDocument{Val: doc}
}
