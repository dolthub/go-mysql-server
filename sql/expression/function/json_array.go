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

package function

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// JSON_ARRAY([val[, val] ...])
//
// JSONArray Evaluates a (possibly empty) list of values and returns a JSON array containing those values.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-array

type JSONArray struct {
	Docs []sql.Expression
}

var _ sql.FunctionExpression = (*JSONArray)(nil)

// NewJSONArray creates a new JSONArray function.
func NewJSONArray(args ...sql.Expression) (sql.Expression, error) {
	return &JSONArray{Docs: args}, nil
}

// FunctionName implements sql.FunctionExpression
func (j JSONArray) FunctionName() string {
	return "json_array"
}

// Description implements sql.FunctionExpression
func (j JSONArray) Description() string {
	return "creates JSON array."
}

// IsUnsupported implements sql.UnsupportedFunctionStub
func (j JSONArray) IsUnsupported() bool {
	return false
}

// Resolved implements the Expression interface.
func (j *JSONArray) Resolved() bool {
	for _, d := range j.Docs {
		if !d.Resolved() {
			return false
		}
	}
	return true
}

// String implements the Expression interface.
func (j *JSONArray) String() string {
	children := j.Children()
	var parts = make([]string, len(children))

	for i, c := range children {
		parts[i] = c.String()
	}

	return fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(parts, ", "))
}

// Type implements the Expression interface.
func (j *JSONArray) Type() sql.Type {
	return sql.JSON
}

// IsNullable implements the Expression interface.
func (j *JSONArray) IsNullable() bool {
	for _, d := range j.Docs {
		if d.IsNullable() {
			return true
		}
	}
	return false
}

// Eval implements the Expression interface.
func (j *JSONArray) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(j.Docs) == 0 {
		return sql.JSONDocument{Val: make([]interface{}, 0)}, nil
	}

	var resultArray = make([]interface{}, len(j.Docs))

	for i, doc := range j.Docs {
		jsonDoc, err := doc.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		jsonDoc, err = j.Type().Convert(jsonDoc)
		if err != nil {
			return nil, err
		}

		resultArray[i] = jsonInputToString(jsonDoc.(sql.JSONDocument).Val)
	}

	return sql.JSONDocument{Val: resultArray}, nil
}

// Children implements the Expression interface.
func (j *JSONArray) Children() []sql.Expression {
	return j.Docs
}

// WithChildren implements the Expression interface.
func (j *JSONArray) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(j.Children()) != len(children) {
		return nil, fmt.Errorf("json_array did not receive the correct amount of args")
	}

	return NewJSONArray(children...)
}

// jsonInputToString returns string representation of a json document
func jsonInputToString(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch v.(type) {
	case map[string]interface{}:
		m := v.(map[string]interface{})
		var keys []string
		for k, _ := range m {
			keys = append(keys, k)
		}
		return innerDocToString(v, keys)
	case []interface{}:
		return innerDocToString(v, nil)
	case bool, string, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return v
	default:
		return ""
	}
}

func innerDocToString(d interface{}, keys []string) string {
	if d == nil {
		return "NULL"
	}

	switch d.(type) {
	case map[string]interface{}:
		m := d.(map[string]interface{})
		newString := "{"
		for _, k := range keys {
			if mm, ok := m[k].(map[string]interface{}); ok {
				var mmKeys []string
				for mk, _ := range mm {
					mmKeys = append(mmKeys, mk)
				}
				newString += fmt.Sprintf("\"%s\"", k) + ": " + innerDocToString(mm, mmKeys) + ", "
			} else {
				newString += fmt.Sprintf("\"%s\"", k) + ": " + innerDocToString(m[k], nil) + ", "
			}
		}
		newString = strings.TrimSuffix(newString, ", ") + "}"
		return newString
	case []interface{}:
		arr := d.([]interface{})
		newString := "["
		for _, value := range arr {
			newString += innerDocToString(value, nil) + ", "
		}
		newString = strings.TrimSuffix(newString, ", ") + "]"
		return newString
	case string:
		res, err := json.Marshal(d)
		if err != nil {
			return ""
		}
		return string(res)
	case bool, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", d)
	default:
		return ""
	}
}
