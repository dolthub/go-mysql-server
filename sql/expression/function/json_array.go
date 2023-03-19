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
	"fmt"
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

// JSON_ARRAY([val[, val] ...])
//
// JSONArray Evaluates a (possibly empty) list of values and returns a JSON array containing those values.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-array

type JSONArray struct {
	vals []sql.Expression
}

var _ sql.FunctionExpression = (*JSONArray)(nil)

// NewJSONArray creates a new JSONArray function.
func NewJSONArray(args ...sql.Expression) (sql.Expression, error) {
	return &JSONArray{vals: args}, nil
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
	for _, d := range j.vals {
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

	return fmt.Sprintf("%s(%s)", j.FunctionName(), strings.Join(parts, ","))
}

// Type implements the Expression interface.
func (j *JSONArray) Type() sql.Type {
	return types.JSON
}

// IsNullable implements the Expression interface.
func (j *JSONArray) IsNullable() bool {
	for _, d := range j.vals {
		if d.IsNullable() {
			return true
		}
	}
	return false
}

// Eval implements the Expression interface.
func (j *JSONArray) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(j.vals) == 0 {
		return types.JSONDocument{Val: make([]interface{}, 0)}, nil
	}

	var resultArray = make([]interface{}, len(j.vals))

	for i, vs := range j.vals {
		val, err := vs.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if json, ok := val.(types.JSONValue); ok {
			doc, err := json.(types.JSONValue).Unmarshall(ctx)
			if err != nil {
				return nil, err
			}
			val = doc.Val
		}
		resultArray[i] = val
	}

	return types.JSONDocument{Val: resultArray}, nil
}

// Children implements the Expression interface.
func (j *JSONArray) Children() []sql.Expression {
	return j.vals
}

// WithChildren implements the Expression interface.
func (j *JSONArray) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(j.Children()) != len(children) {
		return nil, fmt.Errorf("json_array did not receive the correct amount of args")
	}

	return NewJSONArray(children...)
}
