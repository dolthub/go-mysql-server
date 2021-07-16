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

package function

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// JSON_OBJECT([key, val[, key, val] ...])
//
// JSONObject Evaluates a (possibly empty) list of key-value pairs and returns a JSON object containing those pairs. An
// error occurs if any key name is NULL or the number of arguments is odd.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-object
type JSONObject struct {
	keyValPairs []sql.Expression
}

var _ sql.FunctionExpression = JSONObject{}

// NewJSONObject creates a new JSONObject function.
func NewJSONObject(ctx *sql.Context, exprs ...sql.Expression) (sql.Expression, error) {
	if len(exprs)%2 != 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("JSON_OBJECT", "an even number of", len(exprs))
	}

	return JSONObject{keyValPairs: exprs}, nil
}

// FunctionName implements sql.FunctionExpression
func (j JSONObject) FunctionName() string {
	return "json_object"
}

func (j JSONObject) Resolved() bool {
	for _, child := range j.Children() {
		if child != nil && !child.Resolved() {
			return false
		}
	}

	return true
}

func (j JSONObject) String() string {
	children := j.Children()
	var parts = make([]string, len(children))

	for i, c := range children {
		parts[i] = c.String()
	}

	return fmt.Sprintf("JSON_OBJECT(%s)", strings.Join(parts, ", "))
}

func (j JSONObject) Type() sql.Type {
	return sql.JSON
}

func (j JSONObject) IsNullable() bool {
	return false
}

func (j JSONObject) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	obj := make(map[string]interface{}, len(j.keyValPairs)/2)

	var key string
	for i, expr := range j.keyValPairs {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if i%2 == 0 {
			var ok bool
			if key, ok = val.(string); !ok {
				return nil, sql.ErrInvalidType.New(expr.Type())
			}
		} else {
			obj[key] = val
		}
	}

	return sql.JSONDocument{Val: obj}, nil
}

func (j JSONObject) Children() []sql.Expression {
	return j.keyValPairs
}

func (j JSONObject) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(j.Children()) != len(children) {
		return nil, fmt.Errorf("json_object did not receive the correct amount of args")
	}

	return NewJSONObject(ctx, children...)
}
