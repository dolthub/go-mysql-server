// Copyright 2024 Dolthub, Inc.
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

package json

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// JSONType (json_val)
//
// Returns a utf8mb4 string indicating the type of a JSON value. This can be an object, an array, or a scalar type.
// JSONType returns NULL if the argument is NULL. An error occurs if the argument is not a valid JSON value
//
// https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-type
type JSONType struct {
	JSON sql.Expression
}

var _ sql.FunctionExpression = &JSONType{}

// NewJSONType creates a new JSONType function.
func NewJSONType(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("JSON_TYPE", "1", len(args))
	}
	return &JSONType{JSON: args[0]}, nil
}

// FunctionName implements sql.FunctionExpression
func (j JSONType) FunctionName() string {
	return "json_type"
}

// Description implements sql.FunctionExpression
func (j JSONType) Description() string {
	return "returns type of JSON value."
}

// Resolved implements the Expression interface.
func (j JSONType) Resolved() bool {
	return j.JSON.Resolved()
}

// String implements the fmt.Stringer interface.
func (j JSONType) String() string {
	return fmt.Sprintf("%s(%s)", j.FunctionName(), j.JSON.String())
}

// Type implements the Expression interface.
func (j JSONType) Type() sql.Type {
	return types.Text
}

// IsNullable implements the Expression interface.
func (j JSONType) IsNullable() bool {
	return j.JSON.IsNullable()
}

// Eval implements the Expression interface.
func (j JSONType) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span(fmt.Sprintf("function.%s", j.FunctionName()))
	defer span.End()

	doc, err := getJSONDocumentFromRow(ctx, row, j.JSON)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return "NULL", nil
	}

	switch v := doc.Val.(type) {
	case nil:
		return "NULL", nil
	case bool:
		return "BOOLEAN", nil
	case float64:
		if conv, ok := j.JSON.(*expression.Convert); ok {
			typ := conv.Child.Type()
			if types.IsUnsigned(typ) || types.IsYear(typ) {
				return "UNSIGNED INTEGER", nil
			}
		}
		if math.Floor(v) == v {
			if v >= (math.MaxInt32+1)*2 {
				return "UNSIGNED INTEGER", nil
			}
			return "INTEGER", nil
		}
		return "DOUBLE", nil
	case string:
		if conv, ok := j.JSON.(*expression.Convert); ok {
			typ := conv.Child.Type()
			if types.IsDecimal(typ) {
				return "DECIMAL", nil
			}
			if types.IsDatetimeType(typ) {
				return "DATETIME", nil
			}
			if types.IsDateType(typ) {
				return "DATE", nil
			}
			if types.IsTime(typ) {
				return "TIME", nil
			}
		}
		return "STRING", nil
	case []interface{}:
		return "ARRAY", nil
	case map[string]interface{}:
		return "OBJECT", nil
	default:
		return "OPAQUE", nil
	}
}

// Children implements the Expression interface.
func (j JSONType) Children() []sql.Expression {
	return []sql.Expression{j.JSON}
}

// WithChildren implements the Expression interface.
func (j JSONType) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewJSONType(children...)
}
