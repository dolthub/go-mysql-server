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

package function

import (
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/internal/strings"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// JSONUnquote unquotes JSON value and returns the result as a utf8mb4 string.
// Returns NULL if the argument is NULL.
// An error occurs if the value starts and ends with double quotes but is not a valid JSON string literal.
type JSONUnquote struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*JSONUnquote)(nil)

// NewJSONUnquote creates a new JSONUnquote UDF.
func NewJSONUnquote(ctx *sql.Context, json sql.Expression) sql.Expression {
	return &JSONUnquote{expression.UnaryExpression{Child: json}}
}

// FunctionName implements sql.FunctionExpression
func (js *JSONUnquote) FunctionName() string {
	return "json_unquote"
}

func (js *JSONUnquote) String() string {
	return fmt.Sprintf("JSON_UNQUOTE(%s)", js.Child)
}

// Type implements the Expression interface.
func (*JSONUnquote) Type() sql.Type {
	return sql.LongText
}

// WithChildren implements the Expression interface.
func (js *JSONUnquote) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(js, len(children), 1)
	}
	return NewJSONUnquote(ctx, children[0]), nil
}

// Eval implements the Expression interface.
func (js *JSONUnquote) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	json, err := js.Child.Eval(ctx, row)
	if json == nil || err != nil {
		return json, err
	}

	ex, err := sql.LongText.Convert(json)
	if err != nil {
		return nil, err
	}
	str, ok := ex.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(ex).String())
	}

	return strings.Unquote(str)
}
