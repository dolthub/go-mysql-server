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

package aggregation

import (
	"fmt"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ErrUnsupportedJSONFunction is returned when a unsupported JSON function is called.
var ErrUnsupportedJSONFunction = errors.NewKind("unsupported JSON function: %s")

// JSON_ARRAYAGG(col_or_expr) [over_clause]
//
// JSONArrayAgg Aggregates a result set as a single JSON array whose elements consist of the rows. The order of elements
// in this array is undefined. The function acts on a column or an expression that evaluates to a single value. Returns
// NULL if the result contains no rows, or in the event of an error.
//
// https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_json-arrayagg
//
// see also: https://dev.mysql.com/doc/refman/8.0/en/json.html#json-normalization
type JSONArrayAgg struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = &JSONArrayAgg{}

// NewJSONArrayAgg creates a new JSONArrayAgg function.
func NewJSONArrayAgg(ctx *sql.Context, arg sql.Expression) *JSONArrayAgg {
	return &JSONArrayAgg{expression.UnaryExpression{Child: arg}}
}

// FunctionName implements sql.FunctionExpression
func (j *JSONArrayAgg) FunctionName() string {
	return "json_arrayagg"
}

// NewBuffer creates a new buffer for the aggregation.
func (j *JSONArrayAgg) NewBuffer() sql.Row {
	var row []interface{}
	return sql.NewRow(row)
}

// Type returns the type of the result.
func (j *JSONArrayAgg) Type() sql.Type {
	return sql.JSON
}

// IsNullable returns whether the return value can be null.
func (j *JSONArrayAgg) IsNullable() bool {
	return true
}

// Resolved implements the Expression interface.
func (j *JSONArrayAgg) Resolved() bool {
	if _, ok := j.Child.(*expression.Star); ok {
		return true
	}

	return j.Child.Resolved()
}

func (j *JSONArrayAgg) String() string {
	return fmt.Sprintf("JSON_ARRAYAGG(%s)", j.Child)
}

// WithChildren implements the Expression interface.
func (j *JSONArrayAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 1)
	}
	return NewJSONArrayAgg(ctx, children[0]), nil
}

// Update implements the Aggregation interface.
func (j *JSONArrayAgg) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := j.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	// unwrap JSON values
	if js, ok := v.(sql.JSONValue); ok {
		doc, err := js.Unmarshall(ctx)
		if err != nil {
			return err
		}
		v = doc.Val
	}

	buffer[0] = append(buffer[0].([]interface{}), v)

	return nil
}

// Merge implements the Aggregation interface.
func (j *JSONArrayAgg) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	arr1 := buffer[0].([]interface{})
	arr2 := partial[0].([]interface{})

	buffer[0] = append(arr1, arr2...)

	return nil
}

// Eval implements the Aggregation interface.
func (j *JSONArrayAgg) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	return sql.JSONDocument{Val: buffer[0]}, nil
}

// JSON_OBJECTAGG(key, value) [over_clause]
//
// JSONObjectAgg Takes two column names or expressions as arguments, the first of these being used as a key and the
// second as a value, and returns a JSON object containing key-value pairs. Returns NULL if the result contains no rows,
// or in the event of an error. An error occurs if any key name is NULL or the number of arguments is not equal to 2.
//
// https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_json-objectagg
//
// see also: https://dev.mysql.com/doc/refman/8.0/en/json.html#json-normalization
type JSONObjectAgg struct {
	key   sql.Expression
	value sql.Expression
}

var _ sql.FunctionExpression = JSONObjectAgg{}

// NewJSONObjectAgg creates a new JSONArrayAgg function.
func NewJSONObjectAgg(ctx *sql.Context, key, value sql.Expression) sql.Expression {
	return JSONObjectAgg{key: key, value: value}
}

// FunctionName implements sql.FunctionExpression
func (j JSONObjectAgg) FunctionName() string {
	return "json_objectagg"
}

func (j JSONObjectAgg) Resolved() bool {
	return j.key.Resolved() && j.value.Resolved()
}

func (j JSONObjectAgg) String() string {
	return fmt.Sprintf("JSON_OBJECTAGG(%s, %s)", j.key, j.value)
}

func (j JSONObjectAgg) Type() sql.Type {
	return sql.JSON
}

func (j JSONObjectAgg) IsNullable() bool {
	return false
}

func (j JSONObjectAgg) Children() []sql.Expression {
	return []sql.Expression{j.key, j.value}
}

func (j JSONObjectAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}

	return NewJSONObjectAgg(ctx, children[0], children[1]), nil
}

// NewBuffer implements the Aggregation interface.
func (j JSONObjectAgg) NewBuffer() sql.Row {
	row := make(map[string]interface{})
	return sql.NewRow(row)
}

// Update implements the Aggregation interface.
func (j JSONObjectAgg) Update(ctx *sql.Context, buffer, row sql.Row) error {
	key, err := j.key.Eval(ctx, row)
	if err != nil {
		return err
	}

	// An error occurs if any key name is NULL
	if key == nil {
		return sql.ErrJSONObjectAggNullKey.New()
	}

	val, err := j.value.Eval(ctx, row)
	if err != nil {
		return err
	}

	// unwrap JSON values
	if js, ok := val.(sql.JSONValue); ok {
		doc, err := js.Unmarshall(ctx)
		if err != nil {
			return err
		}
		val = doc.Val
	}

	// Update the map.
	mp := buffer[0].(map[string]interface{})

	keyAsString, err := sql.LongText.Convert(key)
	if err != nil {
		return nil
	}
	mp[keyAsString.(string)] = val

	return nil
}

// Merge implements the Aggregation interface.
func (j JSONObjectAgg) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return j.Update(ctx, buffer, partial)
}

// Eval implements the Aggregation interface.
func (j JSONObjectAgg) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	mp := buffer[0].(map[string]interface{})

	// When no rows are present return NULL
	if len(mp) == 0 {
		return nil, nil
	}

	return sql.JSONDocument{Val: mp}, nil
}
