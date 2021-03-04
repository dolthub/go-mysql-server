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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

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
	sql.Expression
}

var _ sql.FunctionExpression = (JSONArrayAgg)(nil)

// NewJSONArrayAgg creates a new JSONArrayAgg function.
func NewJSONArrayAgg(args ...sql.Expression) (sql.Expression, error) {
	return nil, function.ErrUnsupportedJSONFunction.New(JSONArrayAgg{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONArrayAgg) FunctionName() string {
	return "json_arrayagg"
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
	sql.Expression
}

var _ sql.FunctionExpression = (JSONObjectAgg)(nil)

// NewJSONObjectAgg creates a new JSONArrayAgg function.
func NewJSONObjectAgg(args ...sql.Expression) (sql.Expression, error) {
	return nil, function.ErrUnsupportedJSONFunction.New(JSONObjectAgg{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONObjectAgg) FunctionName() string {
	return "json_objectagg"
}

