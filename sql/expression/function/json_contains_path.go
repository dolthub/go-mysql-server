// Copyright 2023 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

// JSON_CONTAINS_PATH(json_doc, one_or_all, path[, path] ...)
//
// JSONContainsPath Returns 0 or 1 to indicate whether a JSON document contains data at a given path or paths. Returns
// NULL if any argument is NULL. An error occurs if the json_doc argument is not a valid JSON document, any path
// argument is not a valid path expression, or one_or_all is not 'one' or 'all'. To check for a specific value at a
// path, use JSON_CONTAINS() instead.
//
// The return value is 0 if no specified path exists within the document. Otherwise, the return value depends on the
// one_or_all argument:
//   - 'one': 1 if at least one path exists within the document, 0 otherwise.
//   - 'all': 1 if all paths exist within the document, 0 otherwise.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-contains-path
type JSONContainsPath struct {
	sql.Expression
}

var _ sql.FunctionExpression = JSONContainsPath{}

// NewJSONContainsPath creates a new JSONContainsPath function.
func NewJSONContainsPath(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONContainsPath{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONContainsPath) FunctionName() string {
	return "json_contains_path"
}

// Description implements sql.FunctionExpression
func (j JSONContainsPath) Description() string {
	return "returns whether JSON document contains any data at path."
}

// IsUnsupported implements sql.UnsupportedFunctionStub
func (j JSONContainsPath) IsUnsupported() bool {
	return true
}
