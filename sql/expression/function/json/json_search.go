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
					"github.com/dolthub/go-mysql-server/sql"
		)

// JSONSearch (json_doc, one_or_all, search_str[, escape_char[, path] ...])
//
// JSONSearch Returns the path to the given string within a JSON document. Returns NULL if any of the json_doc,
// search_str, or path arguments are NULL; no path exists within the document; or search_str is not found. An error
// occurs if the json_doc argument is not a valid JSON document, any path argument is not a valid path expression,
// one_or_all is not 'one' or 'all', or escape_char is not a constant expression.
// The one_or_all argument affects the search as follows:
//   - 'one': The search terminates after the first match and returns one path string. It is undefined which match is
//     considered first.
//   - 'all': The search returns all matching path strings such that no duplicate paths are included. If there are
//     multiple strings, they are autowrapped as an array. The order of the array elements is undefined.
//
// Within the search_str search string argument, the % and _ characters work as for the LIKE operator: % matches any
// number of characters (including zero characters), and _ matches exactly one character.
//
// To specify a literal % or _ character in the search string, precede it by the escape character. The default is \ if
// the escape_char argument is missing or NULL. Otherwise, escape_char must be a constant that is empty or one character.
// For more information about matching and escape character behavior, see the description of LIKE in Section 12.8.1,
// “String Comparison Functions and Operators”: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html
// For escape character handling, a difference from the LIKE behavior is that the escape character for JSON_SEARCH()
// must evaluate to a constant at compile time, not just at execution time. For example, if JSON_SEARCH() is used in a
// prepared statement and the escape_char argument is supplied using a ? parameter, the parameter value might be
// constant at execution time, but is not at compile time.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-search
type JSONSearch struct {
	JSON      sql.Expression
	OneOrAll  sql.Expression
	SearchStr sql.Expression
	Escape    sql.Expression
	Path      sql.Expression
}

var _ sql.FunctionExpression = &JSONSearch{}

// NewJSONSearch creates a new NewJSONSearch function.
func NewJSONSearch(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONSearch{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j *JSONSearch) FunctionName() string {
	return "json_search"
}

// Description implements sql.FunctionExpression
func (j *JSONSearch) Description() string {
	return "path to value within JSON document."
}

func (j *JSONSearch)Resolved() bool {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)String() string {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)Type() sql.Type {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)IsNullable() bool {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)Eval(ctx *sql.Context, row sql.Row) ( interface{},  error) {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)Children() []sql.Expression {
	//TODO implement me
panic("implement me")
}

func (j *JSONSearch)WithChildren(children ...sql.Expression) ( sql.Expression,  error) {
	//TODO implement me
panic("implement me")
}