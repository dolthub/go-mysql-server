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
	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)


// ErrUnsupportedJSONFunction is returned when a unsupported JSON function is called.
var ErrUnsupportedJSONFunction = errors.NewKind("unsupported JSON function: %s")


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

var _ sql.FunctionExpression = (JSONContainsPath)(nil)

// NewJSONContainsPath creates a new JSONContainsPath function.
func NewJSONContainsPath(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONContainsPath{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONContainsPath) FunctionName() string {
	return "json_contains_path"
}


// JSON_KEYS(json_doc[, path])
//
// JSONKeys Returns the keys from the top-level value of a JSON object as a JSON array, or, if a path argument is given,
// the top-level keys from the selected path. Returns NULL if any argument is NULL, the json_doc argument is not an
// object, or path, if given, does not locate an object. An error occurs if the json_doc argument is not a valid JSON
// document or the path argument is not a valid path expression or contains a * or ** wildcard. The result array is
// empty if the selected object is empty. If the top-level value has nested subobjects, the return value does not
// include keys from those subobjects.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-keys
type JSONKeys struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONKeys)(nil)

// NewJSONKeys creates a new JSONKeys function.
func NewJSONKeys(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONKeys{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONKeys) FunctionName() string {
	return "json_keys"
}


// JSON_OVERLAPS(json_doc1, json_doc2)
//
// JSONOverlaps Compares two JSON documents. Returns true (1) if the two document have any key-value pairs or array
// elements in common. If both arguments are scalars, the function performs a simple equality test.
//
// This function serves as counterpart to JSON_CONTAINS(), which requires all elements of the array searched for to be
// present in the array searched in. Thus, JSON_CONTAINS() performs an AND operation on search keys, while
// JSON_OVERLAPS() performs an OR operation.
//
// Queries on JSON columns of InnoDB tables using JSON_OVERLAPS() in the WHERE clause can be optimized using
// multi-valued indexes. Multi-Valued Indexes, provides detailed information and examples.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-overlaps
type JSONOverlaps struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONOverlaps)(nil)

// NewJSONOverlaps creates a new JSONOverlaps function.
func NewJSONOverlaps(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONOverlaps{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONOverlaps) FunctionName() string {
	return "json_overlaps"
}


// JSON_SEARCH(json_doc, one_or_all, search_str[, escape_char[, path] ...])
//
// JSONSearch Returns the path to the given string within a JSON document. Returns NULL if any of the json_doc,
// search_str, or path arguments are NULL; no path exists within the document; or search_str is not found. An error
// occurs if the json_doc argument is not a valid JSON document, any path argument is not a valid path expression,
// one_or_all is not 'one' or 'all', or escape_char is not a constant expression.
//
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
	sql.Expression
}

var _ sql.FunctionExpression = (JSONSearch)(nil)

// NewJSONSearch creates a new NewJSONSearch function.
func NewJSONSearch(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONSearch{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONSearch) FunctionName() string {
	return "json_search"
}

// JSON_VALUE(json_doc, path)
//
// JSONValue Extracts a value from a JSON document at the path given in the specified document, and returns the
// extracted value, optionally converting it to a desired type.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-value
type JSONValue struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONValue)(nil)

// NewJSONValue creates a new JSONValue function.
func NewJSONValue(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONValue{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONValue) FunctionName() string {
	return "json_value"
}


// value MEMBER OF(json_array)
//
// Returns true (1) if value is an element of json_array, otherwise returns false (0). value must be a scalar or a JSON
// document; if it is a scalar, the operator attempts to treat it as an element of a JSON array. Queries using
// MEMBER OF() on JSON columns of InnoDB tables in the WHERE clause can be optimized using multi-valued indexes. See
// Multi-Valued Indexes, for detailed information and examples.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#operator_member-of
// TODO(andy): relocate


// JSON_ARRAY([val[, val] ...])
//
// JSONArray Evaluates a (possibly empty) list of values and returns a JSON array containing those values.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-array
type JSONArray struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONOverlaps)(nil)

// NewJSONArray creates a new JSONValue function.
func NewJSONArray(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONArray{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONArray) FunctionName() string {
	return "json_array"
}


// JSON_OBJECT([key, val[, key, val] ...])
//
// JSONObject Evaluates a (possibly empty) list of key-value pairs and returns a JSON object containing those pairs. An
// error occurs if any key name is NULL or the number of arguments is odd.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-object
type JSONObject struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONObject)(nil)

// NewJSONObject creates a new JSONValue function.
func NewJSONObject(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONObject{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONObject) FunctionName() string {
	return "json_object"
}


// JSON_QUOTE(string)
//
// JSONQuote Quotes a string as a JSON value by wrapping it with double quote characters and escaping interior quote and
// other characters, then returning the result as a utf8mb4 string. Returns NULL if the argument is NULL. This function
// is typically used to produce a valid JSON string literal for inclusion within a JSON document. Certain special
// characters are escaped with backslashes per the escape sequences shown in Table 12.23, “JSON_UNQUOTE() Special
// Character Escape Sequences”:
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#json-unquote-character-escape-sequences
//
// https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-quote
type JSONQuote struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONQuote)(nil)

// NewJSONQuote creates a new JSONValue function.
func NewJSONQuote(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONQuote{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONQuote) FunctionName() string {
	return "json_quote"
}
