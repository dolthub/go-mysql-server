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

var _ sql.FunctionExpression = (JSONArray)(nil)

// NewJSONArray creates a new JSONArray function.
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

// NewJSONObject creates a new JSONObject function.
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

// NewJSONQuote creates a new JSONQuote function.
func NewJSONQuote(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONQuote{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONQuote) FunctionName() string {
	return "json_quote"
}


// JSON_ARRAY_APPEND(json_doc, path, val[, path, val] ...)
//
// JSONArrayAppend Appends values to the end of the indicated arrays within a JSON document and returns the result.
// Returns NULL if any argument is NULL. An error occurs if the json_doc argument is not a valid JSON document or any
// path argument is not a valid path expression or contains a * or ** wildcard. The path-value pairs are evaluated left
// to right. The document produced by evaluating one pair becomes the new value against which the next pair is
// evaluated. If a path selects a scalar or object value, that value is autowrapped within an array and the new value is
// added to that array. Pairs for which the path does not identify any value in the JSON document are ignored.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-array-append
type JSONArrayAppend struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONArrayAppend)(nil)

// NewJSONArrayAppend creates a new JSONArrayAppend function.
func NewJSONArrayAppend(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONArrayAppend{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONArrayAppend) FunctionName() string {
	return "json_array_append"
}


// JSON_ARRAY_INSERT(json_doc, path, val[, path, val] ...)
//
// JSONArrayInsert Updates a JSON document, inserting into an array within the document and returning the modified
// document. Returns NULL if any argument is NULL. An error occurs if the json_doc argument is not a valid JSON document
// or any path argument is not a valid path expression or contains a * or ** wildcard or does not end with an array
// element identifier. The path-value pairs are evaluated left to right. The document produced by evaluating one pair
// becomes the new value against which the next pair is evaluated. Pairs for which the path does not identify any array
// in the JSON document are ignored. If a path identifies an array element, the corresponding value is inserted at that
// element position, shifting any following values to the right. If a path identifies an array position past the end of
// an array, the value is inserted at the end of the array.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-array-insert
type JSONArrayInsert struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONArrayInsert)(nil)

// NewJSONArrayInsert creates a new JSONArrayInsert function.
func NewJSONArrayInsert(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONArrayInsert{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONArrayInsert) FunctionName() string {
	return "json_array_insert"
}


// JSON_INSERT(json_doc, path, val[, path, val] ...)
//
// JSONInsert Inserts data into a JSON document and returns the result. Returns NULL if any argument is NULL. An error
// occurs if the json_doc argument is not a valid JSON document or any path argument is not a valid path expression or
// contains a * or ** wildcard. The path-value pairs are evaluated left to right. The document produced by evaluating
// one pair becomes the new value against which the next pair is evaluated. A path-value pair for an existing path in
// the document is ignored and does not overwrite the existing document value. A path-value pair for a nonexisting path
// in the document adds the value to the document if the path identifies one of these types of values:
//   - A member not present in an existing object. The member is added to the object and associated with the new value.
//   - A position past the end of an existing array. The array is extended with the new value. If the existing value is
//     not an array, it is autowrapped as an array, then extended with the new value.
// Otherwise, a path-value pair for a nonexisting path in the document is ignored and has no effect.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-insert
type JSONInsert struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONInsert)(nil)

// NewJSONInsert creates a new JSONInsert function.
func NewJSONInsert(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONInsert{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONInsert) FunctionName() string {
	return "json_insert"
}


// JSON_MERGE_PATCH(json_doc, json_doc[, json_doc] ...)
//
// JSONMergePatch Performs an RFC 7396 compliant merge of two or more JSON documents and returns the merged result,
// without preserving members having duplicate keys. Raises an error if at least one of the documents passed as arguments
// to this function is not valid. JSONMergePatch performs a merge as follows:
//   - If the first argument is not an object, the result of the merge is the same as if an empty object had been merged
//	   with the second argument.
//   - If the second argument is not an object, the result of the merge is the second argument.
//   - If both arguments are objects, the result of the merge is an object with the following members:
//     - All members of the first object which do not have a corresponding member with the same key in the second
//       object.
//     - All members of the second object which do not have a corresponding key in the first object, and whose value is
//       not the JSON null literal.
//     - All members with a key that exists in both the first and the second object, and whose value in the second
//       object is not the JSON null literal. The values of these members are the results of recursively merging the
//       value in the first object with the value in the second object.
//
// The behavior of JSONMergePatch is the same as that of JSONMergePreserve, with the following two exceptions:
//   - JSONMergePatch removes any member in the first object with a matching key in the second object, provided that
//     the value associated with the key in the second object is not JSON null.
//   - If the second object has a member with a key matching a member in the first object, JSONMergePatch replaces
//     the value in the first object with the value in the second object, whereas JSONMergePreserve appends the
//     second value to the first value.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-merge-patch
type JSONMergePatch struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONMergePatch)(nil)

// NewJSONMergePatch creates a new JSONMergePatch function.
func NewJSONMergePatch(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONMergePatch{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONMergePatch) FunctionName() string {
	return "json_merge_patch"
}


// JSON_MERGE(json_doc, json_doc[, json_doc] ...)
//
// JSONMerge Merges two or more JSON documents. Synonym for JSONMergePreserve(); deprecated in MySQL 8.0.3 and subject
// to removal in a future release.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-merge
type JSONMerge struct {
	sql.Expression
}


// JSON_MERGE_PRESERVE(json_doc, json_doc[, json_doc] ...)
//
// JSONMergePreserve Merges two or more JSON documents and returns the merged result. Returns NULL if any argument is
// NULL. An error occurs if any argument is not a valid JSON document. Merging takes place according to the following
// rules:
//   - Adjacent arrays are merged to a single array.
//   - Adjacent objects are merged to a single object.
//   - A scalar value is autowrapped as an array and merged as an array.
//   - An adjacent array and object are merged by autowrapping the object as an array and merging the two arrays.
//
// This function was added in MySQL 8.0.3 as a synonym for JSONMerge. The JSONMerge function is now deprecated,
// and is subject to removal in a future release of MySQL.
//
// The behavior of JSONMergePatch is the same as that of JSONMergePreserve, with the following two exceptions:
//   - JSONMergePatch removes any member in the first object with a matching key in the second object, provided that
//     the value associated with the key in the second object is not JSON null.
//   - If the second object has a member with a key matching a member in the first object, JSONMergePatch replaces
//     the value in the first object with the value in the second object, whereas JSONMergePreserve appends the
//     second value to the first value.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-merge-preserve
type JSONMergePreserve struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONMergePreserve)(nil)

// NewJSONMergePreserve creates a new JSONMergePreserve function.
func NewJSONMergePreserve(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONMergePreserve{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONMergePreserve) FunctionName() string {
	return "json_merge_preserve"
}


// JSON_REMOVE(json_doc, path[, path] ...)
//
// JSONRemove Removes data from a JSON document and returns the result. Returns NULL if any argument is NULL. An error
// occurs if the json_doc argument is not a valid JSON document or any path argument is not a valid path expression or
// is $ or contains a * or ** wildcard. The path arguments are evaluated left to right. The document produced by
// evaluating one path becomes the new value against which the next path is evaluated. It is not an error if the element
// to be removed does not exist in the document; in that case, the path does not affect the document.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-remove
type JSONRemove struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONRemove)(nil)

// NewJSONRemove creates a new JSONRemove function.
func NewJSONRemove(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONRemove{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONRemove) FunctionName() string {
	return "json_remove"
}


// JSON_REPLACE(json_doc, path, val[, path, val] ...)
//
// JSONReplace Replaces existing values in a JSON document and returns the result. Returns NULL if any argument is NULL.
// An error occurs if the json_doc argument is not a valid JSON document or any path argument is not a valid path
// expression or contains a * or ** wildcard. The path-value pairs are evaluated left to right. The document produced by
// evaluating one pair becomes the new value against which the next pair is evaluated. A path-value pair for an existing
// path in the document overwrites the existing document value with the new value. A path-value pair for a non-existing
// path in the document is ignored and has no effect.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-replace
type JSONReplace struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONReplace)(nil)

// NewJSONReplace creates a new JSONReplace function.
func NewJSONReplace(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONReplace{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONReplace) FunctionName() string {
	return "json_replace"
}


// JSON_SET(json_doc, path, val[, path, val] ...)
//
// JSONSet Inserts or updates data in a JSON document and returns the result. Returns NULL if any argument is NULL or
// path, if given, does not locate an object. An error occurs if the json_doc argument is not a valid JSON document or
// any path argument is not a valid path expression or contains a * or ** wildcard. The path-value pairs are evaluated
// left to right. The document produced by evaluating one pair becomes the new value against which the next pair is
// evaluated. A path-value pair for an existing path in the document overwrites the existing document value with the
// new value. A path-value pair for a non-existing path in the document adds the value to the document if the path
// identifies one of these types of values:
//   - A member not present in an existing object. The member is added to the object and associated with the new value.
//   - A position past the end of an existing array. The array is extended with the new value. If the existing value is
//     not an array, it is auto-wrapped as an array, then extended with the new value.
// Otherwise, a path-value pair for a non-existing path in the document is ignored and has no effect.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-set
type JSONSet struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONSet)(nil)

// NewJSONSet creates a new JSONSet function.
func NewJSONSet(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONSet{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONSet) FunctionName() string {
	return "json_set"
}


// JSON_DEPTH(json_doc)
//
// JSONDepth Returns the maximum depth of a JSON document. Returns NULL if the argument is NULL. An error occurs if the
// argument is not a valid JSON document. An empty array, empty object, or scalar value has depth 1. A nonempty array
// containing only elements of depth 1 or nonempty object containing only member values of depth 1 has depth 2.
// Otherwise, a JSON document has depth greater than 2.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-depth
type JSONDepth struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONDepth)(nil)

// NewJSONDepth creates a new JSONDepth function.
func NewJSONDepth(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONDepth{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONDepth) FunctionName() string {
	return "json_depth"
}


// JSON_LENGTH(json_doc[, path])
//
// JSONLength Returns the length of a JSON document, or, if a path argument is given, the length of the value within
// the document identified by the path. Returns NULL if any argument is NULL or the path argument does not identify a
// value in the document. An error occurs if the json_doc argument is not a valid JSON document or the path argument is
// not a valid path expression or contains a * or ** wildcard. The length of a document is determined as follows:
//   - The length of a scalar is 1.
//   - The length of an array is the number of array elements.
//   - The length of an object is the number of object members.
//   - The length does not count the length of nested arrays or objects.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-length
type JSONLength struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONLength)(nil)

// NewJSONLength creates a new JSONLength function.
func NewJSONLength(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONLength{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONLength) FunctionName() string {
	return "json_length"
}


// JSON_TYPE(json_val)
//
// Returns a utf8mb4 string indicating the type of a JSON value. This can be an object, an array, or a scalar type.
// JSONType returns NULL if the argument is NULL. An error occurs if the argument is not a valid JSON value
//
// https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-type
type JSONType struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONType)(nil)

// NewJSONType creates a new JSONType function.
func NewJSONType(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONType{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONType) FunctionName() string {
	return "json_type"
}


// JSON_VALID(val)
//
// Returns 0 or 1 to indicate whether a value is valid JSON. Returns NULL if the argument is NULL.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-valid
type JSONValid struct {
	sql.Expression
}

var _ sql.FunctionExpression = (JSONValid)(nil)

// NewJSONValid creates a new JSONValid function.
func NewJSONValid(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONValid{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONValid) FunctionName() string {
	return "json_valid"
}
