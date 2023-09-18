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

package types

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dolthub/jsonpath"

	"github.com/dolthub/go-mysql-server/sql"
)

// JSONValue is an integrator specific implementation of a JSON field value.
type JSONValue interface {
	// Unmarshall converts a JSONValue to a JSONDocument
	Unmarshall(ctx *sql.Context) (val JSONDocument, err error)
	// Compare compares two JSONValues. It maintains the same return value
	// semantics as Type.Compare()
	Compare(ctx *sql.Context, v JSONValue) (cmp int, err error)
	// ToString marshalls a JSONValue to a valid JSON-encoded string.
	ToString(ctx *sql.Context) (string, error)
}

// SearchableJSONValue is JSONValue supporting in-place access operations.
// The query engine can utilize these optimized access methods improve performance
// by minimizing the need to unmarshall a JSONValue into a JSONDocument.
type SearchableJSONValue interface {
	JSONValue

	// Contains is value-specific implementation of JSON_Contains()
	Contains(ctx *sql.Context, candidate JSONValue) (val interface{}, err error)
	// Extract is value-specific implementation of JSON_Extract()
	Extract(ctx *sql.Context, path string) (val JSONValue, err error)
	// Keys is value-specific implementation of JSON_Keys()
	Keys(ctx *sql.Context, path string) (val JSONValue, err error)
	// Overlaps is value-specific implementation of JSON_Overlaps()
	Overlaps(ctx *sql.Context, val SearchableJSONValue) (ok bool, err error)
	// Search is value-specific implementation of JSON_Search()
	Search(ctx *sql.Context) (path string, err error)
}

type MutableJSONValue interface {
	// Insert Adds the value at the given path, only if it is not present. Updated value returned, and bool indicating if
	// a change was made.
	Insert(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error)
	// Remove the value at the given path. Updated value returned, and bool indicating if a change was made.
	Remove(ctx *sql.Context, path string) (MutableJSONValue, bool, error)
	// Set the value at the given path. Updated value returned, and bool indicating if a change was made.
	Set(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error)
	// Replace the value at the given path with the new value. If the path does not exist, no modification is made.
	Replace(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error)
	// ArrayInsert inserts into the array object referenced by the given path. If the path does not exist, no modification is made.
	ArrayInsert(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error)
	// ArrayAppend appends to an  array object referenced by the given path. If the path does not exist, no modification is made,
	// or if the path exists and is not an array, the element will be converted into an array and the element will be
	// appended to it.
	ArrayAppend(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error)
}

type JSONDocument struct {
	Val interface{}
}

var _ JSONValue = JSONDocument{}

func (doc JSONDocument) Unmarshall(_ *sql.Context) (JSONDocument, error) {
	return doc, nil
}

func (doc JSONDocument) Compare(ctx *sql.Context, v JSONValue) (int, error) {
	other, err := v.Unmarshall(ctx)
	if err != nil {
		return 0, err
	}
	return compareJSON(doc.Val, other.Val)
}

func (doc JSONDocument) ToString(_ *sql.Context) (string, error) {
	return marshalToMySqlString(doc.Val)
}

var _ SearchableJSONValue = JSONDocument{}
var _ MutableJSONValue = JSONDocument{}

// Contains returns nil in case of a nil value for either the doc.Val or candidate. Otherwise
// it returns a bool
func (doc JSONDocument) Contains(ctx *sql.Context, candidate JSONValue) (val interface{}, err error) {
	other, err := candidate.Unmarshall(ctx)
	if err != nil {
		return false, err
	}
	return containsJSON(doc.Val, other.Val)
}

func (doc JSONDocument) Extract(ctx *sql.Context, path string) (JSONValue, error) {
	if path == "$" {
		// Special case the identity operation to handle a nil value for doc.Val
		return doc, nil
	}

	c, err := jsonpath.Compile(path)
	if err != nil {
		// Until we throw out jsonpath, let's at least make this error better.
		if err.Error() == "should start with '$'" {
			err = fmt.Errorf("Invalid JSON path expression. Path must start with '$', but received: '%s'", path)
		}
		return nil, err
	}

	// Lookup(obj) throws an error if obj is nil. We want lookups on a json null
	// to always result in sql NULL, except in the case of the identity lookup
	// $.
	r := doc.Val
	if r == nil {
		return nil, nil
	}

	val, err := c.Lookup(r)
	if err != nil {
		if strings.Contains(err.Error(), "key error") {
			// A missing key results in a SQL null
			return nil, nil
		}
		return nil, err
	}

	return JSONDocument{Val: val}, nil
}

func (doc JSONDocument) Keys(ctx *sql.Context, path string) (val JSONValue, err error) {
	panic("not implemented")
}

func (doc JSONDocument) Overlaps(ctx *sql.Context, val SearchableJSONValue) (ok bool, err error) {
	panic("not implemented")
}

func (doc JSONDocument) Search(ctx *sql.Context) (path string, err error) {
	panic("not implemented")
}

var _ driver.Valuer = JSONDocument{}

// Value implements driver.Valuer for interoperability with other go libraries
func (doc JSONDocument) Value() (driver.Value, error) {
	if doc.Val == nil {
		return nil, nil
	}

	mysqlString, err := marshalToMySqlString(doc.Val)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	return mysqlString, nil
}

func ConcatenateJSONValues(ctx *sql.Context, vals ...JSONValue) (JSONValue, error) {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		d, err := v.Unmarshall(ctx)
		if err != nil {
			return nil, err
		}
		arr[i] = d.Val
	}
	return JSONDocument{Val: arr}, nil
}

func containsJSON(a, b interface{}) (interface{}, error) {
	if a == nil || b == nil {
		return nil, nil
	}

	switch a := a.(type) {
	case []interface{}:
		return containsJSONArray(a, b)
	case map[string]interface{}:
		return containsJSONObject(a, b)
	case bool:
		return containsJSONBool(a, b)
	case string:
		return containsJSONString(a, b)
	case float64:
		return containsJSONNumber(a, b)
	default:
		return false, sql.ErrInvalidType.New(a)
	}
}

func containsJSONBool(a bool, b interface{}) (bool, error) {
	switch b := b.(type) {
	case bool:
		return a == b, nil
	default:
		return false, nil
	}
}

// containsJSONArray returns true if b is contained in the JSON array a. From the official
// MySQL docs: "A candidate array is contained in a target array if and only if every
// element in the candidate is contained in *some* element of the target. A candidate
// non-array is contained in a target array if and only if the candidate is contained
// in some element of the target."
//
// Examples:
//
//	select json_contains('[1, [1, 2, 3], 10]', '[1, 10]'); => true
//	select json_contains('[1, [1, 2, 3, 10]]', '[1, 10]'); => true
//	select json_contains('[1, [1, 2, 3], [10]]', '[1, [10]]'); => true
func containsJSONArray(a []interface{}, b interface{}) (bool, error) {
	if _, ok := b.([]interface{}); ok {
		for _, bb := range b.([]interface{}) {
			contains, err := containsJSONArray(a, bb)
			if err != nil {
				return false, err
			}
			if contains == false {
				return false, nil
			}
		}
		return true, nil
	} else {
		// A candidate non-array is contained in a target array if and only if the candidate is contained in some element of the target.
		for _, aa := range a {
			contains, err := containsJSON(aa, b)
			if err != nil {
				return false, err
			}
			if contains == true {
				return true, nil
			}
		}
	}

	return false, nil
}

// containsJSONObject returns true if b is contained in the JSON object a. From the
// official MySQL docs: "A candidate object is contained in a target object if and only
// if for each key in the candidate there is a key with the same name in the target and
// the value associated with the candidate key is contained in the value associated with
// the target key."
//
// Examples:
//
//	select json_contains('{"b": {"a": [1, 2, 3]}}', '{"a": [1]}'); => false
//	select json_contains('{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}', '{"a": [1]}'); => true
//	select json_contains('{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}', '{"a": []}'); => true
//	select json_contains('{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}', '{"a": {}}'); => false
//	select json_contains('{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}', '{"a": [2, 4]}'); => true
//	select json_contains('{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}', '[2]'); => false
//	select json_contains('{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}', '2'); => false
func containsJSONObject(a map[string]interface{}, b interface{}) (bool, error) {
	_, isMap := b.(map[string]interface{})
	if !isMap {
		// If b is a scalar or an array, json_contains always returns false when
		// testing containment in a JSON object
		return false, nil
	}

	for key, bvalue := range b.(map[string]interface{}) {
		avalue, ok := a[key]
		if !ok {
			return false, nil
		}

		contains, err := containsJSON(avalue, bvalue)
		if err != nil {
			return false, err
		}
		if contains == false {
			return false, nil
		}
	}
	return true, nil
}

func containsJSONString(a string, b interface{}) (bool, error) {
	switch b := b.(type) {
	case string:
		return a == b, nil
	default:
		return false, nil
	}
}

func containsJSONNumber(a float64, b interface{}) (bool, error) {
	switch b := b.(type) {
	case float64:
		return a == b, nil
	default:
		return false, nil
	}
}

// JSON values can be compared using the =, <, <=, >, >=, <>, !=, and <=> operators. BETWEEN IN() GREATEST() LEAST() are
// not yet supported with JSON values.
//
// For comparison of JSON and non-JSON values, the non-JSON value is first converted to JSON (see JsonType.Convert()).
// Comparison of JSON values takes place at two levels. The first level of comparison is based on the JSON types of the
// compared values. If the types differ, the comparison result is determined solely by which type has higher precedence.
// If the two values have the same JSON type, a second level of comparison occurs using type-specific rules. The
// following list shows the precedences of JSON types, from highest precedence to the lowest. (The type names are those
// returned by the JSON_TYPE() function.) Types shown together on a line have the same precedence. Any value having a
// JSON type listed earlier in the list compares greater than any value having a JSON type listed later in the list.
//
//			BLOB, BIT, OPAQUE, DATETIME, TIME, DATE, BOOLEAN, ARRAY, OBJECT, STRING, INTEGER, DOUBLE, NULL
//			TODO(andy): implement BLOB BIT OPAQUE DATETIME TIME DATE
//	     current precedence: BOOLEAN, ARRAY, OBJECT, STRING, DOUBLE, NULL
//
// For JSON values of the same precedence, the comparison rules are type specific:
//
//   - ARRAY
//     Two JSON arrays are equal if they have the same length and values in corresponding positions in the arrays are
//     equal. If the arrays are not equal, their order is determined by the elements in the first position where there
//     is a difference. The array with the smaller value in that position is ordered first. If all values of the
//     shorter array are equal to the corresponding values in the longer array, the shorter array is ordered first.
//     e.g.    [] < ["a"] < ["ab"] < ["ab", "cd", "ef"] < ["ab", "ef"]
//
//   - BOOLEAN
//     The JSON false literal is less than the JSON true literal.
//
//   - OBJECT
//     Two JSON objects are equal if they have the same set of keys, and each key has the same value in both objects.
//     The order of two objects that are not equal is unspecified but deterministic.
//     e.g.   {"a": 1, "b": 2} = {"b": 2, "a": 1}
//
//   - STRING
//     Strings are ordered lexically on the first N bytes of the utf8mb4 representation of the two strings being
//     compared, where N is the length of the shorter string. If the first N bytes of the two strings are identical,
//     the shorter string is considered smaller than the longer string.
//     e.g.   "a" < "ab" < "b" < "bc"
//     This ordering is equivalent to the ordering of SQL strings with collation utf8mb4_bin. Because utf8mb4_bin is a
//     binary collation, comparison of JSON values is case-sensitive:
//     e.g.   "A" < "a"
//
//   - DOUBLE
//     JSON values can contain exact-value numbers and approximate-value numbers. For a general discussion of these
//     types of numbers, see Section 9.1.2, “Numeric Literals”. The rules for comparing native MySQL numeric types are
//     discussed in Section 12.3, “Type Conversion in Expression Evaluation”, but the rules for comparing numbers
//     within JSON values differ somewhat:
//
//   - In a comparison between two columns that use the native MySQL INT and DOUBLE numeric types, respectively,
//     it is known that all comparisons involve an integer and a double, so the integer is converted to double for
//     all rows. That is, exact-value numbers are converted to approximate-value numbers.
//
//   - On the other hand, if the query compares two JSON columns containing numbers, it cannot be known in advance
//     whether numbers are integer or double. To provide the most consistent behavior across all rows, MySQL
//     converts approximate-value numbers to exact-value numbers. The resulting ordering is consistent and does
//     not lose precision for the exact-value numbers.
//     e.g.   9223372036854775805 < 9223372036854775806 < 9223372036854775807 < 9.223372036854776e18
//     = 9223372036854776000 < 9223372036854776001
//
//   - NULL
//     For comparison of any JSON value to SQL NULL, the result is UNKNOWN.
//
//     TODO(andy): BLOB, BIT, OPAQUE, DATETIME, TIME, DATE, INTEGER
//
// https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison
func compareJSON(a, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(b, a); hasNulls {
		return res, nil
	}

	switch a := a.(type) {
	case bool:
		return compareJSONBool(a, b)
	case []interface{}:
		return compareJSONArray(a, b)
	case map[string]interface{}:
		return compareJSONObject(a, b)
	case string:
		return compareJSONString(a, b)
	case float64:
		return compareJSONNumber(a, b)
	default:
		return 0, sql.ErrInvalidType.New(a)
	}
}

func compareJSONBool(a bool, b interface{}) (int, error) {
	switch b := b.(type) {
	case bool:
		// The JSON false literal is less than the JSON true literal.
		if a == b {
			return 0, nil
		}
		if a {
			// a > b
			return 1, nil
		} else {
			// a < b
			return -1, nil
		}

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONArray(a []interface{}, b interface{}) (int, error) {
	switch b := b.(type) {
	case bool:
		// a is lower precedence
		return -1, nil

	case []interface{}:
		// Two JSON arrays are equal if they have the same length and values in corresponding positions in the arrays
		// are equal. If the arrays are not equal, their order is determined by the elements in the first position
		// where there is a difference. The array with the smaller value in that position is ordered first.
		for i, aa := range a {
			// If all values of the shorter array are equal to the corresponding values in the longer array,
			// the shorter array is ordered first (is less).
			if i >= len(b) {
				return 1, nil
			}

			cmp, err := compareJSON(aa, b[i])
			if err != nil {
				return 0, err
			}
			if cmp != 0 {
				return cmp, nil
			}
		}
		if len(a) < len(b) {
			return -1, nil
		} else {
			return 0, nil
		}

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONObject(a map[string]interface{}, b interface{}) (int, error) {
	switch b := b.(type) {
	case
		bool,
		[]interface{}:
		// a is lower precedence
		return -1, nil

	case map[string]interface{}:
		// Two JSON objects are equal if they have the same set of keys, and each key has the same value in both
		// objects. The order of two objects that are not equal is unspecified but deterministic.
		inter := jsonObjectKeyIntersection(a, b)
		for _, key := range inter {
			cmp, err := compareJSON(a[key], b[key])
			if err != nil {
				return 0, err
			}
			if cmp != 0 {
				return cmp, nil
			}
		}
		if len(a) == len(b) && len(a) == len(inter) {
			return 0, nil
		}
		return jsonObjectDeterministicOrder(a, b, inter)

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONString(a string, b interface{}) (int, error) {
	switch b := b.(type) {
	case
		bool,
		[]interface{},
		map[string]interface{}:
		// a is lower precedence
		return -1, nil

	case string:
		return strings.Compare(a, b), nil

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONNumber(a float64, b interface{}) (int, error) {
	switch b := b.(type) {
	case
		bool,
		[]interface{},
		map[string]interface{},
		string:
		// a is lower precedence
		return -1, nil

	case float64:
		if a > b {
			return 1, nil
		}
		if a < b {
			return -1, nil
		}
		return 0, nil

	default:
		// a is higher precedence
		return 1, nil
	}
}

func jsonObjectKeyIntersection(a, b map[string]interface{}) (ks []string) {
	for key := range a {
		if _, ok := b[key]; ok {
			ks = append(ks, key)
		}
	}
	sort.Strings(ks)
	return
}

func jsonObjectDeterministicOrder(a, b map[string]interface{}, inter []string) (int, error) {
	if len(a) > len(b) {
		return 1, nil
	}
	if len(a) < len(b) {
		return -1, nil
	}

	// if equal length, compare least non-intersection key
	iset := make(map[string]bool)
	for _, key := range inter {
		iset[key] = true
	}

	var aa string
	for key := range a {
		if _, ok := iset[key]; !ok {
			if key < aa || aa == "" {
				aa = key
			}
		}
	}

	var bb string
	for key := range b {
		if _, ok := iset[key]; !ok {
			if key < bb || bb == "" {
				bb = key
			}
		}
	}

	return strings.Compare(aa, bb), nil
}

func (doc JSONDocument) Insert(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)
	return doc.unwrapAndExecute(ctx, path, val, INSERT)
}

func (doc JSONDocument) Remove(ctx *sql.Context, path string) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)
	if path == "$" {
		return nil, false, fmt.Errorf("The path expression '$' is not allowed in this context.")
	}

	return doc.unwrapAndExecute(ctx, path, nil, REMOVE)
}

func (doc JSONDocument) Set(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)
	return doc.unwrapAndExecute(ctx, path, val, SET)
}

func (doc JSONDocument) Replace(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)
	return doc.unwrapAndExecute(ctx, path, val, REPLACE)
}

func (doc JSONDocument) ArrayAppend(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)
	return doc.unwrapAndExecute(ctx, path, val, ARRAY_APPEND)
}

func (doc JSONDocument) ArrayInsert(ctx *sql.Context, path string, val JSONValue) (MutableJSONValue, bool, error) {
	path = strings.TrimSpace(path)

	if path == "$" {
		// json_array_insert is the only function that produces an error for the '$' path no matter what the value is.
		return nil, false, fmt.Errorf("Path expression is not a path to a cell in an array: $")
	}

	return doc.unwrapAndExecute(ctx, path, val, ARRAY_INSERT)
}

const (
	SET = iota
	INSERT
	REPLACE
	REMOVE
	ARRAY_APPEND
	ARRAY_INSERT
)

// unwrapAndExecute unwraps the JSONDocument and executes the given path on the unwrapped value. The path string passed
// in at this point should be unmodified.
func (doc JSONDocument) unwrapAndExecute(ctx *sql.Context, path string, val JSONValue, mode int) (MutableJSONValue, bool, error) {
	if path == "" {
		return nil, false, fmt.Errorf("Invalid JSON path expression. Empty path")
	}

	var err error
	var unmarshalled JSONDocument
	if val != nil {
		unmarshalled, err = val.Unmarshall(ctx)
		if err != nil {
			return nil, false, err
		}
	} else if mode != REMOVE {
		return nil, false, fmt.Errorf("Invariant violation. value may not be nil")
	}

	if path[0] != '$' {
		return nil, false, fmt.Errorf("Invalid JSON path expression. Path must start with '$'")
	}

	path = path[1:]
	// Cursor is used to track how many characters have been parsed in the path. It is used to enable better error messages,
	// and is passed as a pointer because some function parse a variable number of characters.
	cursor := 1

	resultRaw, changed, parseErr := walkPathAndUpdate(path, doc.Val, unmarshalled.Val, mode, &cursor)
	if parseErr != nil {
		err = fmt.Errorf("%s at character %d of $%s", parseErr.msg, parseErr.character, path)
		return nil, false, err
	}
	return JSONDocument{Val: resultRaw}, changed, nil
}

// parseErr is used to track errors that occur during parsing of the path, specifically to track the index of the character
// where we believe there is a problem.
type parseErr struct {
	msg       string
	character int
}

// walkPathAndUpdate walks the path and updates the document.
// JSONPath Spec (as documented) https://dev.mysql.com/doc/refman/8.0/en/json.html#json-path-syntax
//
// This function recursively consumes the path until it reaches the end, at which point it applies the mutation operation.
//
// Currently, our implementation focuses specifically on the mutation operations, so '*','**', and range index paths are
// not supported.
func walkPathAndUpdate(path string, doc interface{}, val interface{}, mode int, cursor *int) (interface{}, bool, *parseErr) {
	if path == "" {
		// End of Path is kind of a special snowflake for each type and mode.
		switch mode {
		case SET, REPLACE:
			return val, true, nil
		case INSERT:
			return doc, false, nil
		case ARRAY_APPEND:
			if arr, ok := doc.([]interface{}); ok {
				doc = append(arr, val)
				return doc, true, nil
			} else {
				// Otherwise, turn it into an array and append to it, and append to it.
				doc = []interface{}{doc, val}
				return doc, true, nil
			}
		case ARRAY_INSERT, REMOVE:
			// Some mutations should never reach the end of the path.
			return nil, false, &parseErr{msg: "Runtime error when processing json path", character: *cursor}
		default:
			return nil, false, &parseErr{msg: "Invalid JSON path expression. End of path reached", character: *cursor}
		}
	}

	if path[0] == '.' {
		path = path[1:]
		*cursor = *cursor + 1
		strMap, ok := doc.(map[string]interface{})
		if !ok {
			// json_array_insert is the only function that produces an error when the path is to an object which
			// lookup fails in this way. All other functions return the document unchanged. Go figure.
			if mode == ARRAY_INSERT {
				return nil, false, &parseErr{msg: "A path expression is not a path to a cell in an array", character: *cursor}
			}
			// not a map, can't do anything. NoOp
			return doc, false, nil
		}
		return updateObject(path, strMap, val, mode, cursor)
	} else if path[0] == '[' {
		*cursor = *cursor + 1
		right := strings.Index(path, "]")
		if right == -1 {
			return nil, false, &parseErr{msg: "Invalid JSON path expression. Missing ']'", character: *cursor}
		}

		remaining := path[right+1:]
		indexString := path[1:right]

		if arr, ok := doc.([]interface{}); ok {
			return updateArray(indexString, remaining, arr, val, mode, cursor)
		} else {
			return updateObjectTreatAsArray(indexString, doc, val, mode, cursor)
		}
	} else {
		return nil, false, &parseErr{msg: "Invalid JSON path expression. Expected '.' or '['", character: *cursor}
	}
}

// updateObject Take a map[string]interface{} and update the value at the given path. If we are not at the end of the path,
// the object is looked up and the walkPathAndUpdate function is called recursively.
func updateObject(path string, doc map[string]interface{}, val interface{}, mode int, cursor *int) (interface{}, bool, *parseErr) {
	name, remainingPath, err := parseNameAfterDot(path, cursor)
	if err != nil {
		return nil, false, err
	}

	if remainingPath == "" {
		if mode == ARRAY_APPEND {
			newDoc, ok := doc[name]
			if !ok {
				// end of the path with a nil value - no-op
				return doc, false, nil
			}
			newObj, changed, err := walkPathAndUpdate(remainingPath, newDoc, val, mode, cursor)
			if err != nil {
				return nil, false, err
			}
			if changed {
				doc[name] = newObj
			}
			return doc, changed, nil
		}

		// Found an item, and it must be an array in one case only.
		if mode == ARRAY_INSERT {
			return nil, false, &parseErr{msg: "A path expression is not a path to a cell in an array", character: *cursor}
		}

		// does the name exist in the map?
		updated := false
		_, destructive := doc[name]
		if mode == SET ||
			(!destructive && mode == INSERT) ||
			(destructive && mode == REPLACE) {
			doc[name] = val
			updated = true
		} else if destructive && mode == REMOVE {
			delete(doc, name)
			updated = true
		}
		return doc, updated, nil
	} else {
		// go deeper.
		newObj, changed, err := walkPathAndUpdate(remainingPath, doc[name], val, mode, cursor)
		if err != nil {
			return nil, false, err
		}
		if changed {
			doc[name] = newObj
			return doc, true, nil
		}
		return doc, false, nil
	}
}

// compiled regex used to parse the name of a field after a '.' in a JSON path.
var regex = regexp.MustCompile(`^(\w+)(.*)$`)

// parseNameAfterDot parses the json path immediately after a '.'. It returns the name of the field and the remaining path,
// and modifies the cursor to point to the end of the parsed path.
func parseNameAfterDot(path string, cursor *int) (name string, remainingPath string, err *parseErr) {
	if path == "" {
		return "", "", &parseErr{msg: "Invalid JSON path expression. Expected field name after '.'", character: *cursor}
	}

	if path[0] == '"' {
		right := strings.Index(path[1:], "\"")
		if right < 0 {
			return "", "", &parseErr{msg: "Invalid JSON path expression. '\"' expected", character: *cursor}
		}
		name = path[1 : right+1]
		remainingPath = path[right+2:]
		*cursor = *cursor + right + 2
	} else {
		matches := regex.FindStringSubmatch(path)
		if len(matches) != 3 {
			return "", "", &parseErr{msg: "Invalid JSON path expression. Expected field name after '.'", character: *cursor}
		}
		name = matches[1]
		remainingPath = matches[2]
		*cursor = *cursor + len(name)
	}

	return
}

// updateArray will update an array element appropriately when the path element is an array. This includes parsing
// the special indexes. If there are more elements in the path after this element look up, the update will be performed
// by the walkPathAndUpdate function.
func updateArray(indexString string, remaining string, arr []interface{}, val interface{}, mode int, cursor *int) (interface{}, bool, *parseErr) {
	index, err := parseIndex(indexString, len(arr)-1, cursor)
	if err != nil {
		return nil, false, err
	}

	// All operations, except for SET, ignore the underflow case.
	if index.underflow && (mode != SET) {
		return arr, false, nil
	}

	if len(arr) > index.index && !index.overflow {
		// index exists in the array.
		if remaining == "" && mode != ARRAY_APPEND {
			updated := false
			if mode == SET || mode == REPLACE {
				arr[index.index] = val
				updated = true
			} else if mode == REMOVE {
				arr = append(arr[:index.index], arr[index.index+1:]...)
				updated = true
			} else if mode == ARRAY_INSERT {
				newArr := make([]interface{}, len(arr)+1)
				copy(newArr, arr[:index.index])
				newArr[index.index] = val
				copy(newArr[index.index+1:], arr[index.index:])
				arr = newArr
				updated = true
			}
			return arr, updated, nil
		} else {
			newVal, changed, err := walkPathAndUpdate(remaining, arr[index.index], val, mode, cursor)
			if err != nil {
				return nil, false, err
			}
			if changed {
				arr[index.index] = newVal
			}
			return arr, changed, nil
		}
	} else {
		if mode == SET || mode == INSERT || mode == ARRAY_INSERT {
			newArr := append(arr, val)
			return newArr, true, nil
		}
		return arr, false, nil
	}
}

// updateObjectTreatAsArray handles the case where the user is treating an object or scalar as an array. The behavior in MySQL here
// is a little nutty, but we try to match it as closely as possible. In particular, each mode has a different behavior,
// and the behavior defies logic. This is  mimicking MySQL because it's not dangerous, and there may be some crazy
// use case which expects this behavior.
func updateObjectTreatAsArray(indexString string, doc interface{}, val interface{}, mode int, cursor *int) (interface{}, bool, *parseErr) {
	parsedIndex, err := parseIndex(indexString, 0, cursor)
	if err != nil {
		return nil, false, err
	}

	if parsedIndex.underflow {
		if mode == SET || mode == INSERT {
			// SET and INSERT convert {}, to [val, {}]
			var newArr = make([]interface{}, 0, 2)
			newArr = append(newArr, val)
			newArr = append(newArr, doc)
			return newArr, true, nil
		}
	} else if parsedIndex.overflow {
		if mode == SET || mode == INSERT {
			// SET and INSERT convert {}, to [{}, val]
			var newArr = make([]interface{}, 0, 2)
			newArr = append(newArr, doc)
			newArr = append(newArr, val)
			return newArr, true, nil
		}
	} else if mode == SET || mode == REPLACE {
		return val, true, nil
	} else if mode == ARRAY_APPEND {
		// ARRAY APPEND converts {}, to [{}, val] - Does nothing in the over/underflow cases.
		var newArr = make([]interface{}, 0, 2)
		newArr = append(newArr, doc)
		newArr = append(newArr, val)
		return newArr, true, nil
	}
	return doc, false, nil
}

// parseIndexResult is the result of parsing an index by the parseIndex function.
type parseIndexResult struct {
	underflow bool // true if the index was under 0 - will only happen with last-1000, for example.
	overflow  bool // true if the index was greater than the length of the array.
	index     int  // the index to use. Will be 0 if underflow is true, or the length of the array if overflow is true.
}

// parseIndex parses an array index string. These are of the form:
// 1. standard integer
// 2. "last"
// 3. "last-NUMBER" - to get the second to last element in an array.
// 4. "M to N", "last-4 to N", "M to last-4", "last-4 to last-2" (Currently we don't support this)
//
// White space is ignored completely.
//
// The lastIndex sets index of the last element. -1 for an empty array.
func parseIndex(indexStr string, lastIndex int, cursor *int) (*parseIndexResult, *parseErr) {
	// trim whitespace off the ends
	indexStr = strings.TrimSpace(indexStr)

	if indexStr == "last" {
		if lastIndex < 0 {
			lastIndex = 0 // This happens for an empty array
		}
		return &parseIndexResult{index: lastIndex}, nil
	} else {
		// Attempt to split the string on "-". "last-2" gets the second to last element in an array.
		parts := strings.Split(indexStr, "-")
		if len(parts) == 2 {
			part1, part2 := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			if part1 == "last" {
				lastMinus, err := strconv.Atoi(part2)
				if err != nil || lastMinus < 0 {
					*cursor = *cursor + 4 // len("last")
					return nil, &parseErr{msg: "Invalid JSON path expression. Expected a positive integer after 'last-'", character: *cursor}
				}

				underFlow := false
				reducedIdx := lastIndex - lastMinus
				if reducedIdx < 0 {
					reducedIdx = 0
					underFlow = true
				}
				return &parseIndexResult{index: reducedIdx, underflow: underFlow}, nil
			} else {
				return nil, &parseErr{msg: "Invalid JSON path expression. Expected 'last-N'", character: *cursor}
			}
		}
	}

	val, err := strconv.Atoi(indexStr)
	if err != nil {
		msg := fmt.Sprintf("Invalid JSON path expression. Unable to convert %s to an int", indexStr)
		return nil, &parseErr{msg: msg, character: *cursor}
	}

	overflow := false
	if val > lastIndex {
		val = lastIndex
		overflow = true
	}

	return &parseIndexResult{index: val, overflow: overflow}, nil
}
