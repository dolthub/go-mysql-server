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

package sql

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/oliveagle/jsonpath"
)

// JSONValue is an integrator specific implementation of a JSON field value.
type JSONValue interface {
	// Unmarshall converts a JSONValue to a JSONDocument
	Unmarshall(ctx *Context) (val JSONDocument, err error)
	// Compare compares two JSONValues. It maintains the same return value
	// semantics as Type.Compare()
	Compare(ctx *Context, v JSONValue) (cmp int, err error)
	// ToString marshalls a JSONValue to a valid JSON-encoded string.
	ToString(ctx *Context) (string, error)
}

// SearchableJSONValue is JSONValue supporting in-place access operations.
// The query engine can utilize these optimized access methods improve performance
// by minimizing the need to unmarshall a JSONValue into a JSONDocument.
type SearchableJSONValue interface {
	JSONValue

	// Contains is value-specific implementation of JSON_Contains()
	Contains(ctx *Context, candidate JSONValue) (val interface{}, err error)
	// Extract is value-specific implementation of JSON_Extract()
	Extract(ctx *Context, path string) (val JSONValue, err error)
	// Keys is value-specific implementation of JSON_Keys()
	Keys(ctx *Context, path string) (val JSONValue, err error)
	// Overlaps is value-specific implementation of JSON_Overlaps()
	Overlaps(ctx *Context, val SearchableJSONValue) (ok bool, err error)
	// Search is value-specific implementation of JSON_Search()
	Search(ctx *Context) (path string, err error)
}

type JSONDocument struct {
	Val interface{}
}

var _ JSONValue = JSONDocument{}

func (doc JSONDocument) Unmarshall(_ *Context) (JSONDocument, error) {
	return doc, nil
}

func (doc JSONDocument) Compare(ctx *Context, v JSONValue) (int, error) {
	other, err := v.Unmarshall(ctx)
	if err != nil {
		return 0, err
	}
	return compareJSON(doc.Val, other.Val)
}

func (doc JSONDocument) ToString(_ *Context) (string, error) {
	bb, err := json.Marshal(doc.Val)
	return string(bb), err
}

var _ SearchableJSONValue = JSONDocument{}

// Contains returns nil in case of a nil value for either the doc.Val or candidate. Otherwise
// it returns a bool
func (doc JSONDocument) Contains(ctx *Context, candidate JSONValue) (val interface{}, err error) {
	other, err := candidate.Unmarshall(ctx)
	if err != nil {
		return false, err
	}
	return containsJSON(doc.Val, other.Val)
}

func (doc JSONDocument) Extract(ctx *Context, path string) (JSONValue, error) {
	c, err := jsonpath.Compile(path)
	if err != nil {
		return nil, err
	}

	// TODO(andy) handle error
	val, _ := c.Lookup(doc.Val) // err ignored

	return JSONDocument{Val: val}, nil
}

func (doc JSONDocument) Keys(ctx *Context, path string) (val JSONValue, err error) {
	panic("not implemented")
}

func (doc JSONDocument) Overlaps(ctx *Context, val SearchableJSONValue) (ok bool, err error) {
	panic("not implemented")
}

func (doc JSONDocument) Search(ctx *Context) (path string, err error) {
	panic("not implemented")
}

func ConcatenateJSONValues(ctx *Context, vals ...JSONValue) (JSONValue, error) {
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
		return false, ErrInvalidType.New(a)
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
// For comparison of JSON and non-JSON values, the non-JSON value is first converted to JSON (see jsonType.Convert()).
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
	if hasNulls, res := compareNulls(a, b); hasNulls {
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
		return 0, ErrInvalidType.New(a)
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
