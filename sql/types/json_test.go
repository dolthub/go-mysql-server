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
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	// _ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonCompare(t *testing.T) {
	tests := []struct {
		left  string
		right string
		cmp   int
	}{
		// type precedence hierarchy: BOOLEAN, ARRAY, OBJECT, STRING, DOUBLE, NULL
		{`true`, `[0]`, 1},
		{`[0]`, `{"a": 0}`, 1},
		{`{"a": 0}`, `"a"`, 1},
		{`"a"`, `0`, 1},
		{`0`, `null`, 1},

		// json null
		{`null`, `0`, -1},
		{`0`, `null`, 1},
		{`null`, `null`, 0},

		// boolean
		{`true`, `false`, 1},
		{`true`, `true`, 0},
		{`false`, `false`, 0},

		// strings
		{`"A"`, `"B"`, -1},
		{`"A"`, `"A"`, 0},
		{`"C"`, `"B"`, 1},

		// numbers
		{`0`, `0.0`, 0},
		{`0`, `-1`, 1},
		{`0`, `3.14`, -1},

		// arrays
		{`[1,2]`, `[1,2]`, 0},
		{`[1,9]`, `[1,2]`, 1},
		{`[1,2]`, `[1,2,3]`, -1},

		// objects
		{`{"a": 0}`, `{"a": 0}`, 0},
		// deterministic object ordering with arbitrary rules
		{`{"a": 1}`, `{"a": 0}`, 1},                 // 1 > 0
		{`{"a": 0}`, `{"a": 0, "b": 1}`, -1},        // longer
		{`{"a": 0, "c": 2}`, `{"a": 0, "b": 1}`, 1}, // "c" > "b"

		// nested
		{
			left:  `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
			right: `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
			cmp:   0,
		},
		{
			left:  `{"one": ["x", "y"],      "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
			right: `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
			cmp:   -1,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%v_%v__%d", test.left, test.right, test.cmp)
		t.Run(name, func(t *testing.T) {
			cmp, err := JSON.Compare(
				MustJSON(test.left),
				MustJSON(test.right),
			)
			require.NoError(t, err)
			assert.Equal(t, test.cmp, cmp)
		})
	}
}

func TestJsonCompareNulls(t *testing.T) {
	tests := []struct {
		left  interface{}
		right interface{}
		cmp   int
	}{
		{nil, MustJSON(`{"key": "value"}`), 1},
		{MustJSON(`{"key": "value"}`), nil, -1},
		{nil, nil, 0},
		{nil, MustJSON(`null`), 1},
		{MustJSON(`null`), nil, -1},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%v_%v__%d", test.left, test.right, test.cmp)
		t.Run(name, func(t *testing.T) {
			cmp, err := JSON.Compare(test.left, test.right)
			require.NoError(t, err)
			assert.Equal(t, test.cmp, cmp)
		})
	}
}

func TestJsonConvert(t *testing.T) {
	type testStruct struct {
		Field string `json:"field"`
	}
	tests := []struct {
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{`""`, MustJSON(`""`), false},
		{[]int{1, 2}, MustJSON(`[1, 2]`), false},
		{`{"a": true, "b": 3}`, MustJSON(`{"a":true,"b":3}`), false},
		{[]byte(`{"a": true, "b": 3}`), MustJSON(`{"a":true,"b":3}`), false},
		{testStruct{Field: "test"}, MustJSON(`{"field":"test"}`), false},
		{MustJSON(`{"field":"test"}`), MustJSON(`{"field":"test"}`), false},
		{[]string{}, MustJSON(`[]`), false},
		{[]string{`555-555-5555`}, MustJSON(`["555-555-5555"]`), false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, _, err := JSON.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.True(t, reflect.TypeOf(val).Implements(JSON.ValueType()))
				}
			}
		})
	}
}

func TestJsonString(t *testing.T) {
	require.Equal(t, "json", JSON.String())
}

func TestJsonSQL(t *testing.T) {
	tests := []struct {
		val         interface{}
		expectedErr bool
	}{
		{`""`, false},
		{`"555-555-555"`, false},
		{`{}`, false},
		{`{"field":"test"}`, false},
		{MustJSON(`{"field":"test"}`), false},
		{"1", false},
		{`[1,2,3]`, false},
		{[]int{1, 2, 3}, false},
		{[]string{"1", "2", "3"}, false},
		{"thisisbad", true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.val), func(t *testing.T) {
			val, err := JSON.SQL(sql.NewEmptyContext(), nil, test.val)
			if test.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, query.Type_JSON, val.Type())
			}
		})
	}

	// test that nulls are null
	t.Run(fmt.Sprintf("%v", nil), func(t *testing.T) {
		val, err := JSON.SQL(sql.NewEmptyContext(), nil, nil)
		require.NoError(t, err)
		assert.Equal(t, query.Type_NULL_TYPE, val.Type())
	})
}

func TestValuer(t *testing.T) {
	var empty JSONDocument
	res, err := empty.Value()
	require.NoError(t, err)
	require.Equal(t, nil, res)

	withVal := JSONDocument{
		Val: map[string]string{
			"a": "one",
		},
	}
	res, err = withVal.Value()
	require.NoError(t, err)
	require.Equal(t, `{"a": "one"}`, res)
}

func TestLazyJsonDocument(t *testing.T) {
	testCases := []struct {
		s    string
		json interface{}
	}{
		{`"1"`, "1"},
		{`{"a": [1.0, null]}`, map[string]any{"a": []any{1.0, nil}}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.s, func(t *testing.T) {
			doc := NewLazyJSONDocument([]byte(testCase.s))
			val, err := doc.ToInterface()
			require.NoError(t, err)
			require.Equal(t, testCase.json, val)
		})
	}
	t.Run("lazy docs only error when deserialized", func(t *testing.T) {
		doc := NewLazyJSONDocument([]byte("not valid json"))
		_, err := doc.ToInterface()
		require.Error(t, err)
	})
}

type JsonRoundtripTest struct {
	desc     string
	input    string
	expected string
}

var JsonRoundtripTests = []JsonRoundtripTest{
	{
		desc:     "formatted json",
		input:    `{"a": 1, "b": 2}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "unordered keys with correct spacing",
		input:    `{"b": 2, "a": 1}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "missing spaces after comma and colon",
		input:    `{"a":1,"b":2}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "unordered keys with no spacing",
		input:    `{"b":2,"a":1}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "unordered keys with extra spaces",
		input:    `{"b" : 2, "a" : 1}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "unordered keys with extra spaces and missing spaces after comma and colon",
		input:    `{"b" :2,"a" :1}`,
		expected: `{"a": 1, "b": 2}`,
	},
	{
		desc:     "arrays of primitives without spaces",
		input:    `{"a":[1,2,3],"b":[4,5,6]}`,
		expected: `{"a": [1, 2, 3], "b": [4, 5, 6]}`,
	},
	{
		desc:     "unordered keys with arrays of primitives",
		input:    `{"b":[4,5,6],"a":[1,2,3]}`,
		expected: `{"a": [1, 2, 3], "b": [4, 5, 6]}`,
	},
	{
		desc:     "arrays of objects without spaces",
		input:    `{"a":[{"a":1},{"b":2}],"b":[{"c":3},{"d":4}]}`,
		expected: `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`,
	},
	{
		desc:     "unordered keys with arrays of objects",
		input:    `{"b":[{"c":3},{"d":4}],"a":[{"a":1},{"b":2}]}`,
		expected: `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`,
	},
	{
		desc:     "unordered keys with arrays of objects with extra spaces",
		input:    `{"b" : [ { "c" : 3 }, { "d" : 4 } ], "a" : [ { "a" : 1 }, { "b" : 2 } ] }`,
		expected: `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`,
	},
	{
		desc:     "arrays of objects with extra spaces",
		input:    `{"a": [ { "a" : 1 }, { "b" : 2 } ], "b": [ { "c" : 3 }, { "d" : 4 } ] }`,
		expected: `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`,
	},
}

func TestJsonRoundTripping(t *testing.T) {
	for _, test := range JsonRoundtripTests {
		t.Run("JSON roundtripping: "+test.desc, func(t *testing.T) {
			val, err := JSON.SQL(sql.NewEmptyContext(), nil, test.input)
			require.NoError(t, err)
			assert.Equal(t, test.expected, val.ToString())
		})
	}
}

type JsonMutationTest struct {
	desc      string
	doc       string
	path      string
	value     string
	resultVal string
	changed   bool
	//	expectErrStr string
}

var JsonSetTests = []JsonMutationTest{
	{
		desc:      "set root",
		doc:       `{"a": 1, "b": 2}`,
		path:      "$",
		value:     `{"c": 3}`,
		resultVal: `{"c": 3}`,
		changed:   true,
	},
	{
		desc:      "set root ignore white space",
		doc:       `{"a": 1, "b": 2}`,
		path:      "   $   ",
		value:     `{"c": 3}`,
		resultVal: `{"c": 3}`,
		changed:   true,
	},
	{
		desc:      "set middle of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[1]",
		value:     `42`,
		resultVal: `[1, 42, 3]`,
		changed:   true,
	},
	{
		desc:      "set last item of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[2]",
		value:     `42`,
		resultVal: `[1, 2, 42]`,
		changed:   true,
	},
	{
		desc:      "append to an array when overflown",
		doc:       `[1, 2, 3]`,
		path:      "$[23]",
		value:     `42`,
		resultVal: `[1, 2, 3, 42]`,
		changed:   true,
	},
	{
		desc:      "set 'last' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[1, 2, 42]`,
		changed:   true,
	},
	{
		desc:      "set 'last-0' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last-0]",
		value:     `42`,
		resultVal: `[1, 2, 42]`,
		changed:   true,
	},
	{
		desc:      "set 'last-23' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[42, 2, 3]`,
		changed:   true,
	},
	{
		desc:      "array index ignores white space",
		doc:       `[1, 2, 3]`,
		path:      "$[   last -    1]",
		value:     `42`,
		resultVal: `[1, 42, 3]`,
		changed:   true,
	},
	{
		desc:      "empty array last index is 0",
		doc:       `[]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[42]`,
		changed:   true,
	},
	{
		desc:      "treating object as an array replaces for index 0",
		doc:       `{"a":1}`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		desc:      "treating object as an array replaces for index last",
		doc:       `{"a":1}`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		desc:      "treating object will prefix as an array",
		doc:       `{"a":1}`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[42, {"a": 1}]`,
		changed:   true,
	},
	{
		desc:      "treating object will append as an array for out of bounds",
		doc:       `{"a":1}`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `[{"a": 1}, 42]`,
		changed:   true,
	},
	{
		desc:      "scalar will append as an array for out of bounds",
		doc:       `17`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `[17, 42]`,
		changed:   true,
	},
	{
		desc:      "scalar will be overwritten for index 0",
		doc:       `17`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		desc:      "scalar will be prefixed when underflow happens",
		doc:       `17`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[42, 17]`,
		changed:   true,
	},
	{
		desc:      "Object field updated",
		doc:       `{"a": 1}`,
		path:      "$.a",
		value:     `42`,
		resultVal: `{"a": 42}`,
		changed:   true,
	},
	{
		desc:      "Object field set",
		doc:       `{"a": 1}`,
		path:      "$.b",
		value:     `42`,
		resultVal: `{"a": 1, "b": 42}`,
		changed:   true,
	},
	{
		desc:      "Object field set Unicode",
		doc:       `{"❤️🧡💛💚💙💜": {}}`,
		path:      `$."❤️🧡💛💚💙💜"`,
		value:     `42`,
		resultVal: `{"❤️🧡💛💚💙💜": 42 }`,
		changed:   true,
	},
	{
		desc:      "Object field name can optionally have quotes",
		doc:       `{"a": {}}`,
		path:      `$."a"`,
		value:     `42`,
		resultVal: `{"a": 42 }`,
		changed:   true,
	},
	{
		desc:      "object field name can contain escaped quotes",
		doc:       `{"\"a\"": {}}`,
		path:      `$."\"a\""`,
		value:     `42`,
		resultVal: `{"\"a\"": 42 }`,
		changed:   true,
	},
	{
		desc:      "Object field can be set to null",
		doc:       `{"a": {}}`,
		path:      `$."a"`,
		value:     `null`,
		resultVal: `{"a": null }`,
		changed:   true,
	},
	{
		desc:      "Array treated as an object is a no op",
		doc:       `[1, 2, 3]`,
		path:      `$.a`,
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "Setting in a nested array works",
		doc:       `[1, [2]]`,
		path:      "$[1][0]",
		changed:   true,
		value:     `42`,
		resultVal: `[1, [42]]`,
	},
	{
		desc:      "Setting in a nested objects works",
		doc:       `{"a": {"b": 1}}`,
		path:      "$.a.b",
		changed:   true,
		value:     `42`,
		resultVal: `{"a": {"b": 42}}`,
	},
	{
		desc:      "Setting in a nested several levels deep works",
		doc:       `{"a": {"b": [1,2,3,4,[5,6,7]]}}`,
		path:      "$.a.b[4][1]",
		changed:   true,
		value:     `96`,
		resultVal: `{"a": {"b": [1,2,3,4,[5,96,7]]}}`,
	},
	{
		desc:      "setting in a nested several levels deep works",
		doc:       `[9,8, {"a": [3,4,5] } ]`,
		path:      "$[2].a[0]",
		changed:   true,
		value:     `96`,
		resultVal: `[9,8, {"a": [96,4,5]}]`,
	},
	{
		desc:      "setting a deep path has no effect",
		doc:       `{}`,
		path:      "$.a.b.c",
		changed:   false,
		value:     `42`,
		resultVal: `{}`,
	},
	{
		desc:      "setting a deep path has no effect",
		doc:       `{}`,
		path:      "$.a.b[last]",
		changed:   false,
		value:     `42`,
		resultVal: `{}`,
	},
	{
		// I've verified that null in MySQL is treated as a scalar in all ways that I can tell, which makes sense.
		// Therefore, testing beyond these two tests doesn't necessary.
		desc:      "setting a null doc with a value results in the value",
		doc:       `null`,
		path:      "$",
		changed:   true,
		value:     `{"a": 42}`,
		resultVal: `{"a": 42}`,
	},
	{
		desc:      "setting a null doc with a value results in the value",
		doc:       `{"a" : 1}`,
		path:      "$",
		changed:   true,
		value:     `null`,
		resultVal: `null`,
	},

	/* Known ways we don't behave like MySQL. Frankly if anyone is depending on these behaviors they are doing it wrong.
		   mysql> select JSON_SET('{"a": 1}', "$[0][0]", 42);
		+-------------------------------------+
		| JSON_SET('{"a": 1}', "$[0][0]", 42) |
		+-------------------------------------+
		| 42                                  |
		+-------------------------------------+

		mysql> select JSON_SET('{"a": 1}', "$[0][1]", 42);
		+-------------------------------------+
		| JSON_SET('{"a": 1}', "$[0][1]", 42) |
		+-------------------------------------+
		| [{"a": 1}, 42]                      |
		+-------------------------------------+

		mysql> select JSON_SET('{"a": 1}', "$[0][last-3]", 42);
		+------------------------------------------+
		| JSON_SET('{"a": 1}', "$[0][last-3]", 42) |
		+------------------------------------------+
		| [42, {"a": 1}]                           |
		+------------------------------------------+
	    The three examples above seems to indicate that MySQL coerces objects to arrays earlier in the search process than we do.
		Reason for thinking this is that  JSON_SET('{"a": 1}', "$[0][0][ANYTHING]", 42); is a no op.
	*/

}

func TestJsonSet(t *testing.T) {
	for _, test := range JsonSetTests {
		t.Run("JSON set: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			val := MustJSON(test.value)
			res, changed, err := doc.Set(test.path, val)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

var JsonInsertTests = []JsonMutationTest{
	{
		desc:      "insert root",
		doc:       `{"a": 1, "b": 2}`,
		path:      "$",
		value:     `{"c": 3}`,
		resultVal: `{"a": 1, "b": 2}`,
		changed:   false,
	},

	{
		desc:      "insert root ignore white space",
		doc:       `{"a": 1, "b": 2}`,
		path:      "   $   ",
		value:     `{"c": 3}`,
		resultVal: `{"a": 1, "b": 2}`,
		changed:   false,
	},
	{
		desc:      "insert middle of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[1]",
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "insert last item of an array does nothing",
		doc:       `[1, 2, 3]`,
		path:      "$[2]",
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "append to an array when overflown",
		doc:       `[1, 2, 3]`,
		path:      "$[23]",
		value:     `42`,
		resultVal: `[1, 2, 3, 42]`,
		changed:   true,
	},
	{
		desc:      "insert 'last' element of an array does nothing",
		doc:       `[1, 2, 3]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "insert into empty array mutates",
		doc:       `[]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[42]`,
		changed:   true,
	},
	{
		desc:      "treating object as an array replaces for index 0",
		doc:       `{"a":1}`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `{"a":1}`,
		changed:   false,
	},
	{
		// Can't make this stuff up.
		//	mysql> select JSON_INSERT(JSON_OBJECT("a",1),'$[last-21]', 42);
		// +--------------------------------------------------+
		// | JSON_INSERT(JSON_OBJECT("a",1),'$[last-21]', 42) |
		// +--------------------------------------------------+
		// | [42, {"a": 1}]                                   |
		// +--------------------------------------------------+
		desc:      "treating object will prefix as an array",
		doc:       `{"a":1}`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[42, {"a": 1}]`,
		changed:   true,
	},
	{
		// mysql> select JSON_INSERT(JSON_OBJECT("a",1),'$[51]', 42);
		// +---------------------------------------------+
		// | JSON_INSERT(JSON_OBJECT("a",1),'$[51]', 42) |
		// +---------------------------------------------+
		// | [{"a": 1}, 42]                              |
		// +---------------------------------------------+
		desc:      "treating object will append as an array for out of bounds",
		doc:       `{"a":1}`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `[{"a": 1}, 42]`,
		changed:   true,
	},
	{
		desc:      "scalar will append as an array for out of bounds",
		doc:       `17`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `[17, 42]`,
		changed:   true,
	},
	{
		desc:      "scalar will not be overwritten for index 0",
		doc:       `17`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `17`,
		changed:   false,
	},
	{
		desc:      "scalar will be prefixed when underflow happens",
		doc:       `17`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[42, 17]`,
		changed:   true,
	},
	{
		desc:      "existing object field not updated",
		doc:       `{"a": 1}`,
		path:      "$.a",
		value:     `42`,
		resultVal: `{"a": 1}`,
		changed:   false,
	},
	{
		desc:      "new object field inserted",
		doc:       `{"a": 1}`,
		path:      "$.b",
		value:     `42`,
		resultVal: `{"a": 1, "b": 42}`,
		changed:   true,
	},
	{
		desc:      "object field name can optionally have quotes",
		doc:       `{"a": {}}`,
		path:      `$."a"`,
		value:     `42`,
		resultVal: `{"a": {} }`,
		changed:   false,
	},
	{
		desc:      "object field name can contain escaped quotes",
		doc:       `{"\"a\"": {}}`,
		path:      `$."\"a\""`,
		value:     `42`,
		resultVal: `{"\"a\"": {} }`,
		changed:   false,
	},
	{
		desc:      "array treated as an object is a no op",
		doc:       `[1, 2, 3]`,
		path:      `$.a`,
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},

	{
		desc:      "inserting a deep path has no effect",
		doc:       `{}`,
		path:      "$.a.b.c",
		changed:   false,
		value:     `42`,
		resultVal: `{}`,
	},
	{
		desc:      "inserting a deep path has no effect",
		doc:       `{}`,
		path:      "$.a.b[last]",
		changed:   false,
		value:     `42`,
		resultVal: `{}`,
	},
}

func TestJsonInsert(t *testing.T) {
	ctx := context.Background()
	for _, test := range JsonInsertTests {
		t.Run("JSON insert: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			val := MustJSON(test.value)
			res, changed, err := doc.Insert(ctx, test.path, val)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

var JsonRemoveTests = []JsonMutationTest{
	{
		desc:      "remove middle of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[1]",
		resultVal: `[1, 3]`,
		changed:   true,
	},
	{
		desc:      "remove last item of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[2]",
		resultVal: `[1, 2]`,
		changed:   true,
	},
	{
		desc:      "no op remove on array when overflown",
		doc:       `[1, 2, 3]`,
		path:      "$[23]",
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "remove 'last' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last]",
		resultVal: `[1, 2]`,
		changed:   true,
	},
	{
		desc:      "remove 'last-0' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last-0]",
		resultVal: `[1, 2]`,
		changed:   true,
	},
	{
		desc:      "no op remove on underflow 'last-23' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last-23]",
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "no op remove with empty array",
		doc:       `[]`,
		path:      "$[last]",
		resultVal: `[]`,
		changed:   false,
	},
	{
		// Remove behaves much more reasonably than other operations when the path is invalid. When you treat an
		// object or scalar as an array, it is a no-op. period. For this reason, there are far fewer remove tests than
		// there are for insert/set/replace.
		desc:      "treating object as an array is no op",
		doc:       `{"a":1}`,
		path:      "$[0]",
		resultVal: `{"a" : 1}`,
		changed:   false,
	},
	{
		desc:      "scalar will append as an array for out of bounds",
		doc:       `17`,
		path:      "$[0]",
		resultVal: `17`,
		changed:   false,
	},
	{
		desc:      "Object field updated",
		doc:       `{"a": 1, "b": 2}`,
		path:      "$.a",
		resultVal: `{"b": 2}`,
		changed:   true,
	},
	{
		desc:      "No op object change when field not found",
		doc:       `{"a": 1}`,
		path:      "$.b",
		resultVal: `{"a": 1}`,
		changed:   false,
	},
}

func TestJsonRemove(t *testing.T) {
	for _, test := range JsonRemoveTests {
		t.Run("JSON remove: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			res, changed, err := doc.Remove(test.path)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

var JsonReplaceTests = []JsonMutationTest{
	{
		desc:      "replace root",
		doc:       `{"a": 1, "b": 2}`,
		path:      "$",
		value:     `{"c": 3}`,
		resultVal: `{"c": 3}`,
		changed:   true,
	},

	{
		desc:      "replace root ignore white space",
		doc:       `{"a": 1, "b": 2}`,
		path:      "   $   ",
		value:     `{"c": 3}`,
		resultVal: `{"c": 3}`,
		changed:   true,
	},
	{
		desc:      "replace middle of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[1]",
		value:     `42`,
		resultVal: `[1, 42, 3]`,
		changed:   true,
	},
	{
		desc:      "set last item of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[2]",
		value:     `42`,
		resultVal: `[1, 2, 42]`,
		changed:   true,
	},
	{
		desc:      "noupdate to an array when overflown",
		doc:       `[1, 2, 3]`,
		path:      "$[23]",
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "replace 'last' element of an array",
		doc:       `[1, 2, 3]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[1, 2, 42]`,
		changed:   true,
	},
	{
		// mysql> select JSON_REPLACE(JSON_ARRAY(1,2,3),'$[last-23]', 42);
		// +--------------------------------------------------+
		// | JSON_REPLACE(JSON_ARRAY(1,2,3),'$[last-23]', 42) |
		// +--------------------------------------------------+
		// | [1, 2, 3]                                        |
		// +--------------------------------------------------+
		// 1 row in set (0.00 sec)
		desc:      "no update for element underflow",
		doc:       `[1, 2, 3]`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `[1, 2, 3]`,
		changed:   false,
	},
	{
		desc:      "no update for empty array",
		doc:       `[]`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `[]`,
		changed:   false,
	},
	{
		desc:      "treating object as an array replaces for index 0",
		doc:       `{"a":1}`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		// mysql> select JSON_REPLACE(JSON_OBJECT("a",1),'$[last]', 42);
		// +------------------------------------------------+
		// | JSON_REPLACE(JSON_OBJECT("a",1),'$[last]', 42) |
		// +------------------------------------------------+
		// | 42                                             |
		// +------------------------------------------------+
		desc:      "treating object as an array replaces for index last",
		doc:       `{"a":1}`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		desc:      "no op when treating object as an array with underflow",
		doc:       `{"a":1}`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `{"a": 1}`,
		changed:   false,
	},
	{
		desc:      "no op when treating object as an array with overflow",
		doc:       `{"a":1}`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `{"a": 1}`,
		changed:   false,
	},
	{
		desc:      "no update for scalar will treated as an array for out of bounds",
		doc:       `17`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `17`,
		changed:   false,
	},
	{
		desc:      "scalar will be overwritten for index 0",
		doc:       `17`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `42`,
		changed:   true,
	},
	{
		desc:      "no update for scalar when used as an array with underflow",
		doc:       `17`,
		path:      "$[last-23]",
		value:     `42`,
		resultVal: `17`,
		changed:   false,
	},
	{
		desc:      "Object field updated",
		doc:       `{"a": 1}`,
		path:      "$.a",
		value:     `42`,
		resultVal: `{"a": 42}`,
		changed:   true,
	},
	{
		desc:      "Object field not inserted",
		doc:       `{"a": 1}`,
		path:      "$.b",
		value:     `42`,
		resultVal: `{"a": 1}`,
		changed:   false,
	},
}

func TestJsonReplace(t *testing.T) {
	for _, test := range JsonReplaceTests {
		t.Run("JSON replace: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			val := MustJSON(test.value)
			res, changed, err := doc.Replace(test.path, val)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

var JsonArrayAppendTests = []JsonMutationTest{
	{
		desc:      "append to empty object",
		doc:       `{}`,
		path:      "$",
		value:     `42`,
		changed:   true,
		resultVal: `[{}, 42]`,
	},
	{
		desc:      "append to empty array",
		doc:       `[]`,
		path:      "$",
		value:     `42`,
		changed:   true,
		resultVal: `[42]`,
	},
	{
		desc:      "append to a nested array",
		doc:       `[{"a": [1, 2, 3, 4]}]`,
		path:      "$[0].a",
		value:     `42`,
		changed:   true,
		resultVal: `[{"a": [1, 2, 3, 4, 42]}]`,
	},
	{
		desc:      "append a scalar to a scalar leads to an array",
		doc:       `{"a": "eh"}`,
		path:      "$.a",
		changed:   true,
		value:     `42`,
		resultVal: `{"a": ["eh", 42]}`,
	},
	{
		desc:      "append to an array index",
		doc:       `[[1, 2, 3], {"a": "eh"}]`,
		path:      "$[0]",
		changed:   true,
		value:     `42`,
		resultVal: `[[1, 2, 3, 42], {"a": "eh"}]`,
	},
	{
		desc:      "append to an array index",
		doc:       `[{"b" : "be"}, {"a": "eh"}]`,
		path:      "$[0]",
		changed:   true,
		value:     `42`,
		resultVal: `[[{"b" : "be"}, 42], {"a": "eh"}]`,
	},
	{
		desc:      "no op when out of bounds",
		doc:       `[1, 2, 3]`,
		path:      "$[51]",
		changed:   false,
		value:     `42`,
		resultVal: `[1, 2, 3]`,
	},
	{
		desc:      "last index works for lookup",
		doc:       `[1, 2, [3,4]]`,
		path:      "$[last]",
		changed:   true,
		value:     `42`,
		resultVal: `[1, 2, [3, 4, 42]]`,
	},
	{
		desc:      "no op when there is an underflow",
		doc:       `[1, 2, 3]`,
		path:      "$[last-23]",
		changed:   false,
		value:     `42`,
		resultVal: `[1, 2, 3]`,
	},
}

func TestJsonArrayAppend(t *testing.T) {
	for _, test := range JsonArrayAppendTests {
		t.Run("JSON array append: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			val := MustJSON(test.value)
			res, changed, err := doc.ArrayAppend(test.path, val)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

var JsonArrayInsertTests = []JsonMutationTest{
	{
		desc:      "array insert overflow appends",
		doc:       `[1,2,3]`,
		path:      "$[51]",
		value:     `42`,
		resultVal: `[1,2,3,42]`,
		changed:   true,
	},
	{
		desc:      "array insert at first element",
		doc:       `[1,2,3]`,
		path:      "$[0]",
		value:     `42`,
		resultVal: `[42,1,2,3]`,
		changed:   true,
	},
	{
		desc:      "array insert at second element",
		doc:       `[1,2,3]`,
		path:      "$[1]",
		value:     `42`,
		resultVal: `[1,42,2,3]`,
		changed:   true,
	},
	{
		desc:      "insert to empty array",
		doc:       `{"a" :[]}`,
		path:      "$.a[0]",
		value:     `42`,
		resultVal: `{"a" : [42]}`,
		changed:   true,
	},
	{
		desc:      "insert to an object no op",
		doc:       `{"a" :{}}`,
		path:      "$.a[0]",
		value:     `42`,
		resultVal: `{"a" : {}}`,
		changed:   false,
	},
	// mysql> select json_array_insert(json_array(1,2,3), "$[last]", 42);
	// +-----------------------------------------------------+
	// | json_array_insert(json_array(1,2,3), "$[last]", 42) |
	// +-----------------------------------------------------+
	// | [1, 2, 42, 3]                                       |
	// +-----------------------------------------------------+
	{
		desc:      "insert to [last] does crazy things",
		doc:       `[1,2,3]`,
		path:      "$[last]",
		value:     `42`,
		resultVal: `[1,2,42,3]`, // It's true. Try it yourself.
		changed:   true,
	},
	{
		desc:      "insert into non-array results in noop",
		doc:       `{}`,
		path:      "$[0]",
		value:     `42`,
		changed:   false,
		resultVal: `{}`,
	},
}

func TestJsonArrayInsert(t *testing.T) {
	for _, test := range JsonArrayInsertTests {
		t.Run("JSON array insert: "+test.desc, func(t *testing.T) {
			doc := MustJSON(test.doc)
			val := MustJSON(test.value)
			res, changed, err := doc.ArrayInsert(test.path, val)
			require.NoError(t, err)
			assert.Equal(t, MustJSON(test.resultVal), res)
			assert.Equal(t, test.changed, changed)
		})
	}
}

type parseErrTest struct {
	desc         string
	doc          string
	path         string
	expectErrStr string
}

var JsonPathParseErrTests = []parseErrTest{
	{
		desc:         "empty path",
		path:         "",
		expectErrStr: "Invalid JSON path expression. Empty path",
	},
	{
		desc:         "non $ prefix",
		path:         "bogus",
		expectErrStr: "Invalid JSON path expression. Path must start with '$'",
	},
	{
		desc:         "dot to nowhere",
		path:         "$.",
		expectErrStr: "Invalid JSON path expression. Expected field name after '.' at character 2 of $.",
	},
	{
		desc:         "no . or [",
		path:         "$fu.bar",
		expectErrStr: "Invalid JSON path expression. Expected '.' or '[' at character 1 of $fu.bar",
	},
	{
		desc:         "incomplete quoted field",
		path:         `$."a"."b`,
		expectErrStr: `Invalid JSON path expression. '"' expected at character 6 of $."a"."b`,
	},
	{
		desc:         "invalid bare string",
		path:         "$.a@<>bc",
		expectErrStr: `Invalid JSON path expression. Expected '.' or '[' at character 3 of $.a@<>bc`,
	},
	{
		desc:         "non-integer array index",
		path:         "$[abcd]",
		expectErrStr: `Invalid JSON path expression. Unable to convert abcd to an int at character 2 of $[abcd]`,
	},
	{
		desc:         "non-integer array index",
		path:         "$[last-abcd]",
		expectErrStr: `Invalid JSON path expression. Expected a positive integer after 'last-' at character 6 of $[last-abcd]`,
	},
	{
		desc:         "too many dashes in last-",
		path:         "$[last-abcd-xyz]",
		expectErrStr: `Invalid JSON path expression. Unable to convert last-abcd-xyz to an int at character 2 of $[last-abcd-xyz]`,
	},
}

func TestJsonPathErrors(t *testing.T) {
	doc := MustJSON(`{"a": {"b": 2} , "c": [1, 2, 3]}`)

	for _, test := range JsonPathParseErrTests {
		t.Run("JSON Path: "+test.desc, func(t *testing.T) {
			_, changed, err := doc.Set(test.path, MustJSON(`{"a": 42}`))
			assert.Equal(t, false, changed)
			require.Error(t, err)
			assert.Equal(t, test.expectErrStr, err.Error())
		})
	}
}

var JsonArrayInsertErrors = []parseErrTest{
	{
		desc:         "empty path",
		path:         "",
		expectErrStr: "Invalid JSON path expression. Empty path",
	},
	{
		desc:         "insert into root path results in an error",
		doc:          `[]`,
		path:         "$",
		expectErrStr: "Path expression is not a path to a cell in an array: $",
	},
	{
		desc:         "no op insert into non-array",
		doc:          `{"a": "eh"}`,
		path:         "$.a",
		expectErrStr: "A path expression is not a path to a cell in an array at character 3 of $.a",
	},
}

func TestJsonInsertErrors(t *testing.T) {
	doc := MustJSON(`{"a": {"b": 2} , "c": [1, 2, 3]}`)

	for _, test := range JsonArrayInsertErrors {
		t.Run("JSON Path: "+test.desc, func(t *testing.T) {
			_, changed, err := doc.ArrayInsert(test.path, MustJSON(`{"a": 42}`))
			assert.Equal(t, false, changed)
			require.Error(t, err)
			assert.Equal(t, test.expectErrStr, err.Error())
		})
	}
}

func TestRemoveRoot(t *testing.T) {
	// Fairly special case situation which doesn't mesh with our other tests. MySQL returns a specfic message when you
	// attempt to remove the root document.
	doc := MustJSON(`{"a": 1, "b": 2}`)
	_, changed, err := doc.Remove("$")

	require.Error(t, err)
	assert.Equal(t, "The path expression '$' is not allowed in this context.", err.Error())
	assert.Equal(t, false, changed)
}

type jsonIterKV struct {
	key   string
	value interface{}
}

type jsonIterTest struct {
	name          string
	doc           JsonObject
	expectedPairs []jsonIterKV
}

var jsonIterTests = []jsonIterTest{
	{
		name:          "empty object",
		doc:           JsonObject{},
		expectedPairs: []jsonIterKV{},
	},
	{
		name: "iterate over keys in sorted order",
		doc:  JsonObject{"b": 1, "a": 2},
		expectedPairs: []jsonIterKV{
			{key: "a", value: 2},
			{key: "b", value: 1},
		},
	},
	{
		name: "keys use lexicographic order, not key-length order",
		doc:  JsonObject{"b": 1, "aa": 2},
		expectedPairs: []jsonIterKV{
			{key: "aa", value: 2},
			{key: "b", value: 1},
		},
	},
}

func TestJsonIter(t *testing.T) {
	for _, test := range jsonIterTests {
		t.Run(test.name, func(t *testing.T) {
			iter := NewJSONIter(test.doc)
			pairs := make([]jsonIterKV, 0)
			for iter.HasNext() {
				var pair jsonIterKV
				var err error
				pair.key, pair.value, err = iter.Next()
				require.NoError(t, err)
				pairs = append(pairs, pair)
			}
			require.Equal(t, test.expectedPairs, pairs)
		})
	}
}
