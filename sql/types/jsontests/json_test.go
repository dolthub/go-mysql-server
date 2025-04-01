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

package jsontests

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonCompare(t *testing.T) {
	RunJsonCompareTests(t, JsonCompareTests, func(t *testing.T, left, right interface{}) (interface{}, interface{}) {
		return ConvertToJson(t, left), ConvertToJson(t, right)
	})
}

func TestJsonCompareNulls(t *testing.T) {
	RunJsonCompareTests(t, JsonCompareNullsTests, func(t *testing.T, left, right interface{}) (interface{}, interface{}) {
		return ConvertToJson(t, left), ConvertToJson(t, right)
	})
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
		{`""`, types.MustJSON(`""`), false},
		{[]int{1, 2}, types.MustJSON(`[1, 2]`), false},
		{`{"a": true, "b": 3}`, types.MustJSON(`{"a":true,"b":3}`), false},
		{[]byte(`{"a": true, "b": 3}`), types.MustJSON(`{"a":true,"b":3}`), false},
		{testStruct{Field: "test"}, types.MustJSON(`{"field":"test"}`), false},
		{types.MustJSON(`{"field":"test"}`), types.MustJSON(`{"field":"test"}`), false},
		{[]string{}, types.MustJSON(`[]`), false},
		{[]string{`555-555-5555`}, types.MustJSON(`["555-555-5555"]`), false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, _, err := types.JSON.Convert(ctx, test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.True(t, reflect.TypeOf(val).Implements(types.JSON.ValueType()))
				}
			}
		})
	}
}

func TestJsonString(t *testing.T) {
	require.Equal(t, "json", types.JSON.String())
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
		{types.MustJSON(`{"field":"test"}`), false},
		{"1", false},
		{`[1,2,3]`, false},
		{[]int{1, 2, 3}, false},
		{[]string{"1", "2", "3"}, false},
		{"thisisbad", true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.val), func(t *testing.T) {
			val, err := types.JSON.SQL(sql.NewEmptyContext(), nil, test.val)
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
		val, err := types.JSON.SQL(sql.NewEmptyContext(), nil, nil)
		require.NoError(t, err)
		assert.Equal(t, query.Type_NULL_TYPE, val.Type())
	})
}

func TestValuer(t *testing.T) {
	var empty types.JSONDocument
	res, err := empty.Value()
	require.NoError(t, err)
	require.Equal(t, nil, res)

	withVal := types.JSONDocument{
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
			doc := types.NewLazyJSONDocument([]byte(testCase.s))
			val, err := doc.ToInterface()
			require.NoError(t, err)
			require.Equal(t, testCase.json, val)
		})
	}
	t.Run("lazy docs only error when deserialized", func(t *testing.T) {
		doc := types.NewLazyJSONDocument([]byte("not valid json"))
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
			val, err := types.JSON.SQL(sql.NewEmptyContext(), nil, test.input)
			require.NoError(t, err)
			assert.Equal(t, test.expected, val.ToString())
		})
	}
}

func convertStringsToJsonDocuments(t *testing.T, doc, val, result interface{}) (types.MutableJSON, sql.JSONWrapper, types.MutableJSON) {
	if val == "" {
		val = nil
	}
	return ConvertToJson(t, doc), ConvertToJson(t, val), ConvertToJson(t, result)
}

func TestJsonSet(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonSetTests, convertStringsToJsonDocuments, "set")
}

func TestJsonInsert(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonInsertTests, convertStringsToJsonDocuments, "insert")
}

func TestJsonRemove(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonRemoveTests, convertStringsToJsonDocuments, "remove")
}

func TestJsonReplace(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonReplaceTests, convertStringsToJsonDocuments, "replace")
}

func TestJsonArrayAppend(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonArrayAppendTests, convertStringsToJsonDocuments, "arrayappend")
}

func TestJsonArrayInsert(t *testing.T) {
	ctx := context.Background()
	RunJsonMutationTests(ctx, t, JsonArrayInsertTests, convertStringsToJsonDocuments, "arrayinsert")
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
	ctx := context.Background()
	doc := types.MustJSON(`{"a": {"b": 2} , "c": [1, 2, 3]}`)

	for _, test := range JsonPathParseErrTests {
		t.Run("JSON Path: "+test.desc, func(t *testing.T) {
			_, changed, err := doc.Set(ctx, test.path, types.MustJSON(`{"a": 42}`))
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
	doc := types.MustJSON(`{"a": {"b": 2} , "c": [1, 2, 3]}`)

	for _, test := range JsonArrayInsertErrors {
		t.Run("JSON Path: "+test.desc, func(t *testing.T) {
			_, changed, err := doc.ArrayInsert(test.path, types.MustJSON(`{"a": 42}`))
			assert.Equal(t, false, changed)
			require.Error(t, err)
			assert.Equal(t, test.expectErrStr, err.Error())
		})
	}
}

func TestRemoveRoot(t *testing.T) {
	// Fairly special case situation which doesn't mesh with our other tests. MySQL returns a specfic message when you
	// attempt to remove the root document.
	ctx := context.Background()
	doc := types.MustJSON(`{"a": 1, "b": 2}`)
	_, changed, err := doc.Remove(ctx, "$")

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
	doc           types.JsonObject
	expectedPairs []jsonIterKV
}

var jsonIterTests = []jsonIterTest{
	{
		name:          "empty object",
		doc:           types.JsonObject{},
		expectedPairs: []jsonIterKV{},
	},
	{
		name: "iterate over keys in sorted order",
		doc:  types.JsonObject{"b": 1, "a": 2},
		expectedPairs: []jsonIterKV{
			{key: "a", value: 2},
			{key: "b", value: 1},
		},
	},
	{
		name: "keys use lexicographic order, not key-length order",
		doc:  types.JsonObject{"b": 1, "aa": 2},
		expectedPairs: []jsonIterKV{
			{key: "aa", value: 2},
			{key: "b", value: 1},
		},
	},
}

func TestJsonIter(t *testing.T) {
	for _, test := range jsonIterTests {
		t.Run(test.name, func(t *testing.T) {
			iter := types.NewJSONIter(test.doc)
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
