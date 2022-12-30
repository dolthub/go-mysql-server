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

		// null
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
			val, err := JSON.Convert(test.val)
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
	require.Equal(t, `{"a":"one"}`, res)
}
