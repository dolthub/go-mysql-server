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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sqlCtx = sql.NewEmptyContext()

func ConvertToJson(t *testing.T, val interface{}) types.MutableJSON {
	if val == nil {
		return nil
	}
	val, inRange, err := types.JSON.Convert(sqlCtx, val)
	require.NoError(t, err)
	require.True(t, inRange == sql.InRange)
	require.Implements(t, (*sql.JSONWrapper)(nil), val)
	val, err = val.(sql.JSONWrapper).ToInterface(t.Context())
	require.NoError(t, err)
	return types.JSONDocument{Val: val}
}

type prepareJsonCompareValues = func(t *testing.T, left, right interface{}) (interface{}, interface{})

type JsonCompareTest struct {
	Name  string
	Left  interface{}
	Right interface{}
	Cmp   int
}

var JsonCompareTests = []JsonCompareTest{
	// type precedence hierarchy: BOOLEAN, ARRAY, OBJECT, STRING, DOUBLE, NULL
	{Left: `true`, Right: `[0]`, Cmp: 1},
	{Left: `[0]`, Right: `{"a": 0}`, Cmp: 1},
	{Left: `{"a": 0}`, Right: `"a"`, Cmp: 1},
	{Left: `"a"`, Right: `0`, Cmp: 1},
	{Left: `0`, Right: `null`, Cmp: 1},

	// json null
	{Left: `null`, Right: `0`, Cmp: -1},
	{Left: `0`, Right: `null`, Cmp: 1},
	{Left: `null`, Right: `null`, Cmp: 0},

	// boolean
	{Left: `true`, Right: `false`, Cmp: 1},
	{Left: `true`, Right: `true`, Cmp: 0},
	{Left: `false`, Right: `false`, Cmp: 0},

	// strings
	{Left: `"A"`, Right: `"B"`, Cmp: -1},
	{Left: `"A"`, Right: `"A"`, Cmp: 0},
	{Left: `"C"`, Right: `"B"`, Cmp: 1},

	// numbers
	{Left: `0`, Right: `0.0`, Cmp: 0},
	{Left: `0`, Right: `-1`, Cmp: 1},
	{Left: `0`, Right: `3.14`, Cmp: -1},

	// arrays
	{Left: `[1,2]`, Right: `[1,2]`, Cmp: 0},
	{Left: `[1,9]`, Right: `[1,2]`, Cmp: 1},
	{Left: `[1,2]`, Right: `[1,2,3]`, Cmp: -1},

	// objects
	{Left: `{"a": 0}`, Right: `{"a": 0}`, Cmp: 0},
	// deterministic object ordering with arbitrary rules
	{Left: `{"a": 1}`, Right: `{"a": 0}`, Cmp: 1},          // 1 > 0
	{Left: `{"a": 0}`, Right: `{"a": 0, "b": 1}`, Cmp: -1}, // longer
	// {`{"a": 0, "c": 2}`, `{"a": 0, "b": 1}`, 1}, // "c" > "b"

	// nested
	{
		Left:  `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
		Right: `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
		Cmp:   0,
	},
	{
		Left:  `{"one": ["x", "y"],      "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
		Right: `{"one": ["x", "y", "z"], "two": { "a": 0, "b": 1}, "three": false, "four": null, "five": " "}`,
		Cmp:   -1,
	},
}

var JsonCompareNullsTests = []JsonCompareTest{
	{Left: nil, Right: types.MustJSON(`{"key": "value"}`), Cmp: 1},
	{Left: types.MustJSON(`{"key": "value"}`), Right: nil, Cmp: -1},
	{Left: nil, Right: nil, Cmp: 0},
	{Left: nil, Right: types.MustJSON(`null`), Cmp: 1},
	{Left: types.MustJSON(`null`), Right: nil, Cmp: -1},
}

func RunJsonCompareTests(t *testing.T, tests []JsonCompareTest, prepare prepareJsonCompareValues) {
	for _, test := range tests {
		name := test.Name
		if name == "" {
			name = fmt.Sprintf("%v_%v__%d", test.Left, test.Right, test.Cmp)
		}
		t.Run(name, func(t *testing.T) {
			left, right := prepare(t, test.Left, test.Right)
			cmp, err := types.JSON.Compare(context.Background(), left, right)
			require.NoError(t, err)
			assert.Equal(t, test.Cmp, cmp)
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

type PrepareJsonMutationValue = func(t *testing.T, doc, val, result interface{}) (types.MutableJSON, sql.JSONWrapper, types.MutableJSON)

func RunJsonMutationTests(ctx context.Context, t *testing.T, tests []JsonMutationTest, prepare PrepareJsonMutationValue, op string) {
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			doc, val, result := prepare(t, test.doc, test.value, test.resultVal)
			res, changed, err := func() (types.MutableJSON, bool, error) {
				switch op {
				case "set":
					return doc.Set(ctx, test.path, val)
				case "insert":
					return doc.Insert(ctx, test.path, val)
				case "remove":
					return doc.Remove(ctx, test.path)
				case "replace":
					return doc.Replace(ctx, test.path, val)
				case "arrayappend":
					return doc.ArrayAppend(ctx, test.path, val)
				case "arrayinsert":
					return doc.ArrayInsert(ctx, test.path, val)
				default:
					panic("unexpected operation for test")
				}
			}()
			require.NoError(t, err)
			expected, err := result.ToInterface(ctx)
			require.NoError(t, err)
			actual, err := res.ToInterface(ctx)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
			assert.Equal(t, test.changed, changed)
		})
	}
}
