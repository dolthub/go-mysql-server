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

package jsontests

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type prepareJsonValue = func(*testing.T, string) interface{}

type jsonFormatTest struct {
	name        string
	prepareFunc prepareJsonValue
}

var jsonFormatTests = []jsonFormatTest{
	{
		name: "string",
		prepareFunc: func(t *testing.T, js string) interface{} {
			return js
		},
	},
	{
		name: "JsonDocument",
		prepareFunc: func(t *testing.T, js string) interface{} {
			doc, _, err := types.JSON.Convert(js)
			require.NoError(t, err)
			val, err := doc.(sql.JSONWrapper).ToInterface()
			require.NoError(t, err)
			return types.JSONDocument{Val: val}
		},
	},
	{
		name: "LazyJsonDocument",
		prepareFunc: func(t *testing.T, js string) interface{} {
			doc, _, err := types.JSON.Convert(js)
			require.NoError(t, err)
			bytes, err := types.MarshallJson(doc.(sql.JSONWrapper))
			require.NoError(t, err)
			return types.NewLazyJSONDocument(bytes)
		},
	},
}

type testCase struct {
	f        sql.Expression
	row      sql.Row
	expected interface{}
	err      error
}

func buildGetFieldExpressions(t *testing.T, construct func(...sql.Expression) (sql.Expression, error), argCount int) sql.Expression {
	expressions := make([]sql.Expression, 0, argCount)
	for i := 0; i < argCount; i++ {
		expressions = append(expressions, expression.NewGetField(i, types.LongText, "arg"+strconv.Itoa(i), false))
	}

	result, err := construct(expressions...)
	require.NoError(t, err)

	return result
}

func JsonInsertTestCases(t *testing.T, prepare prepareJsonValue) []testCase {

	jsonInput := prepare(t, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`)

	f1 := buildGetFieldExpressions(t, json.NewJSONInsert, 3)
	f2 := buildGetFieldExpressions(t, json.NewJSONInsert, 5)

	return []testCase{
		{f1, sql.Row{jsonInput, "$.A", 10.1}, `{"A": 10.1, "a": 1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                                                   // insert at beginning of top-level object
		{f1, sql.Row{jsonInput, "$.z", 10.1}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}, "z": 10.1}`, nil},                                                   // insert at end of top-level object
		{f1, sql.Row{jsonInput, "$.bb", 10.1}, `{"a": 1, "b": [2, 3], "bb": 10.1, "c": {"d": "foo"}}`, nil},                                                 // insert in middle of top-level object
		{f1, sql.Row{jsonInput, "$.bb.cc", 10.1}, jsonInput, nil},                                                                                           // insert to non-existent path is a no-op
		{f1, sql.Row{jsonInput, "$.a", 10.1}, jsonInput, nil},                                                                                               // insert existing does nothing
		{f1, sql.Row{jsonInput, "$.c.d", "test"}, jsonInput, nil},                                                                                           // insert existing nested does nothing
		{f2, sql.Row{jsonInput, "$.a", 10.1, "$.e", "new"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},                                      // insert multiple, one change.
		{f1, sql.Row{jsonInput, "$.a.e", "test"}, jsonInput, nil},                                                                                           // insert nested does nothing
		{f1, sql.Row{jsonInput, "$.c.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`, nil},                                               // insert nested in existing struct
		{f1, sql.Row{jsonInput, "$.c[5]", 4.1}, `{"a": 1, "b": [2, 3], "c": [{"d": "foo"}, 4.1]}`, nil},                                                     // insert struct with indexing out of range
		{f1, sql.Row{jsonInput, "$.b[0]", 4.1}, jsonInput, nil},                                                                                             // insert element in array does nothing
		{f1, sql.Row{jsonInput, "$.b[5]", 4.1}, `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`, nil},                                                       // insert element in array out of range
		{f1, sql.Row{jsonInput, "$.b.c", 4}, jsonInput, nil},                                                                                                // insert nested in array does nothing
		{f1, sql.Row{jsonInput, "$.a[0]", 4.1}, jsonInput, nil},                                                                                             // struct as array does nothing
		{f1, sql.Row{jsonInput, "$[0]", 4.1}, jsonInput, nil},                                                                                               // struct does nothing.
		{f1, sql.Row{jsonInput, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 2 of $.[0]")},      // improper struct indexing
		{f1, sql.Row{jsonInput, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Path must start with '$'")},                                  // invalid path
		{f1, sql.Row{jsonInput, "$.c.*", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.*")},   // path contains * wildcard
		{f1, sql.Row{jsonInput, "$.c.**", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**")}, // path contains ** wildcard
		{f1, sql.Row{1, "$.c.**", "test"}, nil, sql.ErrInvalidJSONArgument.New(1, "json_insert")},                                                           // path contains ** wildcard
		{f1, sql.Row{`()`, "$.c.**", "test"}, nil, sql.ErrInvalidJSONText.New(1, "json_insert", "()")},                                                      // path contains ** wildcard
		{f1, sql.Row{jsonInput, "$", 10.1}, jsonInput, nil},                                                                                                 // whole document no opt
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},                                                                                                             // sql-null document returns sql-null
		{f1, sql.Row{"null", "$", 42.7}, "null", nil},                                                                                                       // json-null document returns json-null
		{f1, sql.Row{jsonInput, nil, 10}, nil, nil},                                                                                                         // if any path is null, return null

		// mysql> select JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4);
		// +------------------------------------------------------------------------+
		// | JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4) |
		// +------------------------------------------------------------------------+
		// | [1, 2, 3]                                                              |
		// +------------------------------------------------------------------------+
		{buildGetFieldExpressions(t, json.NewJSONInsert, 9),
			sql.Row{`[]`,
				"$[2]", 1.1, // [] -> [1.1]
				"$[2]", 2.2, // [1.1] -> [1.1,2.2]
				"$[2]", 3.3, // [1.1, 2.2] -> [1.1, 2.2, 3.3]
				"$[2]", 4.4}, // [1.1, 2.2, 3.3] -> [1.1, 2.2, 3.3]
			`[1.1, 2.2, 3.3]`, nil},
	}
}

func RunJsonTests(t *testing.T, testCases []testCase) {
	for _, tstC := range testCases {
		var paths []string
		for _, path := range tstC.row[1:] {
			if _, ok := path.(string); ok {
				paths = append(paths, path.(string))
			}
		}

		t.Run(tstC.f.String()+"."+strings.Join(paths, ","), func(t *testing.T) {
			req := require.New(t)
			result, err := tstC.f.Eval(sql.NewEmptyContext(), tstC.row)
			if tstC.err == nil {
				req.NoError(err)

				var expect interface{}
				if tstC.expected != nil {
					expect, _, err = types.JSON.Convert(tstC.expected)
					if err != nil {
						panic("Bad test string. Can't convert string to JSONDocument: " + tstC.expected.(string))
					}
				}

				cmp, err := types.JSON.Compare(expect, result)
				req.NoError(err)
				if cmp != 0 {
					t.Error("Not equal:")
					t.Errorf("expected: %v", expect)
					t.Errorf("actual: %v", result)
					t.Fail()
				}
			} else {
				req.Error(err, "Expected an error but got %v", result)
				req.Equal(tstC.err.Error(), err.Error())
			}
		})
	}
}
