// Copyright 2023 Dolthub, Inc.
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
	json2 "encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONSet(t *testing.T) {
	_, err := NewJSONSet()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONSet, 3)

	f2 := buildGetFieldExpressions(t, NewJSONSet, 5)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f1, sql.Row{json, "$.a", 10.1}, `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                                                           // update existing
		{f1, sql.Row{json, "$.e", "new"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},                                                   // set new
		{f1, sql.Row{json, "$.c.d", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "test"}}`, nil},                                                         // update existing nested
		{f2, sql.Row{json, "$.a", 10.1, "$.e", "new"}, `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},                                   // update existing and set new
		{f1, sql.Row{json, "$.a.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                                                          // set new nested does nothing
		{f1, sql.Row{json, "$.c.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`, nil},                                               // set new nested in existing struct
		{f1, sql.Row{json, "$.c[5]", 4.1}, `{"a": 1, "b": [2, 3], "c": [{"d": "foo"}, 4.1]}`, nil},                                                     // update struct with indexing out of range
		{f1, sql.Row{json, "$.b[0]", 4.1}, `{"a": 1, "b": [4.1, 3], "c": {"d": "foo"}}`, nil},                                                          // update element in array
		{f1, sql.Row{json, "$.b[5]", 4.1}, `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`, nil},                                                       // update element in array out of range
		{f1, sql.Row{json, "$.b.c", 4}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                                                               // set nested in array does nothing
		{f1, sql.Row{json, "$.a[0]", 4.1}, `{"a": 4.1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                                                          // update single element with indexing
		{f1, sql.Row{json, "$[0]", 4.1}, `4.1`, nil},                                                                                                   // struct indexing
		{f1, sql.Row{json, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression")},                                                             // improper struct indexing
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                                                            // invalid path
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},                                  // path contains * wildcard
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},                                 // path contains ** wildcard
		{f1, sql.Row{json, "$", 10.1}, `10.1`, nil},                                                                                                    // whole document
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},                                                                                                        // null document
		{f1, sql.Row{json, nil, 10}, nil, nil},                                                                                                         // if any path is null, return null
		{f2, sql.Row{json, "$.z", map[string]interface{}{"zz": 1.1}, "$.z.zz", 42.1}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"z":{"zz":42.1}}`, nil}, // accumulates L->R

		// mysql> select JSON_SET(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4);
		// +---------------------------------------------------------------------+
		// | JSON_SET(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4) |
		// +---------------------------------------------------------------------+
		// | [1, 2, 4]                                                           |
		// +---------------------------------------------------------------------+
		{buildGetFieldExpressions(t, NewJSONSet, 9),
			sql.Row{`[]`,
				"$[2]", 1.1, // [] -> [1.1]
				"$[2]", 2.2, // [1.1] -> [1.1,2.2]
				"$[2]", 3.3, // [1.1, 2.2] -> [1.1, 2.2, 3.3]
				"$[2]", 4.4}, // [1.1, 2.2, 3.3] -> [1.1, 2.2, 4.4]
			`[1.1, 2.2, 4.4]`, nil},
	}

	for _, tt := range testCases {
		var paths []string
		for _, path := range tt.row[1:] {
			if _, ok := path.(string); ok {
				paths = append(paths, path.(string))
			} else {
				if path == nil {
					paths = append(paths, "null")
				} else if _, ok := path.(int); ok {
					paths = append(paths, strconv.Itoa(path.(int)))
				} else {
					m, _ := json2.Marshal(path)
					paths = append(paths, string(m))
				}
			}
		}

		t.Run(tt.f.String()+"."+strings.Join(paths, ","), func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err == nil {
				require.NoError(err)

				var expect interface{}
				if tt.expected != nil {
					expect, _, err = types.JSON.Convert(tt.expected)
					if err != nil {
						panic("Bad test string. Can't convert string to JSONDocument: " + tt.expected.(string))
					}
				}

				require.Equal(expect, result)
			} else {
				require.Error(tt.err, err)
			}
		})
	}
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
