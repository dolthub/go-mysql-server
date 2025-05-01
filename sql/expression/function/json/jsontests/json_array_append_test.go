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

package jsontests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestArrayAppend(t *testing.T) {
	_, err := json.NewJSONArrayInsert()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, json.NewJSONArrayAppend, 3)
	f2 := buildGetFieldExpressions(t, json.NewJSONArrayAppend, 5)

	jsonInput := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{

		{f1, sql.Row{jsonInput, "$.b[0]", 4.1}, `{"a": 1, "b": [[2,4.1], 3], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{jsonInput, "$.a", 4.1}, `{"a": [1, 4.1], "b": [2, 3], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{jsonInput, "$.e", "new"}, jsonInput, nil},
		{f1, sql.Row{jsonInput, "$.c.d", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": ["foo", "test"]}}`, nil},
		{f2, sql.Row{jsonInput, "$.b[0]", 4.1, "$.c.d", "test"}, `{"a": 1, "b": [[2, 4.1], 3], "c": {"d": ["foo", "test"]}}`, nil},
		{f1, sql.Row{jsonInput, "$.b[5]", 4.1}, jsonInput, nil},
		{f1, sql.Row{jsonInput, "$.b.c", 4}, jsonInput, nil},
		{f1, sql.Row{jsonInput, "$.a[51]", 4.1}, jsonInput, nil},
		{f1, sql.Row{jsonInput, "$.a[last-1]", 4.1}, jsonInput, nil},
		{f1, sql.Row{jsonInput, "$.a[0]", 4.1}, `{"a": [1, 4.1], "b": [2, 3], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{jsonInput, "$.a[last]", 4.1}, `{"a": [1, 4.1], "b": [2, 3], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{jsonInput, "$[0]", 4.1}, `[{"a": 1, "b": [2, 3], "c": {"d": "foo"}}, 4.1]`, nil},
		{f1, sql.Row{jsonInput, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 2 of $.[0]")},
		{f1, sql.Row{jsonInput, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Path must start with '$'")},
		{f1, sql.Row{jsonInput, "$.c.*", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.*")},
		{f1, sql.Row{jsonInput, "$.c.**", "test"}, nil, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**")},
		{f1, sql.Row{1, "$", "test"}, nil, sql.ErrInvalidJSONArgument.New(1, "json_array_append")},
		{f1, sql.Row{`}`, "$", "test"}, nil, sql.ErrInvalidJSONText.New(1, "json_array_append", `}`)},
		{f1, sql.Row{jsonInput, "$", 10.1}, `[{"a": 1, "b": [2, 3], "c": {"d": "foo"}}, 10.1]`, nil},
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},
		{f1, sql.Row{jsonInput, nil, 10}, nil, nil},

		// mysql> select JSON_ARRAY_APPEND(JSON_ARRAY(1,2,3), "$[1]", 51, "$[1]", 52, "$[1]", 53);
		// +--------------------------------------------------------------------------+
		// | JSON_ARRAY_APPEND(JSON_ARRAY(1,2,3), "$[1]", 51, "$[1]", 52, "$[1]", 53) |
		// +--------------------------------------------------------------------------+
		// | [1, [2, 51, 52, 53], 3]                                                  |
		// +--------------------------------------------------------------------------+
		{buildGetFieldExpressions(t, json.NewJSONArrayAppend, 7),
			sql.Row{`[1.0,2.0,3.0]`,
				"$[1]", 51.0, // [1, 2, 3] -> [1, [2, 51], 3]
				"$[1]", 52.0, // [1, [2, 51], 2, 3] -> [1, [2, 51, 52] 3]
				"$[1]", 53.0, // [1, [2, 51, 52], 3] -> [1, [2, 51, 52, 53], 3]
			},
			`[1,[2, 51, 52, 53], 3]`, nil},
	}

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
					expect, _, err = types.JSON.Convert(sqlCtx, tstC.expected)
					if err != nil {
						panic("Bad test string. Can't convert string to JSONDocument: " + tstC.expected.(string))
					}
				}

				req.Equal(expect, result)
			} else {
				req.Nil(result)
				if tstC.err == nil {
					req.NoError(err)
				} else {
					req.Error(err)
					req.Equal(tstC.err.Error(), err.Error())
				}
			}
		})
	}

}
