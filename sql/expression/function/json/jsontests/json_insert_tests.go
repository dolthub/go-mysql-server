// Copyright 2020-2024 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
)

func JsonInsertTestCases(t *testing.T, prepare prepareJsonValue) []testCase {

	jsonInput := prepare(t, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`)

	f1 := buildGetFieldExpressions(t, json.NewJSONInsert, 3)
	f2 := buildGetFieldExpressions(t, json.NewJSONInsert, 5)

	return []testCase{
		{
			name:     "insert into beginning of top-level object",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.A", 10.1},
			expected: `{"A": 10.1, "a": 1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "insert at end of top-level object",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.z", 10.1},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"}, "z": 10.1}`,
		},
		{
			name:     "insert in middle of top-level object",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.bb", 10.1},
			expected: `{"a": 1, "b": [2, 3], "bb": 10.1, "c": {"d": "foo"}}`,
		},
		{
			name:     "insert to non-existent path is a no-op",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.bb.cc", 10.1},
			expected: jsonInput,
		},
		{
			name:     "insert existing does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a", 10.1},
			expected: jsonInput,
		},
		{
			name:     "insert existing nested does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c.d", "test"},
			expected: jsonInput,
		},
		{
			name:     "insert multiple, one change",
			f:        f2,
			row:      sql.UntypedSqlRow{jsonInput, "$.a", 10.1, "$.e", "new"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`,
		},
		{
			name:     "insert nested does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a.e", "test"},
			expected: jsonInput,
		},
		{
			name:     "insert nested in existing struct",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c.e", "test"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`,
		},
		{
			name:     "insert struct with indexing out of range",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c[5]", 4.1},
			expected: `{"a": 1, "b": [2, 3], "c": [{"d": "foo"}, 4.1]}`},
		{
			name:     "insert element in array does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b[0]", 4.1},
			expected: jsonInput,
		},
		{
			name:     "insert element in array out of range",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b[5]", 4.1},
			expected: `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`,
		},
		{
			name:     "insert nested in array does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b.c", 4},
			expected: jsonInput,
		},
		{
			name:     "struct as array does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a[0]", 4.1},
			expected: jsonInput,
		},
		{
			name:     "struct does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$[0]", 4.1},
			expected: jsonInput,
		},
		{
			name: "improper struct indexing",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.[0]", 4.1},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 2 of $.[0]"),
		},
		{
			name: "invalid path",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "foo", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Path must start with '$'"),
		},
		{
			name: "path contains * wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.c.*", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.*"),
		},
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.c.**", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**"),
		},
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{1, "$.c.**", "test"},
			err:  sql.ErrInvalidJSONArgument.New(1, "json_insert"),
		},
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{`()`, "$.c.**", "test"},
			err:  sql.ErrInvalidJSONText.New(1, "json_insert", "()"),
		},
		{
			name:     "whole document no opt",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$", 10.1},
			expected: jsonInput,
		},
		{
			name:     "sql-null document returns sql-null",
			f:        f1,
			row:      sql.UntypedSqlRow{nil, "$", 42.7},
			expected: nil,
		},
		{
			name:     "json-null document returns json-null",
			f:        f1,
			row:      sql.UntypedSqlRow{"null", "$", 42.7},
			expected: "null",
		},
		{
			name: "if any path is null, return null",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, nil, 10},
		},

		// mysql> select JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4);
		// +------------------------------------------------------------------------+
		// | JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4) |
		// +------------------------------------------------------------------------+
		// | [1, 2, 3]                                                              |
		// +------------------------------------------------------------------------+
		{f: buildGetFieldExpressions(t, json.NewJSONInsert, 9),
			row: sql.UntypedSqlRow{`[]`,
				"$[2]", 1.1, // [] -> [1.1]
				"$[2]", 2.2, // [1.1] -> [1.1,2.2]
				"$[2]", 3.3, // [1.1, 2.2] -> [1.1, 2.2, 3.3]
				"$[2]", 4.4}, // [1.1, 2.2, 3.3] -> [1.1, 2.2, 3.3]
			expected: `[1.1, 2.2, 3.3]`},
	}
}
