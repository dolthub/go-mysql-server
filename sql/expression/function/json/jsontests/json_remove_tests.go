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
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression/function/json"

	"github.com/dolthub/go-mysql-server/sql"
)

func JsonRemoveTestCases(t *testing.T, prepare prepareJsonValue) []testCase {

	f1 := buildGetFieldExpressions(t, json.NewJSONRemove, 2)

	f2 := buildGetFieldExpressions(t, json.NewJSONRemove, 3)

	json := prepare(t, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`)

	return []testCase{

		{
			name:     "remove only element",
			f:        f1,
			row:      sql.UntypedSqlRow{prepare(t, `{"a": 1}`), "$.a"},
			expected: prepare(t, `{}`),
		},
		{
			name:     "remove existing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.a"},
			expected: prepare(t, `{"b": [2, 3], "c": {"d": "foo"}}`),
		},
		{
			name:     "remove existing array element",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b[0]"},
			expected: `{"a": 1, "b": [3], "c": {"d": "foo"}}`,
		},
		{
			name:     "remove existing nested",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c.d"},
			expected: `{"a": 1, "b": [2, 3], "c": {}}`,
		},
		{
			name:     "remove existing object",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c"},
			expected: `{"a": 1, "b": [2, 3]}`,
		},
		{
			name:     "remove nothing when path not found",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.a.e"},
			expected: json,
		},
		{
			name:     "remove nothing when path not found",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c[5]"},
			expected: json,
		},
		{
			name:     "remove last element in array",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b[last]"},
			expected: `{"a": 1, "b": [2], "c": {"d": "foo"}}`,
		},
		{
			name:     "remove nothing when array index out of bounds",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b[5]"},
			expected: json,
		},
		{
			name:     "remove nothing when provided a bogus path",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$[0]"},
			expected: json,
		},
		{
			name: "improper struct indexing",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$.[0]"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 2 of $.[0]"),
		},
		{
			name: "invalid path",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "foo", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Path must start with '$'"),
		},
		{
			name: "path contains * wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$.c.*", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.*"),
		},
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$.c.**", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**"),
		},
		{
			name: "whole document",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$"},
			err:  fmt.Errorf("The path expression '$' is not allowed in this context."),
		},
		{
			name: "invalid json type",
			f:    f1,
			row:  sql.UntypedSqlRow{1, "$"},
			err:  sql.ErrInvalidJSONArgument.New(1, "json_remove"),
		},
		{
			name: "invalid json text",
			f:    f1,
			row:  sql.UntypedSqlRow{"}{", "$"},
			err:  sql.ErrInvalidJSONText.New(1, "json_remove", "}{"),
		},
		{
			name: "null document",
			f:    f1,
			row:  sql.UntypedSqlRow{nil, "$"},
		},
		{
			name:     "if any path is null, return null",
			f:        f2,
			row:      sql.UntypedSqlRow{json, "$.foo", nil},
			expected: nil,
		},
		{
			name:     "remove multiple paths",
			f:        f2,
			row:      sql.UntypedSqlRow{json, "$.a", "$.b"},
			expected: `{"c": {"d": "foo"}}`,
		},
	}

}
