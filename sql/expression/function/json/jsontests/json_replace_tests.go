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

	json2 "github.com/dolthub/go-mysql-server/sql/expression/function/json"

	"github.com/dolthub/go-mysql-server/sql"
)

func JsonReplaceTestCases(t *testing.T, prepare prepareJsonValue) []testCase {
	f1 := buildGetFieldExpressions(t, json2.NewJSONReplace, 3)
	f2 := buildGetFieldExpressions(t, json2.NewJSONReplace, 5)

	json := prepare(t, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`)

	return []testCase{
		{
			name:     "replace existing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.a", 10.1},
			expected: `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`},
		{
			name:     "replace non-existing does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.e", "new"},
			expected: json,
		},
		{
			name:     "replace nested",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c.d", "test"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "test"}}`,
		},
		{
			name:     "replace multiple, one change.",
			f:        f2,
			row:      sql.UntypedSqlRow{json, "$.a", 10.1, "$.e", "new"},
			expected: `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "replace nested non-existent does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.a.e", "test"},
			expected: json,
		},
		{
			name:     "replace nested in existing struct missing field does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c.e", "test"},
			expected: json,
		},
		{
			name:     "replace struct with indexing out of range",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.c[5]", 4.1},
			expected: json,
		},
		{
			name:     "replace element in array",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b[0]", 4.1},
			expected: `{"a": 1, "b": [4.1, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "replace element in array out of range does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b[5]", 4.1},
			expected: json,
		},
		{name: "replace nested in array does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.b.c", 4},
			expected: json,
		},
		{
			name:     "replace scalar when treated as array",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$.a[0]", 4.1},
			expected: `{"a": 4.1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "replace root element when treated as array",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$[0]", 4.1},
			expected: `4.1`,
		},
		{
			name: "improper struct indexing",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$.[0]", 4.1},
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
		}, //
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{json, "$.c.**", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**"),
		},
		{
			name: "invalid json type",
			f:    f1,
			row:  sql.UntypedSqlRow{1, "$[0]", 4.1},
			err:  sql.ErrInvalidJSONArgument.New(1, "json_replace"),
		},
		{
			name: "invalid json string",
			f:    f1,
			row:  sql.UntypedSqlRow{``, "$[0]", 4.1},
			err:  sql.ErrInvalidJSONText.New(1, "json_replace", ""),
		},
		{
			name:     "refer to root element with zero index on non-array",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$[0]", 4.1},
			expected: `4.1`,
		},
		{
			name:     "replace root element",
			f:        f1,
			row:      sql.UntypedSqlRow{json, "$", 10.1},
			expected: `10.1`,
		},
		{
			name:     "null document returns null",
			f:        f1,
			row:      sql.UntypedSqlRow{nil, "$", 42.7},
			expected: nil,
		},
		{
			name:     "if any path is null, return null",
			f:        f1,
			row:      sql.UntypedSqlRow{json, nil, 10},
			expected: nil,
		},
	}
}
