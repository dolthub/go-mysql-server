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

package function

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

	f1, err := NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f2, err := NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
		expression.NewGetField(4, types.LongText, "arg5", false),
	)
	require.NoError(t, err)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f1, sql.Row{json, "$.a", 10}, `{"a": 10, "b": [2, 3], "c": {"d": "foo"}}`, nil},                         // update existing
		{f1, sql.Row{json, "$.e", "new"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},             // set new
		{f1, sql.Row{json, "$.c.d", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "test"}}`, nil},                   // update existing nested
		{f2, sql.Row{json, "$.a", 10, "$.e", "new"}, `{"a": 10, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil}, // update existing and set new
		{f1, sql.Row{json, "$.a.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                    // set new nested does nothing
		{f1, sql.Row{json, "$.c.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`, nil},         // set new nested in existing struct
		// {f1, sql.Row{json, "$.c[5]", 4}, `{"a": [1, 4], "b": [2, 3], "c": {"d": "foo"}}`, nil},                   // update struct with indexing out of range
		{f1, sql.Row{json, "$.b[0]", 4}, `{"a": 1, "b": [4, 3], "c": {"d": "foo"}}`, nil},   // update element in array
		{f1, sql.Row{json, "$.b[5]", 4}, `{"a": 1, "b": [2, 3,4], "c": {"d": "foo"}}`, nil}, // update element in array out of range
		{f1, sql.Row{json, "$.b.c", 4}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, nil},    // set nested in array does nothing
		{f1, sql.Row{json, "$.a[0]", 4}, `{"a": 4, "b": [2, 3], "c": {"d": "foo"}}`, nil},   // update single element with indexing
		// {f1, sql.Row{json, "$.a[5]", 4}, `{"a": [1, 4] , "b": [2, 3], "c": {"d": "foo"}}`, nil}, // update single element with indexing out of range
		// {f1, sql.Row{json, "$[0]", 4}, `4`, nil}, // struct indexing
		// {f1, sql.Row{json, "$[0][1]", 4}, `[{"a": 1, "b": [2, 3], "c": {"d": "foo"}}, 4]`, nil},   // nested struct indexing
		{f1, sql.Row{json, "$.[0]", 4}, nil, fmt.Errorf("Invalid JSON path expression")},                                                         // improper struct indexing
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                                                      // invalid path
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},                            // path contains * wildcard
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},                           // path contains ** wildcard
		{f1, sql.Row{nil, "$.a", 10}, nil, nil},                                                                                                  // null document
		{f1, sql.Row{json, nil, 10}, nil, nil},                                                                                                   // if any path is null, return null
		{f2, sql.Row{json, "$.z", map[string]interface{}{"zz": 1}, "$.z.zz", 42}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"z":{"zz":42}}`, nil}, // accumulates L->R
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
			} else {
				require.Error(tt.err, err)
			}

			require.Equal(tt.expected, result)
		})
	}
}
