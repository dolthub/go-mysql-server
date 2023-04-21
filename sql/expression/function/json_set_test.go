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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONSet(t *testing.T) {
	_, err := NewJSONSet()
	require.Error(t, err)

	_, err = NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.Error(t, err)

	_, err = NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.Error(t, err)

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
		{f1, sql.Row{json, "$.b[0]", 4}, `{"a": 1, "b": [4, 3], "c": {"d": "foo"}}`, nil},                        // update element in array
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                      // invalid path
		{f1, sql.Row{nil, "$.a", 10}, nil, nil},                                                                  // null document
	}

	for _, tt := range testCases {
		var paths []string
		for _, path := range tt.row[1:] {
			if _, ok := path.(string); ok {
				paths = append(paths, path.(string))
			} else {
				paths = append(paths, strconv.Itoa(path.(int)))
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
