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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestReplace(t *testing.T) {

	_, err := NewJSONReplace()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONReplace, 3)
	f2 := buildGetFieldExpressions(t, NewJSONReplace, 5)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f1, sql.Row{json, "$.a", 10.1}, `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                           // replace existing
		{f1, sql.Row{json, "$.e", "new"}, json, nil},                                                                   // replace non-existing does nothing
		{f1, sql.Row{json, "$.c.d", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "test"}}`, nil},                         // replace nested
		{f2, sql.Row{json, "$.a", 10.1, "$.e", "new"}, `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`, nil},             // replace multiple, one change.
		{f1, sql.Row{json, "$.a.e", "test"}, json, nil},                                                                // replace nested non-existent does nothing
		{f1, sql.Row{json, "$.c.e", "test"}, json, nil},                                                                // replace nested in existing struct missing field does nothing
		{f1, sql.Row{json, "$.c[5]", 4.1}, json, nil},                                                                  // replace struct with indexing out of range
		{f1, sql.Row{json, "$.b[0]", 4.1}, `{"a": 1, "b": [4.1, 3], "c": {"d": "foo"}}`, nil},                          // replace element in array
		{f1, sql.Row{json, "$.b[5]", 4.1}, json, nil},                                                                  // replace element in array out of range does nothing
		{f1, sql.Row{json, "$.b.c", 4}, json, nil},                                                                     // replace nested in array does nothing
		{f1, sql.Row{json, "$.a[0]", 4.1}, `{"a": 4.1, "b": [2, 3], "c": {"d": "foo"}}`, nil},                          // replace scalar when treated as array
		{f1, sql.Row{json, "$[0]", 4.1}, `4.1`, nil},                                                                   // replace root element when treated as array
		{f1, sql.Row{json, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression")},                             // improper struct indexing
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                            // invalid path
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},  // path contains * wildcard
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")}, // path contains ** wildcard
		{f1, sql.Row{json, "$", 10.1}, `10.1`, nil},                                                                    // replace root element
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},                                                                        // null document returns null
		{f1, sql.Row{json, nil, 10}, nil, nil},                                                                         // if any path is null, return null
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
					expect, _, err = types.JSON.Convert(tstC.expected)
					if err != nil {
						panic("Bad test string. Can't convert string to JSONDocument: " + tstC.expected.(string))
					}
				}

				req.Equal(expect, result)
			} else {
				req.Error(tstC.err, err)
			}
		})
	}
}
