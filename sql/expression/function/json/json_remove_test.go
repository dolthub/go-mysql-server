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

func TestRemove(t *testing.T) {
	_, err := NewJSONRemove()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONRemove, 2)

	f2 := buildGetFieldExpressions(t, NewJSONRemove, 3)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f1, sql.Row{json, "$.a"}, `{"b": [2, 3], "c": {"d": "foo"}}`, nil},                                            // remove existing
		{f1, sql.Row{json, "$.b[0]"}, `{"a": 1, "b": [3], "c": {"d": "foo"}}`, nil},                                    // remove existing array element
		{f1, sql.Row{json, "$.c.d"}, `{"a": 1, "b": [2, 3], "c": {}}`, nil},                                            // remove existing nested
		{f1, sql.Row{json, "$.c"}, `{"a": 1, "b": [2, 3]}`, nil},                                                       // remove existing object
		{f1, sql.Row{json, "$.a.e"}, json, nil},                                                                        // remove nothing when path not found
		{f1, sql.Row{json, "$.c[5]"}, json, nil},                                                                       // remove nothing when path not found
		{f1, sql.Row{json, "$.b[last]"}, `{"a": 1, "b": [2], "c": {"d": "foo"}}`, nil},                                 // remove last element in array
		{f1, sql.Row{json, "$.b[5]"}, json, nil},                                                                       // remove nothing when array index out of bounds
		{f1, sql.Row{json, "$[0]"}, json, nil},                                                                         // remove nothing when provided a bogus path.
		{f1, sql.Row{json, "$.[0]"}, nil, fmt.Errorf("Invalid JSON path expression")},                                  // improper struct indexing
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                            // invalid path
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},  // path contains * wildcard
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")}, // path contains ** wildcard
		{f1, sql.Row{json, "$"}, nil, fmt.Errorf("The path expression '$' is not allowed in this context.")},           // whole document
		{f1, sql.Row{nil, "$"}, nil, nil},                                                                              // null document
		{f2, sql.Row{json, "$.foo", nil}, nil, nil},                                                                    // if any path is null, return null
		{f2, sql.Row{json, "$.a", "$.b"}, `{"c": {"d": "foo"}}`, nil},                                                  // remove multiple paths
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
