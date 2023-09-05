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

func TestInsert(t *testing.T) {
	_, err := NewJSONInsert()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONInsert, 3)
	f2 := buildGetFieldExpressions(t, NewJSONInsert, 5)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f1, sql.Row{json, "$.a", 10.1}, json, nil},                                                                    // insert existing does nothing
		{f1, sql.Row{json, "$.e", "new"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},                   // insert new
		{f1, sql.Row{json, "$.c.d", "test"}, json, nil},                                                                // insert existing nested does nothing
		{f2, sql.Row{json, "$.a", 10.1, "$.e", "new"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`, nil},      // insert multiple, one change.
		{f1, sql.Row{json, "$.a.e", "test"}, json, nil},                                                                // insert nested does nothing
		{f1, sql.Row{json, "$.c.e", "test"}, `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`, nil},               // insert nested in existing struct
		{f1, sql.Row{json, "$.c[5]", 4.1}, `{"a": 1, "b": [2, 3], "c": [{"d": "foo"}, 4.1]}`, nil},                     // insert struct with indexing out of range
		{f1, sql.Row{json, "$.b[0]", 4.1}, json, nil},                                                                  // insert element in array does nothing
		{f1, sql.Row{json, "$.b[5]", 4.1}, `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`, nil},                       // insert element in array out of range
		{f1, sql.Row{json, "$.b.c", 4}, json, nil},                                                                     // insert nested in array does nothing
		{f1, sql.Row{json, "$.a[0]", 4.1}, json, nil},                                                                  // struct as array does nothing
		{f1, sql.Row{json, "$[0]", 4.1}, json, nil},                                                                    // struct does nothing.
		{f1, sql.Row{json, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression")},                             // improper struct indexing
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},                            // invalid path
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},  // path contains * wildcard
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")}, // path contains ** wildcard
		{f1, sql.Row{json, "$", 10.1}, json, nil},                                                                      // whole document no opt
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},                                                                        // null document returns null
		{f1, sql.Row{json, nil, 10}, nil, nil},                                                                         // if any path is null, return null

		// mysql> select JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4);
		// +------------------------------------------------------------------------+
		// | JSON_INSERT(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4) |
		// +------------------------------------------------------------------------+
		// | [1, 2, 3]                                                              |
		// +------------------------------------------------------------------------+
		{buildGetFieldExpressions(t, NewJSONInsert, 9),
			sql.Row{`[]`,
				"$[2]", 1.1, // [] -> [1.1]
				"$[2]", 2.2, // [1.1] -> [1.1,2.2]
				"$[2]", 3.3, // [1.1, 2.2] -> [1.1, 2.2, 3.3]
				"$[2]", 4.4}, // [1.1, 2.2, 3.3] -> [1.1, 2.2, 3.3]
			`[1.1, 2.2, 3.3]`, nil},
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
