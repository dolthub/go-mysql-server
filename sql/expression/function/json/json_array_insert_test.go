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

func TestArrayInsert(t *testing.T) {
	_, err := NewJSONArrayInsert()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONArrayInsert, 3)
	f2 := buildGetFieldExpressions(t, NewJSONArrayInsert, 5)

	json := `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		// Manual testing on MySQL verifies these behaviors are consistent. It seems a little chaotic, but json_array_insert
		// logic is more consistent than other JSON functions. It requires a path ending with an index, and if it doesn't
		// find one, it reports an error about the path expression. If the object lookup does find an array, then it
		// inserts the value at the index. If the index is out of range, it inserts at the appropriate end of the array
		// (similar to other json mutating functions). Finally, if the object lookup finds a non-array, it's a no-op.
		{f1, sql.Row{json, "$.b[0]", 4.1}, `{"a": 1, "b": [4.1, 2, 3], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{json, "$.a", 2}, nil, fmt.Errorf("A path expression is not a path to a cell in an array at character 3 of $.a")},
		{f1, sql.Row{json, "$.e", "new"}, nil, fmt.Errorf("A path expression is not a path to a cell in an array at character 3 of $.e")},
		{f1, sql.Row{json, "$.c.d", "test"}, nil, fmt.Errorf("A path expression is not a path to a cell in an array at character 5 of $.c.d")},
		{f2, sql.Row{json, "$.b[0]", 4.1, "$.c.d", "test"}, nil, fmt.Errorf("A path expression is not a path to a cell in an array at character 5 of $.c.d")},
		{f1, sql.Row{json, "$.b[5]", 4.1}, `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`, nil},
		{f1, sql.Row{json, "$.b.c", 4}, nil, fmt.Errorf("A path expression is not a path to a cell in an array at character 5 of $.b.c")},
		{f1, sql.Row{json, "$.a[0]", 4.1}, json, nil},
		{f1, sql.Row{json, "$[0]", 4.1}, json, nil},
		{f1, sql.Row{json, "$.[0]", 4.1}, nil, fmt.Errorf("Invalid JSON path expression")},
		{f1, sql.Row{json, "foo", "test"}, nil, fmt.Errorf("Invalid JSON path expression")},
		{f1, sql.Row{json, "$.c.*", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},
		{f1, sql.Row{json, "$.c.**", "test"}, nil, fmt.Errorf("Path expressions may not contain the * and ** tokens")},
		{f1, sql.Row{json, "$", 10.1}, nil, fmt.Errorf("Path expression is not a path to a cell in an array: $")},
		{f1, sql.Row{nil, "$", 42.7}, nil, nil},
		{f1, sql.Row{json, nil, 10}, nil, nil},

		// mysql> select JSON_ARRAY_INSERT(JSON_ARRAY(1,2,3), "$[1]", 51, "$[1]", 52, "$[1]", 53);
		//+--------------------------------------------------------------------------+
		//| JSON_ARRAY_INSERT(JSON_ARRAY(1,2,3), "$[1]", 51, "$[1]", 52, "$[1]", 53) |
		//+--------------------------------------------------------------------------+
		//| [1, 53, 52, 51, 2, 3]                                                    |
		//+--------------------------------------------------------------------------+
		{buildGetFieldExpressions(t, NewJSONArrayInsert, 7),
			sql.Row{`[1.0,2.0,3.0]`,
				"$[1]", 51.0, // [1, 2, 3] -> [1, 51, 2, 3]
				"$[1]", 52.0, // [1, 51, 2, 3] -> [1, 52, 51, 2, 3]
				"$[1]", 53.0, // [1, 52, 51, 2, 3] -> [1, 53, 52, 51, 2, 3]
			},
			`[1,53,52,51,2,3]`, nil},
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
				req.Nil(result)
				req.Error(tstC.err, err)
			}
		})
	}

}
