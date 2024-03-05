// Copyright 2022-2024 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONMergePreserve(t *testing.T) {
	f2 := buildGetFieldExpressions(t, NewJSONMergePreserve, 2)
	f3 := buildGetFieldExpressions(t, NewJSONMergePreserve, 3)
	f4 := buildGetFieldExpressions(t, NewJSONMergePreserve, 4)

	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err bool
	}{
		{
			f:   f2,
			row: sql.Row{nil, nil},
			exp: nil,
		},
		{
			f:   f2,
			row: sql.Row{`null`, `null`},
			exp: types.MustJSON(`[null, null]`),
		},
		{
			f:   f2,
			row: sql.Row{`1`, `true`},
			exp: types.MustJSON(`[1, true]`),
		},
		{
			f:   f2,
			row: sql.Row{`"abc"`, `"def"`},
			exp: types.MustJSON(`["abc", "def"]`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `null`},
			exp: types.MustJSON(`[1, 2, null]`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `{"id": 47}`},
			exp: types.MustJSON(`[1, 2, {"id": 47}]`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `[true, false]`},
			exp: types.MustJSON(`[1, 2, true, false]`),
		},
		{
			f:   f2,
			row: sql.Row{`{"name": "x"}`, `{"id": 47}`},
			exp: types.MustJSON(`{"id": 47, "name": "x"}`),
		},
		{
			f:   f2,
			row: sql.Row{
				`{
					"Suspect": {
						"Name": "Bart",
						"Hobbies": ["Skateboarding", "Mischief"]
					},
					"Victim": "Lisa",
					"Case": {
						"Id": 33845,
						"Date": "2006-01-02T15:04:05-07:00",
						"Closed": true
					}
				}`,
				`{
					"Suspect": {
						"Age": 10,
						"Parents": ["Marge", "Homer"],
						"Hobbies": ["Trouble"]
					},
					"Witnesses": ["Maggie", "Ned"]
				}`,
			},
			exp: types.MustJSON(
				`{
					"Case": {
						"Id": 33845, 
						"Date": "2006-01-02T15:04:05-07:00", 
						"Closed": true
					}, 
					"Victim": "Lisa", 
					"Suspect": {
						"Name": "Bart",
						"Age": 10,
						"Hobbies": ["Skateboarding", "Mischief", "Trouble"], 
						"Parents": ["Marge", "Homer"]
					}, 
					"Witnesses": ["Maggie", "Ned"]
			}`),
		},
		{
			f:   f3,
			row: sql.Row{
				`{"a": 1, "b": 2}`,
				`{"a": 3, "c": 4}`,
				`{"a": 5, "d": 6}`,
			},
			exp: types.MustJSON(`{"a": [1, 3, 5], "b": 2, "c": 4, "d": 6}`),
		},
		{
			f:   f4,
			row: sql.Row{
				`{"a": 1, "b": 2}`,
				`{"a": 3, "c": 4}`,
				`{"a": 5, "d": 6}`,
				`{"a": 3, "e": 8}`,
			},
			exp: types.MustJSON(`{"a": [1, 3, 5, 3], "b": 2, "c": 4, "d": 6, "e": 8}`),
		},
		{
			f:   f3,
			row: sql.Row{`{"a": 1, "b": 2}`, `{"a": {"one": false, "two": 2.55, "e": 8}}`, `"single value"`},
			exp: types.MustJSON(`[{"a": [1, {"e": 8, "one": false, "two": 2.55}], "b": 2}, "single value"]`),
		},
	}

	for _, tt := range testCases {
		var args []string
		for _, a := range tt.row {
			args = append(args, fmt.Sprintf("%v", a))
		}
		t.Run(strings.Join(args, ", "), func(t *testing.T) {
			require := require.New(t)
			res, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tt.exp, res)
		})
	}
}
