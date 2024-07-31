// Copyright 2024 Dolthub, Inc.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONMergePatch(t *testing.T) {
	f2 := buildGetFieldExpressions(t, json.NewJSONMergePatch, 2)
	f3 := buildGetFieldExpressions(t, json.NewJSONMergePatch, 3)
	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err error
	}{
		{
			f:   f2,
			row: sql.Row{nil, nil},
			exp: nil,
		},
		{
			f:   f2,
			row: sql.Row{`null`, `null`},
			exp: types.MustJSON(`null`),
		},
		{
			f:   f2,
			row: sql.Row{`1`, `true`},
			exp: types.MustJSON(`true`),
		},
		{
			f:   f2,
			row: sql.Row{`"abc"`, `"def"`},
			exp: types.MustJSON(`"def"`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `null`},
			exp: types.MustJSON(`null`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `{"id": 47}`},
			exp: types.MustJSON(`{"id": 47}`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2]`, `[true, false]`},
			exp: types.MustJSON(`[true, false]`),
		},
		{
			f:   f2,
			row: sql.Row{`{"name": "x"}`, `{"id": 47}`},
			exp: types.MustJSON(`{"id": 47, "name": "x"}`),
		},
		{
			f:   f2,
			row: sql.Row{`{"id": 123}`, `{"id": null}`},
			exp: types.MustJSON(`{}`),
		},
		{
			f: f2,
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
						"Age": 10, 
						"Name": "Bart", 
						"Hobbies": ["Trouble"], 
						"Parents": ["Marge", "Homer"]
					}, 
					"Witnesses": ["Maggie", "Ned"]
			}`),
		},
		{
			f:   f3,
			row: sql.Row{`{"a": 1, "b": 2}`, `{"a": 3, "c": 4}`, `{"a": 5, "d": 6}`},
			exp: types.MustJSON(`{"a": 5, "b": 2, "c": 4, "d": 6}`),
		},
		{
			f:   f3,
			row: sql.Row{`{"a": 1, "b": 2}`, `{"a": {"one": false, "two": 2.55, "e": 8}}`, `"single value"`},
			exp: types.MustJSON(`"single value"`),
		},
		{
			f:   f3,
			row: sql.Row{1, `{"a": {"one": false, "two": 2.55, "e": 8}}`, `{"a": 1, "b": 2}`},
			err: sql.ErrInvalidJSONArgument.New(1, "json_merge_patch"),
		},
		{
			f:   f3,
			row: sql.Row{`{"a": {"one": false, "two": 2.55, "e": 8}}`, 1, `{"a": 1, "b": 2}`},
			err: sql.ErrInvalidJSONArgument.New(2, "json_merge_patch"),
		},
		{
			f:   f3,
			row: sql.Row{`{`, `{"a": {"one": false, "two": 2.55, "e": 8}}`, `{"a": 1, "b": 2}`},
			err: sql.ErrInvalidJSONText.New(1, "json_merge_patch", "{"),
		},
		{
			f:   f3,
			row: sql.Row{`{"a": {"one": false, "two": 2.55, "e": 8}}`, `}`, `{"a": 1, "b": 2}`},
			err: sql.ErrInvalidJSONText.New(2, "json_merge_patch", "}"),
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
			if tt.err != nil {
				require.Error(err)
				require.Equal(tt.err.Error(), err.Error())
				return
			}
			require.NoError(err)
			require.Equal(tt.exp, res)
		})
	}
}
