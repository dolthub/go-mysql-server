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

package json

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestJSONOverlaps(t *testing.T) {
	_, err := NewJSONOverlaps()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f2 := buildGetFieldExpressions(t, NewJSONOverlaps, 2)
	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err bool
	}{
		// errors
		{
			f: f2,
			row: sql.Row{``},
			err: true,
		},
		{
			f:   f2,
			row: sql.Row{``, ``},
			err: true,
		},
		{
			f:   f2,
			row: sql.Row{`asdf`, `badjson`},
			err: true,
		},

		// nulls
		{
			f:   f2,
			row: sql.Row{nil, nil},
			exp: nil,
		},
		{
			f:   f2,
			row: sql.Row{nil, `true`},
			exp: nil,
		},
		{
			f:   f2,
			row: sql.Row{nil, `1`},
			exp: nil,
		},
		{
			f:   f2,
			row: sql.Row{nil, `"abc"`},
			exp: nil,
		},

		// scalar match
		{
			f:   f2,
			row: sql.Row{`null`, `null`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`false`, `false`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`1`, `1`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`"abc"`, `"abc"`},
			exp: true,
		},

		// scalar mismatch
		{
			f:   f2,
			row: sql.Row{`null`, `15`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`false`, `true`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`1`, `2`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`"abc"`, `"def"`},
			exp: false,
		},

		// objects
		{
			f:   f2,
			row: sql.Row{`{"a": 1, "b": null, "d": 4}`, `{"c": 4, "a": 100, "d": 1, "b": null}`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`{"a":1,"b":10,"d":10}`, `{"c":1,"e":10,"f":1,"d":10}`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`{"a":1,"b":10,"d":10}`, `{"a":5,"e":10,"f":1,"d":20}`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`{"a":1, "b": {"a": 1, "b": 2, "c": 3}, "c": 3}`, `{"b": {"c": 3, "b": 2, "a": 1}}`},
			exp: true,
		},

		// arrays
		{
			f:   f2,
			row: sql.Row{`[1, 2, 3, null, null, null]`, `null`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`[true, true, false, false]`, `false`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`[1,3,5,7]`, `7`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`["abc", "def", "ghi", "jkl"]`, `"ghi"`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{"[1,3,5,7]", "[2,5,7]"},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{"[1,3,5,7]", "[2,6,7]"},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{"[1,3,5,7]", "[2,6,8]"},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`[4,5,6,7]`, `6`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`[4,5,"6",7]`, `6`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`[4,5,6,7]`, `"6"`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`[{"a": 1}]`, `{"a": 1}`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{`[{"a": [{"b": 1, "c": 2, "d": "test"}]}]`, `{"a": 1}`},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`[{}, [], {"a": "1"}, {"a": [{"b": 1, "c": 2, "d": "test"}]}]`, `{"a": [{"b": 1, "c": 2, "d": "test"}]}`},
			exp: true,
		},
		{
			f:   f2,
			row: sql.Row{"[[1,2],[3,4],5]", "[1,[2,3],[4,5]]"},
			exp: false,
		},
		{
			f:   f2,
			row: sql.Row{`[[1, 2]]`, `[[2, 1]]`},
			exp: false,
		},
	}

	for _, tt := range testCases {
		var args []string
		for _, a := range tt.row {
			args = append(args, fmt.Sprintf("%v", a))
		}
		t.Run(strings.Join(args, ", "), func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
			}
			require.Equal(tt.exp, result)
		})
	}
}
