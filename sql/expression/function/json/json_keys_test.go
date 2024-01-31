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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONKeys(t *testing.T) {
	_, err := NewJSONKeys()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONKeys, 1)
	f2 := buildGetFieldExpressions(t, NewJSONKeys, 2)

	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err bool
	}{
		{
			f:   f1,
			row: sql.Row{`null`},
			exp: nil,
		},
		{
			f:   f1,
			row: sql.Row{`1`},
			exp: nil,
		},
		{
			f:   f1,
			row: sql.Row{`[1]`},
			exp: nil,
		},
		{
			f:   f1,
			row: sql.Row{`{}`},
			exp: types.MustJSON(`[]`),
		},
		{
			f:   f1,
			row: sql.Row{`"fjsadflkd"`},
			err: true,
		},
		{
			f:   f1,
			row: sql.Row{`{"a": 1}`},
			exp: types.MustJSON(`["a"]`),
		},
		{
			f:   f1,
			row: sql.Row{`{"aa": 1, "bb": 2, "c": 3}`},
			exp: types.MustJSON(`["c", "aa", "bb"]`),
		},

		{
			f:   f2,
			row: sql.Row{`{"a": [1, false]}`, "$"},
			exp: types.MustJSON(`["a"]`),
		},
		{
			f:   f2,
			row: sql.Row{`{"a": {"a": 1}}`, "$.a"},
			exp: types.MustJSON(`["a"]`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2, {"a": 1, "b": {"c": 30}}]`, "$[2]"},
			exp: types.MustJSON(`["a", "b"]`),
		},
		{
			f:   f2,
			row: sql.Row{`[1, 2, {"a": 1, "b": {"c": {"d": 100}}}]`, "$[2].b.c"},
			exp: types.MustJSON(`["d"]`),
		},
		{
			f:   f2,
			row: sql.Row{`{"a": 1, "b": {"c": {"d": "foo"}}}`, "$.b.c"},
			exp: types.MustJSON(`["d"]`),
		},
		{
			f:   f2,
			row: sql.Row{`{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, "$.d"},
			exp: nil,
		},
	}

	for _, tt := range testCases {
		var args []string
		for _, a := range tt.row {
			args = append(args, a.(string))
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
