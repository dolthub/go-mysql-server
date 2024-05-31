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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONSearch(t *testing.T) {
	_, err := NewJSONSearch()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = NewJSONSearch(
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = NewJSONSearch(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f3 := buildGetFieldExpressions(t, NewJSONSearch, 3)
	f4 := buildGetFieldExpressions(t, NewJSONSearch, 4)
	f5 := buildGetFieldExpressions(t, NewJSONSearch, 5)
	f6 := buildGetFieldExpressions(t, NewJSONSearch, 6)

	json := `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`

	testCases := []struct {
		f    sql.Expression
		row  sql.Row
		exp  interface{}
		err  error
		skip bool
	}{
		{
			f:   f3,
			row: sql.Row{1, "one", "abc"},
			err: sql.ErrInvalidJSONArgument.New(1, "json_search"),
		},
		{
			f:   f3,
			row: sql.Row{"", "one", "abc"},
			err: sql.ErrInvalidJSONText.New(1, "json_search", ""),
		},
		{
			f:   f3,
			row: sql.Row{json, "NotOneOrAll", "abc"},
			err: errOneOrAll,
		},
		{
			f:   f3,
			row: sql.Row{json, "one ", "abc"},
			err: errOneOrAll,
		},
		{
			f:   f4,
			row: sql.Row{json, "one", "abc", "badescape"},
			err: errBadEscape,
		},

		{
			f:   f3,
			row: sql.Row{nil, "one", "abc"},
			exp: nil,
		},
		{
			f:   f3,
			row: sql.Row{json, nil, "abc"},
			exp: nil,
		},
		{
			f:   f3,
			row: sql.Row{json, "one", nil},
			exp: nil,
		},
		{
			f:   f5,
			row: sql.Row{json, "one", "abc", "", nil},
			exp: nil,
		},
		{
			f:   f6,
			row: sql.Row{json, "one", "abc", "", "$", nil},
			exp: nil,
		},

		{
			f:   f3,
			row: sql.Row{json, "one", "abc"},
			exp: types.MustJSON(`"$[0]"`),
		},
		{
			f:   f3,
			row: sql.Row{json, "ONE", "abc"},
			exp: types.MustJSON(`"$[0]"`),
		},
		{
			f:   f3,
			row: sql.Row{json, "all", "abc"},
			exp: types.MustJSON(`["$[0]", "$[2].x"]`),
		},
		{
			f:   f3,
			row: sql.Row{json, "all", "ghi"},
			exp: nil,
		},
		{
			f:   f3,
			row: sql.Row{json, "all", "10"},
			exp: types.MustJSON(`"$[1][0].k"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "10", nil, "$"},
			exp: types.MustJSON(`"$[1][0].k"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "10", nil, "$[*]"},
			exp: types.MustJSON(`"$[1][0].k"`),
		},
		{
			// TODO: need to implement ** wildcard in jsonpath package
			skip: true,
			f:    f5,
			row:  sql.Row{json, "all", "10", nil, "$**.k"},
			exp:  types.MustJSON(`"$[1][0].k"`),
		},
		{
			skip: true,
			f:    f5,
			row:  sql.Row{json, "all", "10", nil, "$[*][0].k"},
			exp:  types.MustJSON(`"$[1][0].k"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "10", nil, "$[1]"},
			exp: types.MustJSON(`"$[1][0].k"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "10", nil, "$[1][0]"},
			exp: types.MustJSON(`"$[1][0].k"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "abc", nil, "$[2]"},
			exp: types.MustJSON(`"$[2].x"`),
		},
		{
			f:   f3,
			row: sql.Row{json, "all", "%a%"},
			exp: types.MustJSON(`["$[0]", "$[2].x"]`),
		},
		{
			f:   f3,
			row: sql.Row{json, "all", "%b%"},
			exp: types.MustJSON(`["$[0]", "$[2].x", "$[3].y"]`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "%b%", nil, "$[0]"},
			exp: types.MustJSON(`"$[0]"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "%b%", nil, "$[2]"},
			exp: types.MustJSON(`"$[2].x"`),
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "%b%", nil, "$[1]"},
			exp: nil,
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "%b%", "", "$[1]"},
			exp: nil,
		},
		{
			f:   f5,
			row: sql.Row{json, "all", "%b%", "", "$[3]"},
			exp: types.MustJSON(`"$[3].y"`),
		},

		{
			f:   f4,
			row: sql.Row{`[{"a": "a%c%"}, {"b": "abcd"}]`, "all", `a%c%`, ""},
			exp: types.MustJSON(`["$[0].a", "$[1].b"]`),
		},
		{
			f:   f4,
			row: sql.Row{`[{"a": "a%c%"}, {"b": "abcd"}]`, "all", `a\%c\%`, ""},
			exp: types.MustJSON(`"$[0].a"`),
		},
		{
			f:   f4,
			row: sql.Row{`[{"a": "a%c%"}, {"b": "abcd"}]`, "all", `a\%c\%`, `\`},
			exp: types.MustJSON(`"$[0].a"`),
		},
		{
			f:   f4,
			row: sql.Row{`[{"a": "a%c%"}, {"b": "abcd"}]`, "all", `as%cs%`, `s`},
			exp: types.MustJSON(`"$[0].a"`),
		},

		{
			f:   f6,
			row: sql.Row{json, "all", `abc`, "", "$[0]", "$[2]"},
			exp: types.MustJSON(`["$[0]", "$[2].x"]`),
		},
		{
			f:   f6,
			row: sql.Row{json, "all", `abc`, "", "$[2]", "$"},
			exp: types.MustJSON(`["$[2].x", "$[0]"]`),
		},
	}

	for _, tt := range testCases {
		var args []string
		for _, a := range tt.row {
			args = append(args, fmt.Sprintf("%v", a))
		}
		t.Run(strings.Join(args, ", "), func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
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
