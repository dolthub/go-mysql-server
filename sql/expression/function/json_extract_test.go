// Copyright 2020-2021 Dolthub, Inc.
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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestJSONExtract(t *testing.T) {
	f2, err := NewJSONExtract(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONExtract(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := NewJSONExtract(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
		expression.NewGetField(3, sql.LongText, "arg4", false),
	)
	require.NoError(t, err)

	json := map[string]interface{}{
		"a": []interface{}{float64(1), float64(2), float64(3), float64(4)},
		"b": map[string]interface{}{
			"c": "foo",
			"d": true,
		},
		"e": []interface{}{
			[]interface{}{float64(1), float64(2)},
			[]interface{}{float64(3), float64(4)},
		},
		"f": map[string]interface{}{
			`key.with.dots`:        0,
			`key with spaces`:      1,
			`key"with"dquotes`:     2,
			`key'with'squotes`:     3,
			`key\with\backslashes`: 4,
		},
	}

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f2, sql.Row{json, "FOO"}, nil, errors.New("should start with '$'")},
		{f2, sql.Row{nil, "$.b.c"}, sql.JSONDocument{Val: nil}, nil},
		{f2, sql.Row{json, "$.foo"}, sql.JSONDocument{Val: nil}, nil},
		{f2, sql.Row{json, "$.b.c"}, sql.JSONDocument{Val: "foo"}, nil},
		{f3, sql.Row{json, "$.b.c", "$.b.d"}, sql.JSONDocument{Val: []interface{}{"foo", true}}, nil},
		{f4, sql.Row{json, "$.b.c", "$.b.d", "$.e[0][*]"}, sql.JSONDocument{Val: []interface{}{
			"foo",
			true,
			[]interface{}{1., 2.},
		}}, nil},

		{f2, sql.Row{json, `$.f."key.with.dots"`}, sql.JSONDocument{Val: 0}, nil},
		{f2, sql.Row{json, `$.f."key with spaces"`}, sql.JSONDocument{Val: 1}, nil},
		{f2, sql.Row{json, `$.f.key with spaces`}, sql.JSONDocument{Val: 1}, nil},
		{f2, sql.Row{json, `$.f.key'with'squotes`}, sql.JSONDocument{Val: 3}, nil},
		{f2, sql.Row{json, `$.f."key'with'squotes"`}, sql.JSONDocument{Val: 3}, nil},

		// TODO: Fix these. They work in mysql
		//{f2, sql.Row{json, `$.f.key\\"with\\"dquotes`}, sql.JSONDocument{Val: 2}, nil},
		//{f2, sql.Row{json, `$.f.key\'with\'squotes`}, sql.JSONDocument{Val: 3}, nil},
		//{f2, sql.Row{json, `$.f.key\\with\\backslashes`}, sql.JSONDocument{Val: 4}, nil},
		//{f2, sql.Row{json, `$.f."key\\with\\backslashes"`}, sql.JSONDocument{Val: 4}, nil},
	}

	for _, tt := range testCases {
		var paths []string
		for _, path := range tt.row[1:] {
			paths = append(paths, path.(string))
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

/*func TestUnquoteColumns(t *testing.T) {
	tests := []struct{
		str string
		expected string
	} {
		{"", ""},
		{"$", "$"},
		{"$.", "$."},
		{"$.'", "$.'"},
		{"$.''", "$."},
		{"$.'col'", "$.col"},
	}

	for _, test := range tests {
		t.Run(test.str, func(t *testing.T) {
			res := unquoteColumns(test.str)
			assert.Equal(t, test.expected, res)
		})
	}
}*/
