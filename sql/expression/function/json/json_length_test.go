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
)

func TestJsonLength(t *testing.T) {
	_, err := NewJSONValid()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJsonLength, 1)
	f2 := buildGetFieldExpressions(t, NewJsonLength, 2)

	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
	}{
		{f1, sql.Row{`null`}, nil},
		{f1, sql.Row{`1`}, 1},
		{f1, sql.Row{`[1]`}, 1},
		{f1, sql.Row{`"fjsadflkd"`}, 1},
		{f1, sql.Row{`[1, false]`}, 2},
		{f1, sql.Row{`[1, {"a": 1}]`}, 2},
		{f1, sql.Row{`{"a": 1}`}, 1},
		{f2, sql.Row{`{"a": [1, false]}`, "$.a"}, 2},
		{f2, sql.Row{`{"a": [1, {"a": 1}]}`, "$.a"}, 2},
		{f2, sql.Row{`{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, "$.b"}, 2},
		{f2, sql.Row{`{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, "$.b[0]"}, 1},
		{f2, sql.Row{`{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, "$.c.d"}, 1},
		{f2, sql.Row{`{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`, "$.d"}, nil},
	}

	for _, tt := range testCases {
		var args []string
		for _, a := range tt.row {
			args = append(args, a.(string))
		}
		t.Run(strings.Join(args, ", "), func(t *testing.T) {
			require := require.New(t)
			// any error case will result in output of 'false' value
			result, _ := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			require.Equal(tt.exp, result)
		})
	}
}
