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
	)

func TestJSONDepth(t *testing.T) {
	_, err := NewJSONDepth()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONDepth, 1)
	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err bool
	}{
		{
			f:   f1,
			row: sql.Row{``},
			err: true,
		},
		{
			f:   f1,
			row: sql.Row{`badjson`},
			err: true,
		},
		{
			f:   f1,
			row: sql.Row{true},
			err: true,
		},
		{
			f:   f1,
			row: sql.Row{1},
			err: true,
		},

		{
			f:   f1,
			row: sql.Row{nil},
			exp: nil,
		},

		{
			f:   f1,
			row: sql.Row{`null`},
			exp: 1,
		},
		{
			f:   f1,
			row: sql.Row{`1`},
			exp: 1,
		},
		{
			f:   f1,
			row: sql.Row{`true`},
			exp: 1,
		},

		{
			f:   f1,
			row: sql.Row{`[]`},
			exp: 1,
		},
		{
			f:   f1,
			row: sql.Row{`{}`},
			exp: 1,
		},

		{
			f:   f1,
			row: sql.Row{`[null]`},
			exp: 2,
		},
		{
			f:   f1,
			row: sql.Row{`{"a": null}`},
			exp: 2,
		},
		{
			f:   f1,
			row: sql.Row{`[1]`},
			exp: 2,
		},
		{
			f:   f1,
			row: sql.Row{`{"a": 1}`},
			exp: 2,
		},
		{
			f:   f1,
			row: sql.Row{`[1, 2, 3]`},
			exp: 2,
		},
		{
			f:   f1,
			row: sql.Row{`{"aa": 1, "bb": 2, "c": 3}`},
			exp: 2,
		},

		{
			f:   f1,
			row: sql.Row{`{"a": 1, "b": [1, 2, 3]}`},
			exp: 3,
		},
		{
			f:   f1,
			row: sql.Row{`[0, {"a": 1, "b": 2}]`},
			exp: 3,
		},

		{
			f:   f1,
			row: sql.Row{`{"a": 1, "b": {"aa": 1, "bb": {"aaa": 1, "bbb": {"aaaa": 1}}}}`},
			exp: 5,
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
