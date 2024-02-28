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

func TestJSONType(t *testing.T) {
	_, err := NewJSONType()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, NewJSONType, 1)
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
			exp: "NULL",
		},

		{
			f:   f1,
			row: sql.Row{`null`},
			exp: "NULL",
		},
		{
			f:   f1,
			row: sql.Row{`1`},
			exp: "INTEGER",
		},
		{
			f:   f1,
			row: sql.Row{`true`},
			exp: "BOOLEAN",
		},
		{
			f:   f1,
			row: sql.Row{`123.456`},
			exp: "DOUBLE",
		},

		{
			f:   f1,
			row: sql.Row{`[]`},
			exp: "ARRAY",
		},
		{
			f:   f1,
			row: sql.Row{`{}`},
			exp: "OBJECT",
		},

		{
			f:   f1,
			row: sql.Row{`[1, 2, 3]`},
			exp: "ARRAY",
		},
		{
			f:   f1,
			row: sql.Row{`{"aa": 1, "bb": 2, "c": 3}`},
			exp: "OBJECT",
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
