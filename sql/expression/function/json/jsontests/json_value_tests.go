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

package jsontests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func RunJsonValueTests(t *testing.T, prepare prepareJsonValue) {

	tests := []struct {
		row      sql.Row
		typ      sql.Type
		path     string
		expected interface{}
		err      error
	}{
		{row: sql.Row{prepare(t, `null`)}, expected: nil},
		{row: sql.Row{prepare(t, `1`)}, expected: "1"},
		{row: sql.Row{prepare(t, `[1]`)}, expected: `[1]`},
		{row: sql.Row{prepare(t, `{"a": "fjsadflkd"}`)}, expected: `{"a": "fjsadflkd"}`},
		{row: sql.Row{prepare(t, `[1, false]`)}, expected: `[1, false]`},
		{row: sql.Row{prepare(t, `[1, false]`)}, path: "$[0]", typ: types.Int64, expected: int64(1)},
		{row: sql.Row{prepare(t, `[1, false]`)}, path: "$[0]", typ: types.Uint64, expected: uint64(1)},
		{row: sql.Row{prepare(t, `[1, false]`)}, path: "$[0]", expected: "1"},
		{row: sql.Row{`[1, false]`}, path: "$[3]", expected: nil},
		{row: sql.Row{prepare(t, `[1, {"a": 1}]`)}, path: "$[1].a", typ: types.Int64, expected: int64(1)},
		{row: sql.Row{prepare(t, `[1, {"a": 1}]`)}, path: "$[1]", typ: types.JSON, expected: types.MustJSON(`{"a": 1}`)},
		{
			row:      sql.Row{prepare(t, `[{"a": 1, "b": 2}, {"a": 3, "b": 4}]`)},
			path:     "$[*].a",
			typ:      types.JSON,
			expected: types.MustJSON(`[1, 3]`),
		},
		{row: sql.Row{1}, path: `$.f`, err: sql.ErrInvalidJSONArgument.New(1, "json_value")},
		{row: sql.Row{`}`}, path: `$.f`, err: sql.ErrInvalidJSONText.New(1, "json_value", "}")},
	}

	for _, tt := range tests {
		args := make([]string, len(tt.row))
		for i, a := range tt.row {
			args[i] = fmt.Sprint(a)
		}
		if tt.path == "" {
			tt.path = "$"
		}
		args = append(args, tt.path)
		if tt.typ != nil {
			args = append(args, tt.typ.String())
		}
		t.Run(strings.Join(args, ", "), func(t *testing.T) {
			args := []sql.Expression{expression.NewGetField(0, types.JSON, "", true)}
			if tt.path != "" {
				args = append(args, expression.NewLiteral(tt.path, types.Text))
			}
			if tt.typ != nil {
				args = append(args, expression.NewLiteral(tt.typ.Zero(), tt.typ))
			}
			f, _ := json.NewJsonValue(args...)
			require := require.New(t)
			// any error case will result in output of 'false' value
			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err == nil {
				require.NoError(err)
				if tt.typ == types.JSON {
					cmp, err := types.JSON.Compare(context.Background(), tt.expected, result)
					require.NoError(err)
					require.Equal(0, cmp)
				} else {
					require.Equal(tt.expected, result)
				}
			} else {
				require.Error(err)
				require.Equal(tt.err.Error(), err.Error())
			}
		})
	}
}
