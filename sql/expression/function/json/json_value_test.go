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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonValue(t *testing.T) {
	_, err := NewJSONValid()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	tests := []struct {
		row  sql.Row
		typ  sql.Type
		path string
		exp  interface{}
	}{
		{row: sql.Row{`null`}, exp: nil},
		{row: sql.Row{`1`}, exp: "1"},
		{row: sql.Row{`[1]`}, exp: `[1]`},
		{row: sql.Row{`{"a": "fjsadflkd"}`}, exp: `{"a": "fjsadflkd"}`},
		{row: sql.Row{`[1, false]`}, exp: `[1, false]`},
		{row: sql.Row{`[1, false]`}, path: "$[0]", typ: types.Int64, exp: int64(1)},
		{row: sql.Row{`[1, false]`}, path: "$[0]", typ: types.Uint64, exp: uint64(1)},
		{row: sql.Row{`[1, false]`}, path: "$[0]", exp: "1"},
		{row: sql.Row{`[1, {"a": 1}]`}, path: "$[1].a", typ: types.Int64, exp: int64(1)},
		{row: sql.Row{`[1, {"a": 1}]`}, path: "$[1]", typ: types.JSON, exp: types.MustJSON(`{"a": 1}`)},
	}

	for _, tt := range tests {
		args := make([]string, len(tt.row))
		for i, a := range tt.row {
			args[i] = a.(string)
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
			f, _ := NewJsonValue(args...)
			require := require.New(t)
			// any error case will result in output of 'false' value
			result, _ := f.Eval(sql.NewEmptyContext(), tt.row)
			require.Equal(tt.exp, result)
		})
	}
}
