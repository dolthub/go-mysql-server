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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONType(t *testing.T) {
	_, err := json.NewJSONType()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	f1 := buildGetFieldExpressions(t, json.NewJSONType, 1)
	testCases := []struct {
		f   sql.Expression
		row sql.Row
		exp interface{}
		err error
	}{
		{
			f:   f1,
			row: sql.Row{``},
			err: sql.ErrInvalidJSONText.New(1, "json_type", ""),
		},
		{
			f:   f1,
			row: sql.Row{`badjson`},
			err: sql.ErrInvalidJSONText.New(1, "json_type", "badjson"),
		},
		{
			f:   f1,
			row: sql.Row{true},
			err: sql.ErrInvalidJSONArgument.New(1, "json_type"),
		},
		{
			f:   f1,
			row: sql.Row{1},
			err: sql.ErrInvalidJSONArgument.New(1, "json_type"),
		},
		{
			f:   f1,
			row: sql.Row{1.5},
			err: sql.ErrInvalidJSONArgument.New(1, "json_type"),
		},
		{
			f:   f1,
			row: sql.Row{decimal.New(15, -1)},
			err: sql.ErrInvalidJSONArgument.New(1, "json_type"),
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

		{
			f:   f1,
			row: sql.Row{types.JSONDocument{nil}},
			exp: "NULL",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{uint64(1)}},
			exp: "UNSIGNED INTEGER",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{int64(1)}},
			exp: "INTEGER",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{true}},
			exp: "BOOLEAN",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{123.456}},
			exp: "DOUBLE",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{decimal.New(123456, -3)}},
			exp: "DECIMAL",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{[]interface{}{}}},
			exp: "ARRAY",
		},
		{
			f:   f1,
			row: sql.Row{types.JSONDocument{map[string]interface{}{}}},
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
			if tt.err != nil {
				require.Error(err)
				require.Equal(tt.err.Error(), err.Error())
			} else {
				require.NoError(err)
			}
			require.Equal(tt.exp, result)
		})
	}
}
