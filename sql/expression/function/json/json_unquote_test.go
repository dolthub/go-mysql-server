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

package json

import (
	"fmt"
"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONUnquote(t *testing.T) {
	testCases := []struct {
		arg sql.Expression
		exp interface{}
		err bool
	}{
		{
			arg: expression.NewLiteral(true, types.Boolean),
			err: true,
		},
		{
			arg: expression.NewLiteral(123, types.Int64),
			err: true,
		},
		{
			arg: expression.NewLiteral(123.0, types.Float64),
			err: true,
		},
		{
			arg: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			arg: expression.NewLiteral(types.MustJSON(`{"a": 1}`), types.JSON),
			exp: `{"a": 1}`,
		},
		{
			arg: expression.NewLiteral(`"abc"`, types.Text),
			exp: `abc`,
		},
		{
			arg: expression.NewLiteral(`"[1, 2, 3]"`, types.Text),
			exp: `[1, 2, 3]`,
		},
		{
			arg: expression.NewLiteral(`"\t\u0032"`, types.Text),
			exp: "\t2",
		},
		{
			arg: expression.NewLiteral(`\`, types.Text),
			err: true,
		},
		{
			arg: expression.NewLiteral(`\b\f\n\r\t\"`, types.Text),
			exp: "\b\f\n\r\t\"",
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%v", tt.arg), func(t *testing.T) {
			require := require.New(t)
			js := NewJSONUnquote(tt.arg)
			result, err := js.Eval(sql.NewEmptyContext(), nil)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tt.exp, result)
		})
	}
}
