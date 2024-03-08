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
		"github.com/dolthub/go-mysql-server/sql/expression"
"testing"


"github.com/dolthub/go-mysql-server/sql"
"github.com/dolthub/go-mysql-server/sql/types"
"github.com/stretchr/testify/require"
)

func TestJSONPretty(t *testing.T) {
	testCases := []struct {
		arg sql.Expression
		exp interface{}
		err bool
	}{
		{
			arg: expression.NewLiteral(``, types.Text),
			err: true,
		},
		{
			arg: expression.NewLiteral(`badjson`, types.Text),
			err: true,
		},


		{
			arg: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			arg: expression.NewLiteral(`null`, types.Text),
			exp: `null`,
		},
		{
			arg: expression.NewLiteral(`true`, types.Text),
			exp: `true`,
		},
		{
			arg: expression.NewLiteral(`false`, types.Text),
			exp: `false`,
		},
		{
			arg: expression.NewLiteral(`123`, types.Text),
			exp: `123`,
		},
		{
			arg: expression.NewLiteral(`123.456`, types.Text),
			exp: `123.456`,
		},
		{
			arg: expression.NewLiteral(`"hello"`, types.Text),
			exp: `"hello"`,
		},

		{
			arg: expression.NewLiteral(`[]`, types.Text),
			exp: `[]`,
		},
		{
			arg: expression.NewLiteral(`{}`, types.Text),
			exp: `{}`,
		},
		{
			arg: expression.NewLiteral(`[1,3,5]`, types.Text),
			exp:
`[
  1,
  3,
  5
]`,
		},
		{
			arg: expression.NewLiteral(`["a",1,{"key1": "value1"},"5",     "77" , {"key2":["value3","valueX", "valueY"]},"j", "2"   ]`, types.Text),
			exp:
`[
  "a",
  1,
  {
    "key1": "value1"
  },
  "5",
  "77",
  {
    "key2": [
      "value3",
      "valueX",
      "valueY"
    ]
  },
  "j",
  "2"
]`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.arg.String(), func(t *testing.T) {
			require := require.New(t)
			f := NewJSONPretty(tt.arg)
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tt.exp, res)
		})
	}
}

