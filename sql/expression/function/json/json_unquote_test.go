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
	js := NewJSONUnquote(expression.NewGetField(0, types.LongText, "json", false))

	testCases := []struct {
		row sql.Row
		exp interface{}
		err bool
	}{
		{
			row: sql.Row{nil},
			exp: nil,
		},
		{
			row: sql.Row{"\"abc\""},
			exp: `abc`,
		},
		{
			row: sql.Row{"[1, 2, 3]"},
			exp: `[1, 2, 3]`,
		},
		{
			row: sql.Row{`"\t\u0032"`},
			exp: "\t2",
		},
		{
			row: sql.Row{`\`},
			err: true,
		},
		{
			row: sql.Row{`\b\f\n\r\t\"`},
			exp: "\b\f\n\r\t\"",
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%v", tt.row[0]), func(t *testing.T) {
			require := require.New(t)
			result, err := js.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tt.exp, result)
		})
	}
}
