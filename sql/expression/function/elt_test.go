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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestElt(t *testing.T) {
	tests := []struct {
		name string
		args []sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			name: "null argument",
			args: []sql.Expression{
				nil,
				nil,
			},
			exp: nil,
		},
		{
			name: "zero returns null",
			args: []sql.Expression{
				expression.NewLiteral(0, types.Int32),
				expression.NewLiteral("foo", types.Text),
			},
			exp: nil,
		},
		{
			name: "negative returns null",
			args: []sql.Expression{
				expression.NewLiteral(-10, types.Int32),
				expression.NewLiteral("foo", types.Text),
			},
			exp: nil,
		},
		{
			name: "too large returns null",
			args: []sql.Expression{
				expression.NewLiteral(100, types.Int32),
				expression.NewLiteral("foo", types.Text),
			},
			exp: nil,
		},
		{
			name: "simple case",
			args: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral("foo", types.Text),
			},
			exp: "foo",
		},
		{
			name: "simple case again",
			args: []sql.Expression{
				expression.NewLiteral(3, types.Int32),
				expression.NewLiteral("foo1", types.Text),
				expression.NewLiteral("foo2", types.Text),
				expression.NewLiteral("foo3", types.Text),
			},
			exp: "foo3",
		},
		{
			name: "index is float",
			args: []sql.Expression{
				expression.NewLiteral(2.9, types.Float64),
				expression.NewLiteral("foo1", types.Text),
				expression.NewLiteral("foo2", types.Text),
				expression.NewLiteral("foo3", types.Text),
			},
			exp: "foo3",
		},
		{
			name: "index is valid string",
			args: []sql.Expression{
				expression.NewLiteral("2", types.Text),
				expression.NewLiteral("foo1", types.Text),
				expression.NewLiteral("foo2", types.Text),
				expression.NewLiteral("foo3", types.Text),
			},
			exp: "foo2",
		},
		{
			name: "args are cast to string",
			args: []sql.Expression{
				expression.NewLiteral("3", types.Text),
				expression.NewLiteral("foo1", types.Text),
				expression.NewLiteral("foo2", types.Text),
				expression.NewLiteral(123, types.Int32),
			},
			exp: "123",
		},
		{
			// we don't do truncation yet
			// https://github.com/dolthub/dolt/issues/7302
			name: "scientific string is truncated",
			args: []sql.Expression{
				expression.NewLiteral("1e1", types.Text),
				expression.NewLiteral("foo1", types.Text),
				expression.NewLiteral("foo2", types.Text),
				expression.NewLiteral("foo3", types.Int32),
			},
			exp:  "foo3",
			skip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}

			ctx := sql.NewEmptyContext()
			f, err := NewElt(tt.args...)
			require.NoError(t, err)

			res, err := f.Eval(ctx, nil)
			if tt.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.exp, res)
		})
	}
}
