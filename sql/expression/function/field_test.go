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

func TestField(t *testing.T) {
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
			exp:  int32(0),
		},
		{
			name: "not found",
			args: []sql.Expression{
				expression.NewLiteral("abc", types.Text),
				expression.NewLiteral("def", types.Text),
			},
			exp:  int32(0),
		},
		{
			name: "simple case",
			args: []sql.Expression{
				expression.NewLiteral("abc", types.Int32),
				expression.NewLiteral("abc", types.Int32),
				expression.NewLiteral("def", types.Int32),
				expression.NewLiteral("xyz", types.Int32),
			},
			exp:  int32(1),
		},
		{
			name: "simple case again",
			args: []sql.Expression{
				expression.NewLiteral("def", types.Int32),
				expression.NewLiteral("abc", types.Int32),
				expression.NewLiteral("def", types.Int32),
				expression.NewLiteral("xyz", types.Int32),
			},
			exp:  int32(2),
		},
		{
			name: "index is int",
			args: []sql.Expression{
				expression.NewLiteral(10, types.Int32),
				expression.NewLiteral("8", types.Text),
				expression.NewLiteral("8", types.Text),
				expression.NewLiteral("10", types.Text),
			},
			exp:  int32(3),
		},
		{
			name: "index is float",
			args: []sql.Expression{
				expression.NewLiteral(2.9, types.Float64),
				expression.NewLiteral("1", types.Text),
				expression.NewLiteral("2.9", types.Text),
				expression.NewLiteral("3", types.Text),
			},
			exp:  int32(2),
		},
		{
			name: "scientific string is truncated",
			args: []sql.Expression{
				expression.NewLiteral(1e2, types.Int32),
				expression.NewLiteral("100", types.Text),
			},
			exp:  int32(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}

			ctx := sql.NewEmptyContext()
			f, err := NewField(tt.args...)
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
