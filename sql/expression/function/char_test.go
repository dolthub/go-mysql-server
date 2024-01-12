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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestChar(t *testing.T) {
	tests := []struct {
		name string
		args []sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			name: "null",
			args: []sql.Expression{
				nil,
			},
			exp: []byte{},
		},
		{
			name: "null literal",
			args: []sql.Expression{
				expression.NewLiteral(nil, types.Null),
			},
			exp: []byte{},
		},
		{
			name: "nulls are skipped",
			args: []sql.Expression{
				expression.NewLiteral(int32(1), types.Int32),
				expression.NewLiteral(nil, types.Null),
				expression.NewLiteral(int32(300), types.Int32),
				expression.NewLiteral(int32(4000), types.Int32),
			},
			exp: []byte{0x1, 0x01, 0x2c, 0xf, 0xa0},
		},
		{
			name: "-1",
			args: []sql.Expression{
				expression.NewLiteral(int32(-1), types.Int32),
			},
			exp: []byte{0xff, 0xff, 0xff, 0xff},
		},
		{
			name: "256",
			args: []sql.Expression{
				expression.NewLiteral(int32(256), types.Int32),
			},
			exp: []byte{0x1, 0x0},
		},
		{
			name: "512",
			args: []sql.Expression{
				expression.NewLiteral(int32(512), types.Int32),
			},
			exp: []byte{0x2, 0x0},
		},
		{
			name: "256 * 256",
			args: []sql.Expression{
				expression.NewLiteral(int32(256*256), types.Int32),
			},
			exp: []byte{0x1, 0x0, 0x0},
		},
		{
			name: "1 2 3 4",
			args: []sql.Expression{
				expression.NewLiteral(int32(1), types.Int32),
				expression.NewLiteral(int32(2), types.Int32),
				expression.NewLiteral(int32(3), types.Int32),
				expression.NewLiteral(int32(4), types.Int32),
			},
			exp: []byte{0x1, 0x2, 0x3, 0x4},
		},
		{
			name: "1 20 300 4000",
			args: []sql.Expression{
				expression.NewLiteral(int32(1), types.Int32),
				expression.NewLiteral(int32(20), types.Int32),
				expression.NewLiteral(int32(300), types.Int32),
				expression.NewLiteral(int32(4000), types.Int32),
			},
			exp: []byte{0x1, 0x14, 0x1, 0x2c, 0xf, 0xa0},
		},
		{
			name: "float32 1.99",
			args: []sql.Expression{
				expression.NewLiteral(float32(1.99), types.Float32),
			},
			exp: []byte{0x2},
		},
		{
			name: "float64 1.99",
			args: []sql.Expression{
				expression.NewLiteral(1.99, types.Float64),
			},
			exp: []byte{0x2},
		},
		{
			name: "decimal 1.99",
			args: []sql.Expression{
				expression.NewLiteral(decimal.NewFromFloat(1.99), types.DecimalType_{}),
			},
			exp: []byte{0x2},
		},
		{
			name: "good string",
			args: []sql.Expression{
				expression.NewLiteral("12", types.Text),
			},
			exp: []byte{0x0C},
		},
		{
			name: "bad string",
			args: []sql.Expression{
				expression.NewLiteral("abc", types.Text),
			},
			exp: []byte{0x0},
		},
		{
			name: "mix types",
			args: []sql.Expression{
				expression.NewLiteral(1, types.Int32),
				expression.NewLiteral(9999, types.Int32),
				expression.NewLiteral(1.23, types.Int32),
				expression.NewLiteral("78", types.Text),
				expression.NewLiteral("abc", types.Text),
			},
			exp: []byte{0x01, 0x27, 0x0F, 0x01, 0x4E, 0x0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}

			ctx := sql.NewEmptyContext()
			f, err := NewChar(tt.args...)
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
