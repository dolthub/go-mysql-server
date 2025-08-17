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

func TestBitCount(t *testing.T) {
	tests := []struct {
		name string
		arg  sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			name: "null argument",
			arg:  nil,
			exp:  nil,
			err:  false,
		},
		{
			name: "zero",
			arg:  expression.NewLiteral(int64(0), types.Int64),
			exp:  int32(0),
			err:  false,
		},
		{
			name: "one",
			arg:  expression.NewLiteral(int64(1), types.Int64),
			exp:  int32(1),
			err:  false,
		},
		{
			name: "ten",
			arg:  expression.NewLiteral(int64(10), types.Int64),
			exp:  int32(2),
			err:  false,
		},
		{
			name: "negative",
			arg:  expression.NewLiteral(int64(-1), types.Int64),
			exp:  int32(64),
			err:  false,
		},
		{
			name: "float32 rounds down",
			arg:  expression.NewLiteral(float32(1.1), types.Float32),
			exp:  int32(1),
			err:  false,
		},
		{
			name: "float64 rounds down",
			arg:  expression.NewLiteral(1.1, types.Float64),
			exp:  int32(1),
			err:  false,
		},
		{
			name: "decimal rounds down",
			arg:  expression.NewLiteral(decimal.NewFromFloat(1.1), types.DecimalType_{}),
			exp:  int32(1),
			err:  false,
		},
		{
			name: "float32 rounds up",
			arg:  expression.NewLiteral(float32(2.99), types.Float32),
			exp:  int32(2),
			err:  false,
		},
		{
			name: "float64 rounds up",
			arg:  expression.NewLiteral(2.99, types.Float64),
			exp:  int32(2),
			err:  false,
		},
		{
			name: "decimal rounds up",
			arg:  expression.NewLiteral(decimal.NewFromFloat(2.99), types.DecimalType_{}),
			exp:  int32(2),
			err:  false,
		},
		{
			name: "negative float32",
			arg:  expression.NewLiteral(float32(-12.34), types.Float32),
			exp:  int32(61),
			err:  false,
		},
		{
			name: "negative float64",
			arg:  expression.NewLiteral(-12.34, types.Float64),
			exp:  int32(61),
			err:  false,
		},
		{
			name: "negative decimal",
			arg:  expression.NewLiteral(decimal.NewFromFloat(-12.34), types.DecimalType_{}),
			exp:  int32(61),
			err:  false,
		},
		{
			name: "string is 0",
			arg:  expression.NewLiteral("notanumber", types.Text),
			exp:  int32(0),
			err:  false,
		},
		{
			name: "numerical string",
			arg:  expression.NewLiteral("15", types.Text),
			exp:  int32(4),
			err:  false,
		},
		{
			// we don't do truncation yet
			// https://github.com/dolthub/dolt/issues/7302
			name: "scientific string is truncated",
			arg:  expression.NewLiteral("1e1", types.Text),
			exp:  int32(1),
			err:  false,
			skip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}

			ctx := sql.NewEmptyContext()
			f := NewBitCount(tt.arg)

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
