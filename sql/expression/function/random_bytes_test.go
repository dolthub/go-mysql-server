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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestRandomBytes(t *testing.T) {
	testCases := []struct {
		expr sql.Expression
		exp  interface{}
		skip bool
		err  *errors.Kind
	}{
		{
			expr: expression.NewLiteral(nil, types.Null),
			exp:  nil,
		},
		{
			expr: expression.NewLiteral(int32(0), types.Int32),
			err:  sql.ErrValueOutOfRange,
		},
		{
			expr: expression.NewLiteral(int32(-1), types.Int32),
			err:  sql.ErrValueOutOfRange,
		},
		{
			expr: expression.NewLiteral(int32(randomBytesMax+1), types.Int32),
			err:  sql.ErrValueOutOfRange,
		},
		{
			expr: expression.NewLiteral(int32(1), types.Int32),
			exp:  make([]byte, 1),
		},
		{
			expr: expression.NewLiteral(int32(100), types.Int32),
			exp:  make([]byte, 100),
		},
		{
			expr: expression.NewLiteral(int32(randomBytesMax), types.Int32),
			exp:  make([]byte, randomBytesMax),
		},
		{
			expr: expression.NewLiteral(int32(randomBytesMax), types.Int32),
			exp:  make([]byte, randomBytesMax),
		},
		{
			expr: expression.NewLiteral(3.9, types.Float64),
			exp:  make([]byte, 4),
		},
		{
			expr: expression.NewLiteral(3.4, types.Float64),
			exp:  make([]byte, 3),
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s(%v)", "random_bytes", test.expr.String()), func(t *testing.T) {
			if test.skip {
				t.Skip()
			}
			ctx := sql.NewEmptyContext()
			f := NewRandomBytes(test.expr)
			res, err := f.Eval(ctx, nil)
			if test.err != nil {
				require.True(t, test.err.Is(err))
				return
			}
			require.NoError(t, err)
			if test.exp == nil {
				require.Equal(t, test.exp, res)
				return
			}
			require.Equal(t, len(test.exp.([]byte)), len(res.([]byte)))
		})
	}
}
