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

package expression

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestMult(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    string
	}{
		{"1 * 1", 1, 1, "1"},
		{"-1 * 1", -1, 1, "-1"},
		{"0 * 0", 0, 0, "0"},
		{"3.14159 * 3.0", 3.14159, 3.0, "9.42477"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMult(
				NewLiteral(tt.left, types.Float64),
				NewLiteral(tt.right, types.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			r, ok := result.(decimal.Decimal)
			assert.True(t, ok)
			assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
		})
	}

	require := require.New(t)
	result, err := NewMult(NewLiteral("10", types.LongText), NewLiteral("10", types.LongText)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(100.0, result)
}
