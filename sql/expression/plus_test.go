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
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestPlus(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    string
	}{
		{"1 + 1", 1, 1, "2"},
		{"-1 + 1", -1, 1, "0"},
		{"0 + 0", 0, 0, "0"},
		{"0.14159 + 3.0", 0.14159, 3.0, "3.14159"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewPlus(
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
	result, err := NewPlus(NewLiteral("2", types.LongText), NewLiteral(3, types.Float64)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(5.0, result)
}

func TestPlusInterval(t *testing.T) {
	require := require.New(t)

	expected := time.Date(2018, time.May, 2, 0, 0, 0, 0, time.UTC)
	op := NewPlus(
		NewLiteral("2018-05-01", types.LongText),
		NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
	)

	result, err := op.Eval(sql.NewEmptyContext(), nil)
	require.NoError(err)
	require.Equal(expected, result)

	op = NewPlus(
		NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
		NewLiteral("2018-05-01", types.LongText),
	)

	result, err = op.Eval(sql.NewEmptyContext(), nil)
	require.NoError(err)
	require.Equal(expected, result)
}
