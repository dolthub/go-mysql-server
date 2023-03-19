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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestStrCmp(t *testing.T) {
	testCases := []struct {
		name     string
		e1Type   sql.Type
		e2Type   sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"equal strings", types.Text, types.Text, sql.NewRow("a", "a"), int(0), nil},
		{"first string is smaller", types.Text, types.Text, sql.NewRow("a", "b"), int(-1), nil},
		{"second string is smaller", types.Text, types.Text, sql.NewRow("b", "a"), int(1), nil},
		{"first argument is null", types.Text, types.Text, sql.NewRow(nil, "a"), nil, nil},
		{"second argument is null", types.Text, types.Text, sql.NewRow("a", nil), nil, nil},
		{"both arguments are null", types.Text, types.Text, sql.NewRow(nil, nil), nil, nil},
		{"first argument is text, second argument is not text", types.Text, types.Date, sql.NewRow("a", 2022), int(1), nil},
		{"first argument is not text, second argument is text", types.Int8, types.Text, sql.NewRow(1, "1"), int(0), nil},
		{"both arguments are non-text, different types", types.Int8, types.Date, sql.NewRow(3, 2007), int(1), nil},
		{"type coercion, equal arguments", types.Int8, types.Int8, sql.NewRow(1, 1), int(0), nil},
		{"type coercion, first argument is smaller", types.Int8, types.Int8, sql.NewRow(0, 1), int(-1), nil},
		{"type coercion, second argument is smaller", types.Int8, types.Int8, sql.NewRow(1, 0), int(1), nil},
		// TODO: returning different results from MySQL
		// {"same character set, both case sensitive", sql.CreateTinyText(sql.Collation_utf8mb4_0900_as_cs), sql.CreateTinyText(sql.Collation_utf8mb4_cs_0900_as_cs), sql.NewRow("a", "a"), nil, sql.ErrCollationIllegalMix},
		// {"same character set, both case insensitive", sql.CreateTinyText(sql.Collation_latin1_general_ci), sql.CreateTinyText(sql.Collation_latin1_german1_ci), sql.NewRow("a", "a"), nil, sql.ErrCollationIllegalMix},
		{"different character sets, both case sensitive", types.CreateTinyText(sql.Collation_utf8mb4_0900_as_cs), types.CreateTinyText(sql.Collation_latin1_general_cs), sql.NewRow("a", "a"), int(0), nil},
		{"different character sets, both case insensitive", types.CreateTinyText(sql.Collation_utf8mb4_0900_ai_ci), types.CreateTinyText(sql.Collation_latin1_general_ci), sql.NewRow("a", "a"), int(0), nil},
		{"different character sets, one case sensitive, one case insensitive", types.CreateTinyText(sql.Collation_utf8mb4_0900_ai_ci), types.CreateTinyText(sql.Collation_latin1_general_cs), sql.NewRow("a", "a"), int(0), nil},
	}

	for _, tt := range testCases {
		args0 := expression.NewGetField(0, tt.e1Type, "", false)
		args1 := expression.NewGetField(1, tt.e2Type, "", false)
		f := NewStrCmp(args0, args1)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	t.Run("too many arguments", func(t *testing.T) {
		require := require.New(t)

		f := NewStrCmp(expression.NewLiteral('a', types.Text), expression.NewLiteral('b', types.Text))
		_, err := f.WithChildren(expression.NewLiteral('a', types.Text), expression.NewLiteral('b', types.Text), expression.NewLiteral('c', types.Text))
		require.Error(err)
	})

	t.Run("too few arguments", func(t *testing.T) {
		require := require.New(t)

		f := NewStrCmp(expression.NewLiteral('a', types.Text), expression.NewLiteral('b', types.Text))
		_, err := f.WithChildren(expression.NewLiteral('a', types.Text))
		require.Error(err)
	})
}
