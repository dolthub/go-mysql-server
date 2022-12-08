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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
	"testing"
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
		{"equal strings", sql.Text, sql.Text, sql.NewRow("a", "a"), int(0), nil},
		{"first string is smaller", sql.Text, sql.Text, sql.NewRow("a", "b"), int(-1), nil},
		{"second string is smaller", sql.Text, sql.Text, sql.NewRow("b", "a"), int(1), nil},
		{"arguments have different types", sql.Int8, sql.Text, sql.NewRow(1, "1"), int(0), nil},
		{"strcmp is case insensitive", sql.Int8, sql.Text, sql.NewRow("abc123", "ABC123"), int(0), nil},
		{"first argument is null", sql.Text, sql.Text, sql.NewRow(nil, "a"), nil, nil},
		{"second argument is null", sql.Text, sql.Text, sql.NewRow("a", nil), nil, nil},
		{"both arguments are null", sql.Text, sql.Text, sql.NewRow(nil, nil), nil, nil},
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

		f := NewStrCmp(expression.NewLiteral('a', sql.Text), expression.NewLiteral('b', sql.Text))
		_, err := f.WithChildren(expression.NewLiteral('a', sql.Text), expression.NewLiteral('b', sql.Text), expression.NewLiteral('c', sql.Text))
		require.Error(err)
	})

	t.Run("too few arguments", func(t *testing.T) {
		require := require.New(t)

		f := NewStrCmp(expression.NewLiteral('a', sql.Text), expression.NewLiteral('b', sql.Text))
		_, err := f.WithChildren(expression.NewLiteral('a', sql.Text))
		require.Error(err)
	})
}
