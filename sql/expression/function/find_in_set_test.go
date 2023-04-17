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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestFindInSet(t *testing.T) {
	testCases := []struct {
		name     string
		left     string
		right    string
		expected int
		skip     bool
	}{
		{
			name: "string exists",
			left: "b",
			right: "a,b,c",
			expected: 2,
		},
		{
			name: "string does not exist",
			left: "abc",
			right: "a,b,c",
			expected: 0,
		},
		{
			name: "whitespace not removed",
			left: "  b   ",
			right: "a,b,c",
			expected: 0,
		},
		{
			name: "whitespace not removed 2",
			left: "b",
			right: "  a  ,  b ,  c  ",
			expected: 0,
		},
		{
			name: "whitespace not removed 3",
			left: " a b ",
			right: "a, a b ,c",
			expected: 2,
		},
		{
			name: "comma bad",
			left: "b,",
			right: "a,b,c",
			expected: 0,
		},
		{
			name: "special characters ok",
			left: "test@example.com",
			right: "nottest@example.com,hello@example.com,test@example.com",
			expected: 3,
		},
		{
			name: "look for empty string",
			left: "",
			right: "a,",
			expected: 2,
		},
		{
			name: "look in empty string",
			left: "a",
			right: "",
			expected: 0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
			require := require.New(t)
			f := NewFindInSet(expression.NewLiteral(tt.left, types.LongText), expression.NewLiteral(tt.right, types.LongText))
			v, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			require.Equal(tt.expected, v)
		})
	}

	t.Run("test find in null set", func(t *testing.T) {
		require := require.New(t)
		f := NewFindInSet(expression.NewLiteral("a", types.LongText), expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("find null in set", func(t *testing.T) {
		require := require.New(t)
		f := NewFindInSet(expression.NewLiteral("a", types.LongText), expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("find number", func(t *testing.T) {
		require := require.New(t)
		f := NewFindInSet(expression.NewLiteral(500, types.Int64), expression.NewLiteral("1,2,3,500", types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(4, v)
	})
}
