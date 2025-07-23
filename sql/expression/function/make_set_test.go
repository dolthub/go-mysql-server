// Copyright 2020-2024 Dolthub, Inc.
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

func TestMakeSet(t *testing.T) {
	testCases := []struct {
		name     string
		args     []interface{}
		expected interface{}
		err      bool
	}{
		// MySQL documentation examples
		{"mysql example 1", []interface{}{1, "a", "b", "c"}, "a", false},
		{"mysql example 2", []interface{}{1 | 4, "hello", "nice", "world"}, "hello,world", false},
		{"mysql example 3", []interface{}{1 | 4, "hello", "nice", nil, "world"}, "hello", false},
		{"mysql example 4", []interface{}{0, "a", "b", "c"}, "", false},

		// Basic functionality tests
		{"single bit set - bit 0", []interface{}{1, "first", "second", "third"}, "first", false},
		{"single bit set - bit 1", []interface{}{2, "first", "second", "third"}, "second", false},
		{"single bit set - bit 2", []interface{}{4, "first", "second", "third"}, "third", false},
		{"no bits set", []interface{}{0, "first", "second", "third"}, "", false},

		// Multiple bits set
		{"bits 0 and 1", []interface{}{3, "a", "b", "c"}, "a,b", false},
		{"bits 0 and 2", []interface{}{5, "a", "b", "c"}, "a,c", false},
		{"bits 1 and 2", []interface{}{6, "a", "b", "c"}, "b,c", false},
		{"all bits set", []interface{}{7, "a", "b", "c"}, "a,b,c", false},

		// Large bit numbers
		{"bit 10 set", []interface{}{1024, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}, "k", false},
		{"bits 0 and 10", []interface{}{1025, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}, "a,k", false},

		// NULL handling
		{"null bits", []interface{}{nil, "a", "b", "c"}, nil, false},
		{"null in middle", []interface{}{7, "a", nil, "c"}, "a,c", false},
		{"null at start", []interface{}{7, nil, "b", "c"}, "b,c", false},
		{"null at end", []interface{}{7, "a", "b", nil}, "a,b", false},
		{"all nulls", []interface{}{7, nil, nil, nil}, "", false},

		// Type conversion
		{"string bits", []interface{}{"5", "a", "b", "c"}, "a,c", false},
		{"float bits", []interface{}{5.7, "a", "b", "c"}, "b,c", false}, // 5.7 converts to 6 (binary 110)
		{"negative bits", []interface{}{-1, "a", "b", "c"}, "a,b,c", false},

		// Different value types
		{"numeric strings", []interface{}{3, "1", "2", "3"}, "1,2", false},
		{"mixed types", []interface{}{3, 123, "hello", 456}, "123,hello", false},

		// Edge cases
		{"no strings provided", []interface{}{1}, "", true},
		{"bit beyond available strings", []interface{}{16, "a", "b", "c"}, "", false},
		{"bit partially beyond strings", []interface{}{9, "a", "b", "c"}, "a", false},

		// Large numbers
		{"max uint64 bits", []interface{}{^uint64(0), "a", "b", "c"}, "a,b,c", false},
		{"large positive number", []interface{}{4294967295, "a", "b", "c"}, "a,b,c", false},

		// Empty strings
		{"empty string values", []interface{}{3, "", "test", ""}, ",test", false},
		{"only empty strings", []interface{}{3, "", ""}, ",", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			// Convert test args to expressions
			args := make([]sql.Expression, len(tt.args))
			for i, arg := range tt.args {
				if arg == nil {
					args[i] = expression.NewLiteral(nil, types.Null)
				} else {
					switch v := arg.(type) {
					case int:
						args[i] = expression.NewLiteral(int64(v), types.Int64)
					case uint64:
						args[i] = expression.NewLiteral(v, types.Uint64)
					case float64:
						args[i] = expression.NewLiteral(v, types.Float64)
					case string:
						args[i] = expression.NewLiteral(v, types.LongText)
					default:
						args[i] = expression.NewLiteral(v, types.LongText)
					}
				}
			}

			f, err := NewMakeSet(args...)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)

			v, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, v)
		})
	}
}

func TestMakeSetArguments(t *testing.T) {
	require := require.New(t)

	// Test invalid number of arguments
	_, err := NewMakeSet()
	require.Error(err)

	_, err = NewMakeSet(expression.NewLiteral(1, types.Int64))
	require.Error(err)

	// Test valid argument counts
	validArgs := [][]sql.Expression{
		{expression.NewLiteral(1, types.Int64), expression.NewLiteral("a", types.Text)},
		{expression.NewLiteral(1, types.Int64), expression.NewLiteral("a", types.Text), expression.NewLiteral("b", types.Text)},
		{expression.NewLiteral(1, types.Int64), expression.NewLiteral("a", types.Text), expression.NewLiteral("b", types.Text), expression.NewLiteral("c", types.Text)},
	}

	for _, args := range validArgs {
		_, err := NewMakeSet(args...)
		require.NoError(err)
	}
}
