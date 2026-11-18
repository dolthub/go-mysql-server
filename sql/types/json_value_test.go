// Copyright 2026 Dolthub, Inc.
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

package types

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestMemberAccessOnNonObject(t *testing.T) {
	array := JsonArray{1.0, 2.0, 3.0}
	objectWithArray := JsonObject{"a": JsonArray{1.0, 2.0}}
	objectWithScalar := JsonObject{"a": 5.0}
	objectWithObject := JsonObject{"a": JsonObject{"b": 9.0}}

	tests := []struct {
		name     string
		document interface{}
		path     string
		want     bool
	}{
		// A member access lands on a non-object, so the result is certainly empty.
		{"member on array", array, "$.a", true},
		{"member wildcard on array", array, "$.*", true},
		{"nested member on array", objectWithArray, "$.a.b", true},
		{"nested wildcard on array", objectWithArray, "$.a.*", true},
		{"member on scalar", objectWithScalar, "$.a.b", true},
		{"member after index on array", objectWithArray, "$.a[0].b", true},

		// The path locates a value, so we must defer to the jsonpath library.
		{"value is array", objectWithArray, "$.a", false},
		{"member on object", objectWithObject, "$.a.b", false},
		{"index into array", array, "$[0]", false},
		{"index auto-wraps non-array", objectWithScalar, "$.a[0]", false},

		// Malformed or unmodelled paths must fall back without panicking.
		{"empty path", array, "", false},
		{"root only", array, "$", false},
		{"trailing dot", array, "$.", false},
		{"trailing dot after member", objectWithArray, "$.a.", false},
		{"unterminated bracket", array, "$[5", false},
		{"empty bracket", array, "$[]", false},
		{"open bracket only", array, "$[", false},
		{"unterminated quote", objectWithObject, `$."a`, false},
		{"array cell wildcard", array, "$[*].a", false},
		{"ellipsis", objectWithObject, "$**.a", false},
		{"index overflows int32", array, "$[99999999999999999999]", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, memberAccessOnNonObject(test.document, test.path))
		})
	}
}

// TestValidateJSONPathEscapes distinguishes quoted names from invalid unquoted escapes.
func TestValidateJSONPathEscapes(t *testing.T) {
	for _, path := range []string{
		"$", "$.a[92]", "$.*", "$**.a", "$[1 to 3]",
		`$."a\\b"`, `$."a\"b"`, `$."a\nb"`, `$."a\\b".child`,
	} {
		t.Run(path, func(t *testing.T) {
			require.NoError(t, ValidateJSONPathEscapes(path))
		})
	}
	for _, path := range []string{
		`$.a\/b`, `$.a\q`, `$.a\b`, `$."quoted".a\b`, `$.a\b[0]`,
	} {
		t.Run(path, func(t *testing.T) {
			err := ValidateJSONPathEscapes(path)
			require.True(t, sql.ErrInvalidJSONPath.Is(err))
			sqlErr := sql.CastSQLError(err)
			require.Equal(t, 3143, sqlErr.Num)
			require.Equal(t, "42000", sqlErr.State)
		})
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := ValidateJSONPathEscapes("$.outer.array[92].value"); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations)
}

// TestJSONNullPathEscapes validates malformed paths even when the document is JSON null.
func TestJSONNullPathEscapes(t *testing.T) {
	_, err := (JSONDocument{Val: nil}).Lookup(context.Background(), `$.a\/b`)
	require.True(t, sql.ErrInvalidJSONPath.Is(err))
	sqlErr := sql.CastSQLError(err)
	require.Equal(t, 3143, sqlErr.Num)
	require.Equal(t, "42000", sqlErr.State)
}
