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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestIsBinary(t *testing.T) {
	f := NewIsBinary(sql.NewEmptyContext(), expression.NewGetField(0, sql.Blob, "blob", true))

	testCases := []struct {
		name     string
		row      sql.Row
		expected bool
	}{
		{"binary", sql.NewRow([]byte{0, 1, 2}), true},
		{"not binary", sql.NewRow([]byte{1, 2, 3}), false},
		{"null", sql.NewRow(nil), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})
	}
}

func TestSubstringArity(t *testing.T) {
	expr := expression.NewGetField(0, sql.Int64, "foo", false)
	testCases := []struct {
		name string
		args []sql.Expression
		ok   bool
	}{
		{"0 args", nil, false},
		{"1 args", []sql.Expression{expr}, false},
		{"2 args", []sql.Expression{expr, expr}, true},
		{"3 args", []sql.Expression{expr, expr, expr}, true},
		{"4 args", []sql.Expression{expr, expr, expr, expr}, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			f, err := NewSubstring(sql.NewEmptyContext(), tt.args...)
			if tt.ok {
				require.NotNil(f)
				require.NoError(err)
			} else {
				require.Error(err)
			}
		})
	}
}
