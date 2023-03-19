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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestTrim(t *testing.T) {
	f := NewTrim(expression.NewGetField(0, types.LongText, "", false), expression.NewGetField(1, types.LongText, "", false), "b")
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil, " ", "b"), nil, false},
		{"trimmed string", sql.NewRow("foo", " ", "b"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    ", " ", "b"), "foo", false},
		{"spaces in left side", sql.NewRow("  foo", " ", "b"), "foo", false},
		{"spaces in right side", sql.NewRow("foo    ", " ", "b"), "foo", false},
		{"two words with spaces", sql.NewRow(" foo   bar ", " ", "b"), "foo   bar", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v", " ", "b"), "\r\tfoo   bar \v", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestLTrim(t *testing.T) {
	f := NewLeftTrim(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"trimmed string", sql.NewRow("foo"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    "), "foo    ", false},
		{"spaces in left side", sql.NewRow("  foo"), "foo", false},
		{"spaces in right side", sql.NewRow("foo    "), "foo    ", false},
		{"two words with spaces", sql.NewRow(" foo   bar "), "foo   bar ", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v"), "\r\tfoo   bar \v", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestRTrim(t *testing.T) {
	f := NewRightTrim(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"trimmed string", sql.NewRow("foo"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    "), "  foo", false},
		{"spaces in left side", sql.NewRow("  foo"), "  foo", false},
		{"spaces in right side", sql.NewRow("foo    "), "foo", false},
		{"two words with spaces", sql.NewRow(" foo   bar "), " foo   bar", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v"), "\r\tfoo   bar \v", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
