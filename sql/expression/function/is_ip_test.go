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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestIsIPv4(t *testing.T) {
	f := NewIsIPv4(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow("10.0.5.10"), true, false},
		{"valid ipv6 address", sql.NewRow("fdfe::5a55:caff:fefa:9098"), false, false},
		{"malformed ipv4 address", sql.NewRow("1.10.0.5.10"), false, false},
		{"malformed ipv6 address", sql.NewRow("::ffffff"), false, false},
		{"invalid ip address", sql.NewRow("thisisnotavalidipaddress"), false, false},
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

func TestIsIPv6(t *testing.T) {
	f := NewIsIPv6(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow("10.0.5.10"), false, false},
		{"valid ipv6 address", sql.NewRow("::10.0.5.10"), true, false},
		{"valid ipv6 address", sql.NewRow("fdfe::5a55:caff:fefa:9098"), true, false},
		{"malformed ipv4 address", sql.NewRow("1.10.0.5.10"), false, false},
		{"malformed ipv6 address", sql.NewRow("::ffffff"), false, false},
		{"invalid ip address", sql.NewRow("thisisnotavalidipaddress"), false, false},
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

func TestIsIPv4Compat(t *testing.T) {
	f := NewIsIPv4Compat(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow([]byte{10, 0, 1, 10}), false, false},
		{"valid ipv4-compat ipv6 address", sql.NewRow([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 1, 10}), true, false},
		{"valid ipv4-mapped ipv6 address", sql.NewRow([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 1, 10}), false, false},
		{"malformed hex string", sql.NewRow([]byte{0, 0}), false, false},
		{"invalid ip address", sql.NewRow("thisisnotavalidipaddress"), false, false},
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

func TestIsIPv4Mapped(t *testing.T) {
	f := NewIsIPv4Mapped(expression.NewGetField(0, types.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow([]byte{10, 0, 1, 10}), false, false},
		{"valid ipv4-compat ipv6 address", sql.NewRow([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 1, 10}), false, false},
		{"valid ipv4-mapped ipv6 address", sql.NewRow([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 1, 10}), true, false},
		{"malformed hex string", sql.NewRow([]byte{0, 0}), false, false},
		{"invalid ip address", sql.NewRow("thisisnotavalidipaddress"), false, false},
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
