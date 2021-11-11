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
func TestNewINETATON(t *testing.T) {
	f := NewINETATON(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow("10.0.5.10"), uint32(167773450), false},
		// Output does not match MySQL, but it also indicates it shouldn't be used for short-form anyway: https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_inet-aton
		{"valid short-form ipv4 address", sql.NewRow("10.5.10"), nil, false},
		{"valid shoft-form ip4 address (non-string)", sql.NewRow(10.0), nil, false},
		{"valid ipv6 address", sql.NewRow("::10.0.5.10"), nil, false},
		{"valid ipv6 address", sql.NewRow("fdfe::5a55:caff:fefa:9098"), nil, false},
		{"invalid ipv4 address", sql.NewRow("1.10.0.5.10"), nil, false},
		{"valid ipv6 address", sql.NewRow("thisisnotavalidipaddress"), nil, false},
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


func TestNewINETNTOA(t *testing.T) {
	f := NewINETNTOA(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 int", sql.NewRow(uint32(167773450)), "10.0.5.10", false},
		{"valid ipv4 int as string", sql.NewRow("167773450"), "10.0.5.10", false},
		{"floating point ipv4", sql.NewRow(10.1), "0.0.0.10", false},
		{"valid ipv6 int", sql.NewRow("\000\000\000\000"), "0.0.0.0", false},
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

func TestINET6ATON(t *testing.T) {
	f := NewINET6ATON(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 address", sql.NewRow("10.0.5.10"), uint32(167773450), false},
		{"valid ipv4-mapped ipv6 address", sql.NewRow("::10.0.5.10"), "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x05\x0a", false},
		{"valid short-form ipv4 address", sql.NewRow("10.5.10"), nil, false},
		{"valid ipv6 address", sql.NewRow("fdfe::5a55:caff:fefa:9098"), "\xfd\xfe\x00\x00\x00\x00\x00\x00\x5a\x55\xca\xff\xfe\xfa\x90\x98", false},
		{"invalid ipv4 address", sql.NewRow("1.10.0.5.10"), nil, false},
		{"valid ipv6 address", sql.NewRow("thisisnotavalidipaddress"), nil, false},
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

func TestNewINET6NTOA(t *testing.T) {
	f := NewINET6NTOA(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"valid ipv4 int", sql.NewRow(uint32(167773450)), nil, false},
		{"valid ipv4 int as string", sql.NewRow("167773450"), nil, false},
		{"floating point ipv4", sql.NewRow(10.1), nil, false},
		{"valid ipv6 int", sql.NewRow("\x00\x00\x00\x00"), "0.0.0.0", false},
		{"valid ipv6 int", sql.NewRow("\xfd\xfe\x00\x00\x00\x00\x00\x00\x5a\x55\xca\xff\xfe\xfa\x90\x98"), "fdfe::5a55:caff:fefa:9098", false},
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