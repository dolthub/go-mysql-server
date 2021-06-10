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

func TestReverse(t *testing.T) {
	f := NewReverse(sql.NewEmptyContext(), expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"empty string", sql.NewRow(""), "", false},
		{"handles numbers as strings", sql.NewRow(123), "321", false},
		{"valid string", sql.NewRow("foobar"), "raboof", false},
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

func TestRepeat(t *testing.T) {
	f := NewRepeat(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "", false),
		expression.NewGetField(1, sql.Int32, "", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"empty string", sql.NewRow("", 2), "", false},
		{"count is zero", sql.NewRow("foo", 0), "", false},
		{"count is negative", sql.NewRow("foo", -2), "foo", true},
		{"valid string", sql.NewRow("foobar", 2), "foobarfoobar", false},
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

func TestReplace(t *testing.T) {
	f := NewReplace(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "", false),
		expression.NewGetField(1, sql.LongText, "", false),
		expression.NewGetField(2, sql.LongText, "", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null inputs", sql.NewRow(nil), nil, false},
		{"empty str", sql.NewRow("", "foo", "bar"), "", false},
		{"empty fromStr", sql.NewRow("foobarfoobar", "", "car"), "foobarfoobar", false},
		{"empty toStr", sql.NewRow("foobarfoobar", "bar", ""), "foofoo", false},
		{"valid strings", sql.NewRow("foobarfoobar", "bar", "car"), "foocarfoocar", false},
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
