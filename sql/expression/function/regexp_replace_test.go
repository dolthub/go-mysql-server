// Copyright 2021 Dolthub, Inc.
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

func TestRegexpReplaceInvalidArgNumber(t *testing.T) {
	_, err := NewRegexpReplace()
	require.Error(t, err)

	_, err = NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
	)
	require.Error(t, err)

	_, err = NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
	)
	require.Error(t, err)

	_, err = NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
		expression.NewGetField(2, types.LongText, "replaceStr", true),
		expression.NewGetField(3, types.LongText, "position", true),
		expression.NewGetField(4, types.LongText, "occurrence", true),
		expression.NewGetField(5, types.LongText, "flags", true),
		expression.NewGetField(6, types.LongText, "???", true),
	)
	require.Error(t, err)
}

func TestRegexpReplace(t *testing.T) {
	f, err := NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
		expression.NewGetField(2, types.LongText, "replaceStr", true),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{
			"nil str",
			sql.NewRow(nil, `[a-z]`, "X"),
			nil,
			false,
		},
		{
			"nil pattern",
			sql.NewRow("abc def ghi", nil, "X"),
			nil,
			false,
		},
		{
			"nil replaceStr",
			sql.NewRow("abc def ghi", `[a-z]`, nil),
			nil,
			false,
		},
		{
			"empty str",
			sql.NewRow("", `[a-z]`, "a"),
			"",
			false,
		},
		{
			"empty pattern",
			sql.NewRow("abc def ghi", ``, nil),
			nil,
			true,
		},
		{
			"empty replaceStr",
			sql.NewRow("abc def ghi", `[a-z]`, ""),
			"  ",
			false,
		},
		{
			"valid case",
			sql.NewRow("abc def ghi", `[a-z]`, "X"),
			"XXX XXX XXX",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestRegexpReplaceWithPosition(t *testing.T) {
	f, err := NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
		expression.NewGetField(2, types.LongText, "replaceStr", true),
		expression.NewGetField(3, types.LongText, "position", true),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{
			"nil position",
			sql.NewRow("abc def ghi", `[a-z]`, "X", nil),
			nil,
			false,
		},
		{
			"negative position",
			sql.NewRow("abc def ghi", `[a-z]`, "X", -1),
			nil,
			true,
		},
		{
			"zero position",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 0),
			nil,
			true,
		},
		{
			"too large position",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1000),
			nil,
			true,
		},
		{
			"string type position",
			sql.NewRow("abc def ghi", `[a-z]`, "X", "1"),
			"XXX XXX XXX",
			false,
		},
		{
			"valid case",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1),
			"XXX XXX XXX",
			false,
		},
		{
			"valid case",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 2),
			"aXX XXX XXX",
			false,
		},
		{
			"valid case",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 5),
			"abc XXX XXX",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestRegexpReplaceWithOccurrence(t *testing.T) {
	f, err := NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
		expression.NewGetField(2, types.LongText, "replaceStr", true),
		expression.NewGetField(3, types.LongText, "position", true),
		expression.NewGetField(4, types.LongText, "occurrence", true),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{
			"nil occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, nil),
			nil,
			false,
		},
		{
			"string type occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, "0"),
			"XXX XXX XXX",
			false,
		},
		{
			"negative occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, -1),
			"Xbc def ghi",
			false,
		},
		{
			"zero occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 0),
			"XXX XXX XXX",
			false,
		},
		{
			"one occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 1),
			"Xbc def ghi",
			false,
		},
		{
			"positive occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 4),
			"abc Xef ghi",
			false,
		},
		{
			"too large occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 1000),
			"abc def ghi",
			false,
		},
		{
			"position and occurrence",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 5, 4),
			"abc def Xhi",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestRegexpReplaceWithFlags(t *testing.T) {
	f, err := NewRegexpReplace(
		expression.NewGetField(0, types.LongText, "str", true),
		expression.NewGetField(1, types.LongText, "pattern", true),
		expression.NewGetField(2, types.LongText, "replaceStr", true),
		expression.NewGetField(3, types.LongText, "position", true),
		expression.NewGetField(4, types.LongText, "occurrence", true),
		expression.NewGetField(5, types.LongText, "flags", true),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{
			"nil flags",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 0, nil),
			nil,
			false,
		},
		{
			"bad flags",
			sql.NewRow("abc def ghi", `[a-z]`, "X", 1, 0, "a"),
			nil,
			true,
		},
		{
			"case-sensitive flags",
			sql.NewRow("abc DEF ghi", `[a-z]`, "X", 1, 0, "c"),
			"XXX DEF XXX",
			false,
		},
		{
			"case-insensitive flags",
			sql.NewRow("abc DEF ghi", `[a-z]`, "X", 1, 0, "i"),
			"XXX XXX XXX",
			false,
		},
		{
			"multiline flags",
			sql.NewRow("abc\r\ndef\r\nghi", `^[a-z].*$`, "X", 1, 0, "m"),
			"X\nX\nX",
			false,
		},
		{
			"insensitive and multiline flags",
			sql.NewRow("abc\r\nDEF\r\nghi", `^[a-z].*$`, "X", 1, 0, "im"),
			"X\nX\nX",
			false,
		},
		{
			"sensitive and multiline flags",
			sql.NewRow("abc\r\nDEF\r\nghi", `^[a-z].*$`, "X", 1, 0, "cm"),
			"X\nDEF\r\nX",
			false,
		},
		{
			"all flags",
			sql.NewRow("abc\r\nDEF\r\nghi", `^[a-z].*$`, "X", 1, 0, "icm"),
			"X\nDEF\r\nX",
			false,
		},
		{
			"repeated flags",
			sql.NewRow("abc DEF ghi", `[a-z]`, "X", 1, 0, "iiiiiicccc"),
			"XXX DEF XXX",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
