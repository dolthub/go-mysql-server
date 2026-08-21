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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLocate(t *testing.T) {
	intPtr := func(i int) *int {
		return &i
	}

	testCases := []struct {
		Name     string
		Substr   string
		Str      string
		Start    *int
		Expected int32
	}{
		{
			Name:     "locate",
			Substr:   "o",
			Str:      "locate",
			Expected: 2,
		},
		{
			Name:     "locate not found",
			Substr:   "notinlocate",
			Str:      "locate",
			Expected: 0,
		},
		{
			Name:     "locate with start before",
			Substr:   "ocate",
			Str:      "locate",
			Start:    intPtr(1),
			Expected: 2,
		},
		{
			Name:     "locate with start after",
			Substr:   "o",
			Str:      "locate",
			Start:    intPtr(3),
			Expected: 0,
		},
		{
			Name:     "locate with start after 1st iteration",
			Substr:   "o",
			Str:      "locate the second o",
			Start:    intPtr(5),
			Expected: 15,
		},
		{
			Name:     "locate is case insensitive",
			Substr:   "c",
			Str:      "LOCATE",
			Expected: 3,
		},
		{
			Name:     "locate with start 0",
			Substr:   "o",
			Str:      "locate",
			Start:    intPtr(0),
			Expected: 0,
		},
		{
			Name:     "locate with negative start",
			Substr:   "o",
			Str:      "locate",
			Start:    intPtr(-1),
			Expected: 0,
		},
		{
			Name:     "locate empty substring",
			Substr:   "",
			Str:      "locate",
			Expected: 1,
		},
		{
			Name:     "locate empty substring start 1",
			Substr:   "",
			Str:      "locate",
			Start:    intPtr(1),
			Expected: 1,
		},
		{
			Name:     "locate all empty",
			Substr:   "",
			Str:      "",
			Expected: 1,
		},
		{
			Name:     "locate all empty with start > 1",
			Substr:   "",
			Str:      "",
			Start:    intPtr(2),
			Expected: 0,
		},
		// #3649 multibyte positions
		{
			Name:     "locate arabic character position",
			Substr:   "ب",
			Str:      "ااااب",
			Expected: 5,
		},
		{
			Name:     "locate multi-char multibyte needle",
			Substr:   "اب",
			Str:      "ااااب",
			Expected: 4,
		},
		{
			Name:     "locate start counted in characters not bytes",
			Substr:   "a",
			Str:      "ééa",
			Start:    intPtr(2),
			Expected: 3,
		},
		{
			Name:     "locate cafe character position",
			Substr:   "e",
			Str:      "café xe",
			Expected: 7,
		},
		{
			Name:     "locate empty substr with start on multibyte",
			Substr:   "",
			Str:      "ااااب",
			Start:    intPtr(3),
			Expected: 3,
		},
		{
			Name:     "locate start past character length",
			Substr:   "ب",
			Str:      "ااااب",
			Start:    intPtr(6),
			Expected: 0,
		},
		{
			Name:     "locate cross multibyte case fold",
			Substr:   "É",
			Str:      "xyzé",
			Expected: 4,
		},
		{
			Name:     "locate needle in empty with start past end",
			Substr:   "a",
			Str:      "",
			Start:    intPtr(2),
			Expected: 0,
		},
		{
			Name:     "locate empty needle at len+1",
			Substr:   "",
			Str:      "abc",
			Start:    intPtr(4),
			Expected: 4,
		},
		{
			Name:     "locate empty needle past len+1",
			Substr:   "",
			Str:      "abc",
			Start:    intPtr(5),
			Expected: 0,
		},
		{
			Name:     "locate empty empty position 1",
			Substr:   "",
			Str:      "",
			Start:    intPtr(1),
			Expected: 1,
		},
		{
			Name:     "locate non-empty start past end no match",
			Substr:   "x",
			Str:      "abc",
			Start:    intPtr(4),
			Expected: 0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			require := require.New(t)

			exprs := []sql.Expression{
				expression.NewGetField(0, types.Text, "substr", false),
				expression.NewGetField(1, types.LongText, "str", false),
			}
			row := sql.Row{tt.Substr, tt.Str}

			if tt.Start != nil {
				exprs = append(exprs, expression.NewGetField(2, types.Int32, "start", false))
				row = append(row, *tt.Start)
			}

			f, err := NewLocate(sql.NewEmptyContext(), exprs...)
			require.NoError(err)

			result, err := f.Eval(sql.NewEmptyContext(), row)
			require.NoError(err)
			require.Equal(tt.Expected, result)
		})
	}
}

func TestLocateBinaryCaseSensitive(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	f, err := NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, types.Blob, "str", false),
	)
	require.NoError(err)
	result, err := f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(0), result)

	f, err = NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, types.LongText, "str", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(4), result)

	f, err = NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, types.Blob, "str", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"a", "xyza"})
	require.NoError(err)
	require.Equal(int32(4), result)

	// binary needle
	f, err = NewLocate(ctx,
		expression.NewGetField(0, types.Blob, "substr", false),
		expression.NewGetField(1, types.LongText, "str", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(0), result)

	// 3-arg + binary haystack
	f, err = NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, types.Blob, "str", false),
		expression.NewGetField(2, types.Int32, "start", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"a", "xyza", 3})
	require.NoError(err)
	require.Equal(int32(4), result)
	result, err = f.Eval(ctx, sql.Row{"A", "xyza", 1})
	require.NoError(err)
	require.Equal(int32(0), result)

	binType := types.MustCreateBinary(sqltypes.Binary, 4)
	f, err = NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, binType, "str", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(0), result)

	varbinType := types.MustCreateBinary(sqltypes.VarBinary, 16)
	f, err = NewLocate(ctx,
		expression.NewGetField(0, varbinType, "substr", false),
		expression.NewGetField(1, types.LongText, "str", false),
	)
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(0), result)
}

func TestLocateJSONCaseInsensitive(t *testing.T) {
	// JSON is not a binary string type; search stays case-insensitive.
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	f, err := NewLocate(ctx,
		expression.NewGetField(0, types.Text, "substr", false),
		expression.NewGetField(1, types.JSON, "str", false),
	)
	require.NoError(err)
	result, err := f.Eval(ctx, sql.Row{"A", "xyza"})
	require.NoError(err)
	require.Equal(int32(4), result)
}
