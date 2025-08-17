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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

// Tests taken from https://dev.mysql.com/doc/refman/8.0/en/regexp.html#regexp-syntax
func TestRegexpLikeWithoutFlags(t *testing.T) {
	testCases := []struct {
		text     string
		pattern  string
		expected int8
	}{
		{
			"fo\nfo",
			"^fo$",
			0,
		},
		{
			"fofo",
			"^fo",
			1,
		},
		{
			"fo\no",
			"^fo",
			1,
		},
		{
			"fo\no",
			"^fo\no$",
			1,
		},
		{
			"fo\no",
			"^fo$",
			0,
		},
		{
			"fofo",
			"^f.*$",
			1,
		},
		{
			"fo\r\nfo",
			"^f.*$",
			0,
		},
		{
			"fo\r\nfo",
			"(?m)^f.*$",
			1,
		},
		{
			"Ban",
			"^Ba*n",
			1,
		},
		{
			"Baaan",
			"^Ba*n",
			1,
		},
		{
			"Bn",
			"^Ba*n",
			1,
		},
		{
			"Ban",
			"^Ba+n",
			1,
		},
		{
			"Bn",
			"^Ba+n",
			0,
		},
		{
			"Bn",
			"^Ba?n",
			1,
		},
		{
			"Ban",
			"^Ba?n",
			1,
		},
		{
			"Baan",
			"^Ba?n",
			0,
		},
		{
			"pi",
			"pi|apa",
			1,
		},
		{
			"axe",
			"pi|apa",
			0,
		},
		{
			"apa",
			"pi|apa",
			1,
		},
		{
			"apa",
			"^(pi|apa)$",
			1,
		},
		{
			"pi",
			"^(pi|apa)$",
			1,
		},
		{
			"pix",
			"^(pi|apa)$",
			0,
		},
		{
			"pi",
			"^(pi)*$",
			1,
		},
		{
			"pip",
			"^(pi)*$",
			0,
		},
		{
			"pipi",
			"^(pi)*$",
			1,
		},
		{
			"abcde",
			"a[bcd]{2}e",
			0,
		},
		{
			"abcde",
			"a[bcd]{3}e",
			1,
		},
		{
			"abcde",
			"a[bcd]{1,10}e",
			1,
		},
		{
			"aXbc",
			"[a-dXYZ]",
			1,
		},
		{
			"aXbc",
			"^[a-dXYZ]$",
			0,
		},
		{
			"aXbc",
			"^[a-dXYZ]+$",
			1,
		},
		{
			"aXbc",
			"^[^a-dXYZ]+$",
			0,
		},
		{
			"gheis",
			"^[^a-dXYZ]+$",
			1,
		},
		{
			"gheisa",
			"^[^a-dXYZ]+$",
			0,
		},
		{
			"justalnums",
			"[[:alnum:]]+",
			1,
		},
		{
			"!!",
			"[[:alnum:]]+",
			0,
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s|%s", test.text, test.pattern), func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f, err := NewRegexpLike(
				expression.NewLiteral(test.text, types.LongText),
				expression.NewLiteral(test.pattern, types.LongText),
			)
			require.NoError(t, err)
			defer f.(*RegexpLike).Dispose()
			res, err := f.Eval(ctx, nil)
			require.Equal(t, test.expected, res)
		})
	}
}

func TestRegexpLikeWithFlags(t *testing.T) {
	testCases := []struct {
		text     string
		pattern  string
		flags    string
		expected int8
	}{
		{
			"fo\r\nfo",
			"^f.*$",
			"m",
			1,
		},
		{
			"fofo",
			"FOFO",
			"i",
			1,
		},
		{
			"fofo",
			"FOFo",
			"c",
			0,
		},
		{
			"fofo",
			"FOfO",
			"ci",
			1,
		},
		{
			"fofo",
			"FoFO",
			"ic",
			0,
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%v|%v", test.text, test.pattern), func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f, err := NewRegexpLike(
				expression.NewLiteral(test.text, types.LongText),
				expression.NewLiteral(test.pattern, types.LongText),
				expression.NewLiteral(test.flags, types.LongText),
			)
			require.NoError(t, err)
			defer f.(*RegexpLike).Dispose()
			res, err := f.Eval(ctx, nil)
			require.Equal(t, test.expected, res)
		})
	}
}

func TestRegexpLikeNilAndErrors(t *testing.T) {
	ctx := sql.NewEmptyContext()

	f, err := NewRegexpLike(
		expression.NewLiteral("", types.LongText),
	)
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))

	f, err = NewRegexpLike(
		expression.NewLiteral("", types.LongText),
		expression.NewLiteral("", types.LongText),
		expression.NewLiteral("", types.LongText),
		expression.NewLiteral("", types.LongText),
	)
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))

	f, err = NewRegexpLike(
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral("z", types.LongText),
	)
	require.NoError(t, err)
	_, err = f.Eval(ctx, nil)
	require.True(t, sql.ErrInvalidArgument.Is(err))
	f.(*RegexpLike).Dispose()

	f, err = NewRegexpLike(
		expression.NewLiteral(nil, types.Null),
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral("i", types.LongText),
	)
	require.NoError(t, err)
	res, err := f.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, res)
	f.(*RegexpLike).Dispose()

	f, err = NewRegexpLike(
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral(nil, types.Null),
		expression.NewLiteral("i", types.LongText),
	)
	require.NoError(t, err)
	res, err = f.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, res)
	f.(*RegexpLike).Dispose()

	f, err = NewRegexpLike(
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral(nil, types.Null),
	)
	require.NoError(t, err)
	res, err = f.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, res)
	f.(*RegexpLike).Dispose()

	f, err = NewRegexpLike(
		expression.NewLiteral(nil, types.Null),
		expression.NewLiteral("foo", types.LongText),
	)
	require.NoError(t, err)
	res, err = f.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, res)
	f.(*RegexpLike).Dispose()

	f, err = NewRegexpLike(
		expression.NewLiteral("foo", types.LongText),
		expression.NewLiteral(nil, types.Null),
	)
	require.NoError(t, err)
	res, err = f.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, nil, res)
	f.(*RegexpLike).Dispose()
}

// Last Run: 06/17/2025
// BenchmarkRegexpLike
// BenchmarkRegexpLike-14    	     100	  98269522 ns/op
// BenchmarkRegexpLike-14    	   10000	    958159 ns/op
func BenchmarkRegexpLike(b *testing.B) {
	ctx := sql.NewEmptyContext()
	data := make([]sql.Row, 100)
	for i := range data {
		data[i] = sql.Row{fmt.Sprintf("test%d", i)}
	}

	for i := 0; i < b.N; i++ {
		f, err := NewRegexpLike(
			expression.NewGetField(0, types.LongText, "text", false),
			expression.NewLiteral("^test[0-9]$", types.LongText),
		)
		require.NoError(b, err)
		var total int8
		for _, row := range data {
			res, err := f.Eval(ctx, row)
			require.NoError(b, err)
			total += res.(int8)
		}
		require.Equal(b, int8(10), total)
		f.(*RegexpLike).Dispose()
	}
}
