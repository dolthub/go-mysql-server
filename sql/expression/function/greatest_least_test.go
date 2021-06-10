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
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestGreatest(t *testing.T) {
	testCases := []struct {
		name     string
		args     []sql.Expression
		expected interface{}
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, sql.Null),
				expression.NewLiteral(5, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			},
			nil,
		},
		{
			"negative and all ints",
			[]sql.Expression{
				expression.NewLiteral(int64(-1), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			int64(5),
		},
		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral(string("9"), sql.LongText),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(9),
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), sql.LongText),
				expression.NewLiteral(string("foobar"), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(10.5),
		},
		{
			"float mixed",
			[]sql.Expression{
				expression.NewLiteral(float64(10.0), sql.Float64),
				expression.NewLiteral(int(5), sql.Int64),
				expression.NewLiteral(int(1), sql.Int64),
			},
			float64(10.0),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"bbb",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"bbb",
		},
		{
			"nulls of a non-null type, char",
			[]sql.Expression{
				expression.NewConvert(expression.NewLiteral("aaa", sql.LongText), expression.ConvertToChar),
				expression.NewConvert(expression.NewLiteral(nil, sql.Null), expression.ConvertToChar),
			},
			nil,
		},
		{
			"nulls of a non-null type, signed",
			[]sql.Expression{
				expression.NewConvert(expression.NewLiteral(3.14159265359, sql.Float64), expression.ConvertToSigned),
				expression.NewConvert(expression.NewLiteral(nil, sql.Null), expression.ConvertToSigned),
			},
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			f, err := NewGreatest(ctx, tt.args...)
			require.NoError(err)

			output, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, output)
		})
	}
}

func TestGreatestUnsignedOverflow(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	var x int
	var gr sql.Expression
	var err error

	switch unsafe.Sizeof(x) {
	case 4:
		gr, err = NewGreatest(ctx,
			expression.NewLiteral(int32(1), sql.Int32),
			expression.NewLiteral(uint32(4294967295), sql.Uint32),
		)
		require.NoError(err)
	case 8:
		gr, err = NewGreatest(ctx,
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(uint64(18446744073709551615), sql.Uint64),
		)
		require.NoError(err)
	default:
		// non 32/64 bits??
		return
	}

	_, err = gr.Eval(ctx, nil)
	require.EqualError(err, "Unsigned integer too big to fit on signed integer")
}

func TestLeast(t *testing.T) {
	testCases := []struct {
		name     string
		args     []sql.Expression
		expected interface{}
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, sql.Null),
				expression.NewLiteral(5, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			},
			nil,
		},
		{
			"negative and all ints",
			[]sql.Expression{
				expression.NewLiteral(int64(-1), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			int64(-1),
		},
		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral(string("10"), sql.LongText),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(1),
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), sql.LongText),
				expression.NewLiteral(string("foobar"), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(1),
		},
		{
			"float mixed",
			[]sql.Expression{
				expression.NewLiteral(float64(10.0), sql.Float64),
				expression.NewLiteral(int(5), sql.Int64),
				expression.NewLiteral(int(1), sql.Int64),
			},
			float64(1.0),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
			},
			"9999",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			require := require.New(t)

			f, err := NewLeast(ctx, tt.args...)
			require.NoError(err)

			output, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, output)
		})
	}
}
