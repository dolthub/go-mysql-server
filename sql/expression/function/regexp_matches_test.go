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
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestRegexpMatches(t *testing.T) {
	testCases := []struct {
		pattern  interface{}
		text     interface{}
		flags    interface{}
		expected interface{}
		err      *errors.Kind
	}{
		{
			`^foobar(.*)bye$`,
			"foobarhellobye",
			"",
			[]interface{}{"foobarhellobye", "hello"},
			nil,
		},
		{
			"bop",
			"bopbeepbop",
			"",
			[]interface{}{"bop", "bop"},
			nil,
		},
		{
			"bop",
			"bopbeepBop",
			"i",
			[]interface{}{"bop", "Bop"},
			nil,
		},
		{
			"bop",
			"helloworld",
			"",
			nil,
			nil,
		},
		{
			"foo",
			"",
			"",
			nil,
			nil,
		},
		{
			"",
			"",
			"",
			[]interface{}{""},
			nil,
		},
		{
			"bop",
			nil,
			"",
			nil,
			nil,
		},
		{
			"bop",
			"beep",
			nil,
			nil,
			nil,
		},
		{
			nil,
			"bop",
			"",
			nil,
			nil,
		},
		{
			"bop",
			"bopbeepBop",
			"ix",
			nil,
			errInvalidRegexpFlag,
		},
	}

	t.Run("cacheable", func(t *testing.T) {
		for _, tt := range testCases {
			var flags sql.Expression
			if tt.flags != "" {
				flags = expression.NewLiteral(tt.flags, sql.LongText)
			}
			f, err := NewRegexpMatches(
				expression.NewLiteral(tt.text, sql.LongText),
				expression.NewLiteral(tt.pattern, sql.LongText),
				flags,
			)
			require.NoError(t, err)

			t.Run(f.String(), func(t *testing.T) {
				require := require.New(t)
				result, err := f.Eval(sql.NewEmptyContext(), nil)
				if tt.err == nil {
					require.NoError(err)
					require.Equal(tt.expected, result)
				} else {
					require.Error(err)
					require.True(tt.err.Is(err))
				}
			})
		}
	})

	t.Run("not cacheable", func(t *testing.T) {
		for _, tt := range testCases {
			var flags sql.Expression
			if tt.flags != "" {
				flags = expression.NewGetField(2, sql.LongText, "x", false)
			}
			f, err := NewRegexpMatches(
				expression.NewGetField(0, sql.LongText, "x", false),
				expression.NewGetField(1, sql.LongText, "x", false),
				flags,
			)
			require.NoError(t, err)

			t.Run(f.String(), func(t *testing.T) {
				require := require.New(t)
				result, err := f.Eval(sql.NewEmptyContext(), sql.Row{tt.text, tt.pattern, tt.flags})
				if tt.err == nil {
					require.NoError(err)
					require.Equal(tt.expected, result)
				} else {
					require.Error(err)
					require.True(tt.err.Is(err))
				}
			})
		}
	})
}
