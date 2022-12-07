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

func TestStrCmp(t *testing.T) {
	t.Run("equal strings", func(t *testing.T) {
		require := require.New(t)
		f := NewStrCmp(expression.NewLiteral("a", sql.LongText),
			expression.NewLiteral("a", sql.LongText),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("first string is smaller", func(t *testing.T) {
		require := require.New(t)
		f := NewStrCmp(expression.NewLiteral("a", sql.LongText),
			expression.NewLiteral("b", sql.LongText),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(-1, v)
	})

	t.Run("second string is smaller", func(t *testing.T) {
		require := require.New(t)
		f := NewStrCmp(expression.NewLiteral("b", sql.LongText),
			expression.NewLiteral("a", sql.LongText),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("some argument is nil", func(t *testing.T) {
		require := require.New(t)
		f := NewStrCmp(expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(nil, sql.LongText),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
