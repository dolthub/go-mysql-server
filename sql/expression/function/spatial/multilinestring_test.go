// Copyright 2020-2022 Dolthub, Inc.
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

package spatial

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestMultiLineString(t *testing.T) {
	t.Run("create valid multilinestring", func(t *testing.T) {
		require := require.New(t)
		f, err := NewMultiLineString(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}}}}}, v)
	})

	t.Run("create valid multilinestring with multiple linestrings", func(t *testing.T) {
		require := require.New(t)
		f, err := NewMultiLineString(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}}}, types.LineStringType{}),
			expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}}}, types.LineStringType{}),
			expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}}}}}, v)
	})
}

func TestNewMultiLineString(t *testing.T) {
	require := require.New(t)
	_, err := NewMultiLineString(expression.NewLiteral(nil, types.LineStringType{}),
		expression.NewLiteral(nil, types.LineStringType{}),
		expression.NewLiteral(nil, types.LineStringType{}),
	)
	require.NoError(err)
}
