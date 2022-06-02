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

func TestLineString(t *testing.T) {
	t.Run("create valid linestring with points", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLineString(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}),
			expression.NewLiteral(sql.Point{X: 3, Y: 4}, sql.PointType{}),
			expression.NewLiteral(sql.Point{X: 5, Y: 6}, sql.PointType{}),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}, v)
	})
}

func TestNewLineString(t *testing.T) {
	require := require.New(t)
	_, err := NewLineString(expression.NewLiteral(nil, sql.PointType{}),
		expression.NewLiteral(nil, sql.PointType{}),
		expression.NewLiteral(nil, sql.PointType{}),
	)
	require.NoError(err)
}
