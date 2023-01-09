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

func TestMultiPoint(t *testing.T) {
	t.Run("create valid multipoint with points", func(t *testing.T) {
		require := require.New(t)
		f, err := NewMultiPoint(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(types.Point{X: 3, Y: 4}, types.PointType{}),
			expression.NewLiteral(types.Point{X: 5, Y: 6}, types.PointType{}),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}, v)
	})
}

func TestNewMultiPoint(t *testing.T) {
	require := require.New(t)
	_, err := NewMultiPoint(expression.NewLiteral(nil, types.PointType{}),
		expression.NewLiteral(nil, types.PointType{}),
		expression.NewLiteral(nil, types.PointType{}),
	)
	require.NoError(err)
}
