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

func TestPolygon(t *testing.T) {
	t.Run("create valid polygon", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LinestringValue{Points: []sql.PointValue{{0,0}, {1,1}, {1,0}, {0,0}}}, sql.Linestring))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.PolygonValue{Lines:[]sql.LinestringValue{{[]sql.PointValue{{0,0},{1,1},{1,0},{0,0}}}}}, v)
	})

	t.Run("create invalid using non-linearring linestring", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPolygon(expression.NewLiteral(sql.LinestringValue{Points: []sql.PointValue{{0,0}}}, sql.Linestring))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}


func TestNewPolygon(t *testing.T) {
	require := require.New(t)
	_, err := NewLinestring(expression.NewLiteral(nil, sql.Point),
		expression.NewLiteral(nil, sql.Point),
		expression.NewLiteral(nil, sql.Point),
	)
	require.NoError(err)
}