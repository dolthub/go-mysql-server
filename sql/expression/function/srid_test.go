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

func TestSRID(t *testing.T) {
	t.Run("select unspecified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 0, X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 4230", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 4230, X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(4230), v)
	})

	t.Run("select specified invalid SRID is 1234", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(sql.Point{SRID: 4230, X: 1, Y: 2}, sql.PointType{}))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}