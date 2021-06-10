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

package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestFunctionRegistry(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	c.MustRegister(sql.Function1{
		Name: name,
		Fn:   func(ctx *sql.Context, arg sql.Expression) sql.Expression { return expected },
	})

	f, err := c.Function(name)
	require.NoError(err)

	ctx := sql.NewEmptyContext()
	e, err := f.NewInstance(ctx, nil)
	require.Error(err)
	require.Nil(e)

	e, err = f.NewInstance(ctx, []sql.Expression{expression.NewStar()})
	require.NoError(err)
	require.Equal(expected, e)

	e, err = f.NewInstance(ctx, []sql.Expression{expression.NewStar(), expression.NewStar()})
	require.Error(err)
	require.Nil(e)
}

func TestFunctionRegistryMissingFunction(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	f, err := c.Function("func")
	require.Error(err)
	require.Nil(f)
}
