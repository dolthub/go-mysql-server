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

package function_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestFunctionRegistry(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	reg := function.NewRegistry()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	reg.Register(sql.Function1{
		Name: name,
		Fn:   func(ctx *sql.Context, arg sql.Expression) sql.Expression { return expected },
	})

	f, ok := reg.Function(sql.NewEmptyContext(), "", name)
	require.True(ok)

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

	reg := function.NewRegistry()
	f, ok := reg.Function(sql.NewEmptyContext(), "", "func")
	require.False(ok)
	require.Nil(f)
}

func TestMultiLineStringFromTextRegistryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	reg := function.NewRegistry()
	reg.Register(function.BuiltIns...)
	fn, ok := reg.Function(ctx, "", "st_multilinestringfromtext")
	require.True(t, ok)
	expr, err := fn.NewInstance(ctx, []sql.Expression{
		expression.NewLiteral("MULTILINESTRING((1 2, 3 4))", types.Text),
	})
	require.NoError(t, err)
	require.IsType(t, &spatial.MLineFromText{}, expr)
	require.Equal(t, "st_mlinefromtext('MULTILINESTRING((1 2, 3 4))')", expr.String())
	_, err = sql.NewMysqlParser().ParseSimple("SELECT " + expr.String())
	require.NoError(t, err)
}
