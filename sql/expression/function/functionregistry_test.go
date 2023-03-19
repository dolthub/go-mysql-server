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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/expression/function"
)

func TestFunctionRegistry(t *testing.T) {
	require := require.New(t)

	reg := function.NewRegistry()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	reg.Register(sql.Function1{
		Name: name,
		Fn:   func(arg sql.Expression) sql.Expression { return expected },
	})

	f, err := reg.Function(sql.NewEmptyContext(), name)
	require.NoError(err)

	e, err := f.NewInstance(nil)
	require.Error(err)
	require.Nil(e)

	e, err = f.NewInstance([]sql.Expression{expression.NewStar()})
	require.NoError(err)
	require.Equal(expected, e)

	e, err = f.NewInstance([]sql.Expression{expression.NewStar(), expression.NewStar()})
	require.Error(err)
	require.Nil(e)
}

func TestFunctionRegistryMissingFunction(t *testing.T) {
	require := require.New(t)

	reg := function.NewRegistry()
	f, err := reg.Function(sql.NewEmptyContext(), "func")
	require.Error(err)
	require.Nil(f)
}
