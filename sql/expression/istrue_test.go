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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestIsTrue(t *testing.T) {
	require := require.New(t)

	boolF := NewGetField(0, types.Boolean, "col1", true)
	e := NewIsTrue(boolF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(true)))
	require.Equal(false, eval(t, e, sql.NewRow(false)))

	intF := NewGetField(0, types.Int64, "col1", true)
	e = NewIsTrue(intF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(100)))
	require.Equal(true, eval(t, e, sql.NewRow(-1)))
	require.Equal(false, eval(t, e, sql.NewRow(0)))

	floatF := NewGetField(0, types.Float64, "col1", true)
	e = NewIsTrue(floatF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(1.5)))
	require.Equal(true, eval(t, e, sql.NewRow(-1.5)))
	require.Equal(false, eval(t, e, sql.NewRow(0)))

	stringF := NewGetField(0, types.Text, "col1", true)
	e = NewIsTrue(stringF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow("")))
	require.Equal(false, eval(t, e, sql.NewRow("false")))
	require.Equal(false, eval(t, e, sql.NewRow("true")))
}

func TestIsFalse(t *testing.T) {
	require := require.New(t)

	boolF := NewGetField(0, types.Boolean, "col1", true)
	e := NewIsFalse(boolF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(true)))
	require.Equal(true, eval(t, e, sql.NewRow(false)))

	intF := NewGetField(0, types.Int64, "col1", true)
	e = NewIsFalse(intF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(100)))
	require.Equal(false, eval(t, e, sql.NewRow(-1)))
	require.Equal(true, eval(t, e, sql.NewRow(0)))

	floatF := NewGetField(0, types.Float64, "col1", true)
	e = NewIsFalse(floatF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(1.5)))
	require.Equal(false, eval(t, e, sql.NewRow(-1.5)))
	require.Equal(true, eval(t, e, sql.NewRow(0)))

	stringF := NewGetField(0, types.Text, "col1", true)
	e = NewIsFalse(stringF)
	require.Equal(types.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow("")))
	require.Equal(true, eval(t, e, sql.NewRow("false")))
	require.Equal(true, eval(t, e, sql.NewRow("true")))
}
