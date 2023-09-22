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

package rowexec

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestSet(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewUserVar("foo"), expression.NewLiteral("bar", types.LongText)),
			expression.NewSetField(expression.NewUserVar("baz"), expression.NewLiteral(int64(1), types.Int64)),
		},
	)

	_, err := DefaultBuilder.Build(ctx, s, nil)
	require.NoError(err)

	typ, v, err := ctx.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(types.MustCreateStringWithDefaults(sqltypes.VarChar, 3), typ)
	require.Equal("bar", v)

	typ, v, err = ctx.GetUserVariable(ctx, "baz")
	require.NoError(err)
	require.Equal(types.Int64, typ)
	require.Equal(int64(1), v)
}

func newPersistedSqlContext() (*sql.Context, memory.GlobalsMap) {
	ctx, _ := context.WithCancel(context.TODO())
	pro := memory.NewDBProvider()
	sess := memory.NewSession(sql.NewBaseSession(), pro)

	persistedGlobals := map[string]interface{}{"max_connections": 1000}
	sess.SetGlobals(persistedGlobals)

	sqlCtx := sql.NewContext(ctx)
	sqlCtx.Session = sess
	return sqlCtx, persistedGlobals
}

func TestPersistedSessionSetIterator(t *testing.T) {
	setTests := []struct {
		title        string
		name         string
		value        int
		scope        sql.SystemVariableScope
		err          *errors.Kind
		globalCmp    interface{}
		persistedCmp interface{}
	}{
		{"persist var", "max_connections", 10, sql.SystemVariableScope_Persist, nil, int64(10), int64(10)},
		{"persist only", "max_connections", 10, sql.SystemVariableScope_PersistOnly, nil, int64(151), int64(10)},
		{"no persist", "auto_increment_increment", 3300, sql.SystemVariableScope_Global, nil, int64(3300), nil},
		{"persist unknown variable", "nonexistant", 10, sql.SystemVariableScope_Persist, sql.ErrUnknownSystemVariable, nil, nil},
		{"persist only unknown variable", "nonexistant", 10, sql.SystemVariableScope_PersistOnly, sql.ErrUnknownSystemVariable, nil, nil},
	}

	for _, test := range setTests {
		t.Run(test.title, func(t *testing.T) {
			variables.InitSystemVariables()
			sqlCtx, globals := newPersistedSqlContext()
			s := plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar(test.name, test.scope), expression.NewLiteral(int64(test.value), types.Int64)),
				},
			)

			_, err := DefaultBuilder.Build(sqlCtx, s, nil)
			if test.err != nil {
				assert.True(t, test.err.Is(err))
				return
			} else {
				assert.NoError(t, err)
			}

			res := globals[test.name]
			assert.Equal(t, test.persistedCmp, res)

			_, val, _ := sql.SystemVariables.GetGlobal(test.name)
			assert.Equal(t, test.globalCmp, val)
		})
	}
}
