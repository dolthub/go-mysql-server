// Copyright 2022 Dolthub, Inc.
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

package variables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var newConn = sql.SystemVariable{
	Name:    "max_connections",
	Scope:   sql.SystemVariableScope_Global,
	Dynamic: true,
	Type:    types.NewSystemIntType("max_connections", 1, 100000, false),
	Default: int64(1000),
}

var newTimeout = sql.SystemVariable{
	Name:    "net_write_timeout",
	Scope:   sql.SystemVariableScope_Both,
	Dynamic: true,
	Type:    types.NewSystemIntType("net_write_timeout", 1, 9223372036854775807, false),
	Default: int64(1),
}

var newUnknown = sql.SystemVariable{
	Name:    "net_write_timeout",
	Scope:   sql.SystemVariableScope_Both,
	Dynamic: true,
	Type:    types.NewSystemIntType("net_write_timeout", 1, 9223372036854775807, false),
	Default: int64(1),
}

func TestInitSystemVariablesWithDefaults(t *testing.T) {
	tests := []struct {
		name             string
		persistedGlobals []sql.SystemVariable
		err              *errors.Kind
		expectedCmp      []sql.SystemVariable
	}{
		{
			name:             "set max_connections",
			persistedGlobals: []sql.SystemVariable{newConn},
			expectedCmp:      []sql.SystemVariable{newConn},
		}, {
			name:             "set two variables",
			persistedGlobals: []sql.SystemVariable{newConn, newTimeout},
			expectedCmp:      []sql.SystemVariable{newConn, newTimeout},
		}, {
			name:             "unknown system variable",
			persistedGlobals: []sql.SystemVariable{newUnknown},
			expectedCmp:      []sql.SystemVariable{newUnknown},
		}, {
			name: "bad type", // TODO: no checks to prevent incorrect types currently
			persistedGlobals: []sql.SystemVariable{{
				Name:    "max_connections",
				Scope:   sql.SystemVariableScope_Global,
				Dynamic: true,
				Type:    types.NewSystemIntType("max_connections", 1, 100000, false),
				Default: "1000",
			}},
			expectedCmp: []sql.SystemVariable{{
				Name:    "max_connections",
				Scope:   sql.SystemVariableScope_Global,
				Dynamic: true,
				Type:    types.NewSystemIntType("max_connections", 1, 100000, false),
				Default: "1000",
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			InitSystemVariables()
			sql.SystemVariables.AddSystemVariables(test.persistedGlobals)

			for i, sysVar := range test.persistedGlobals {
				cmp, _, _ := sql.SystemVariables.GetGlobal(sysVar.Name)
				assert.Equal(t, test.expectedCmp[i], cmp)
			}
		})
	}
}

func TestHasDefaultValue(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()
	sess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz", User: "bar"}, 1)

	err := sess.SetSessionVariable(ctx, "auto_increment_increment", 123)
	require.NoError(err)
	require.False(sql.HasDefaultValue(ctx, sess, "auto_increment_increment"))
	require.True(sql.HasDefaultValue(ctx, sess, "non_existing_key")) // Returns true for non-existent keys
}

func TestInitReadonlySessionVariable(t *testing.T) {
	const readonlyVariable = "external_user"
	const variableValue = "aoeu"

	require := require.New(t)
	ctx := sql.NewEmptyContext()
	sess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz", User: "bar"}, 1)

	err := sess.SetSessionVariable(ctx, readonlyVariable, variableValue)
	require.Error(err)

	val, err := sess.GetSessionVariable(ctx, readonlyVariable)
	require.NoError(err)
	require.NotEqual(variableValue, val.(string))

	err = sess.InitSessionVariable(ctx, readonlyVariable, variableValue)
	require.NoError(err)

	val, err = sess.GetSessionVariable(ctx, readonlyVariable)
	require.NoError(err)
	require.Equal(variableValue, val.(string))

	err = sess.InitSessionVariable(ctx, readonlyVariable, variableValue)
	require.Error(err)
	require.True(sql.ErrSystemVariableReinitialized.Is(err))
}
