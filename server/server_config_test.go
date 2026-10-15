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

package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestConfigWithDefaults(t *testing.T) {
	tests := []struct {
		Name        string
		Scope       sql.MysqlSVScopeType
		Type        sql.SystemVariableType
		ConfigField string
		Default     interface{}
		ExpectedCmp interface{}
	}{
		{
			Name:        "max_connections",
			Scope:       sql.SystemVariableScope_Global,
			Type:        types.NewSystemIntType("max_connections", 1, 100000, false),
			ConfigField: "MaxConnections",
			Default:     int64(1000),
			ExpectedCmp: uint64(1000),
		},
		{
			Name:        "net_write_timeout",
			Scope:       sql.SystemVariableScope_Both,
			Type:        types.NewSystemIntType("net_write_timeout", 1, 9223372036854775807, false),
			ConfigField: "ConnWriteTimeout",
			Default:     int64(76),
			ExpectedCmp: int64(76000000000),
		}, {
			Name:        "net_read_timeout",
			Scope:       sql.SystemVariableScope_Both,
			Type:        types.NewSystemIntType("net_read_timeout", 1, 9223372036854775807, false),
			ConfigField: "ConnReadTimeout",
			Default:     int64(67),
			ExpectedCmp: int64(67000000000),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("server config var: %s", test.Name), func(t *testing.T) {
			vars := variables.NewEngineVariables()
			vars.System.AddSystemVariables([]sql.SystemVariable{
				&sql.MysqlSystemVariable{
					Name:    test.Name,
					Scope:   sql.GetMysqlScope(test.Scope),
					Dynamic: true,
					Type:    test.Type,
					Default: test.Default,
				},
			})
			serverConf := Config{}
			serverConf, err := serverConf.NewConfigWithVariables(vars)
			assert.NoError(t, err)

			r := reflect.ValueOf(serverConf)
			f := reflect.Indirect(r).FieldByName(test.ConfigField)
			var res interface{}
			switch f.Kind() {
			case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
				res = f.Int()
			case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
				res = f.Uint()
			default:
			}
			assert.Equal(t, test.ExpectedCmp, res)
		})
	}
}

func TestConfigWithEngineVariablesPrecedence(t *testing.T) {
	_, err := (Config{}).NewConfigWithVariables(nil)
	require.ErrorContains(t, err, "requires engine variables")
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	ctxB := sql.NewContextWithVariables(context.Background(), b)
	require.NoError(t, a.System.SetGlobal(ctxA, "max_connections", int64(123)))
	require.NoError(t, b.System.SetGlobal(ctxB, "max_connections", int64(456)))
	require.NoError(t, a.System.SetGlobal(ctxA, "net_read_timeout", int64(17)))
	require.NoError(t, b.System.SetGlobal(ctxB, "net_read_timeout", int64(29)))
	ca, err := (Config{}).NewConfigWithVariables(a)
	require.NoError(t, err)
	cb, err := (Config{}).NewConfigWithVariables(b)
	require.NoError(t, err)
	require.Equal(t, uint64(123), ca.MaxConnections)
	require.Equal(t, uint64(456), cb.MaxConnections)
	require.Equal(t, 17*time.Second, ca.ConnReadTimeout)
	require.Equal(t, 29*time.Second, cb.ConnReadTimeout)

	explicit, err := (Config{MaxConnections: 77, ConnReadTimeout: 8 * time.Second}).NewConfigWithVariables(a)
	require.NoError(t, err)
	require.Equal(t, uint64(77), explicit.MaxConnections)
	require.Equal(t, 8*time.Second, explicit.ConnReadTimeout)
}
