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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestConfigWithDefaults(t *testing.T) {
	tests := []struct {
		Name        string
		Scope       sql.SystemVariableScope
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
			ExpectedCmp: int64(76000000),
		}, {
			Name:        "net_read_timeout",
			Scope:       sql.SystemVariableScope_Both,
			Type:        types.NewSystemIntType("net_read_timeout", 1, 9223372036854775807, false),
			ConfigField: "ConnReadTimeout",
			Default:     int64(67),
			ExpectedCmp: int64(67000000),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("server config var: %s", test.Name), func(t *testing.T) {
			variables.InitSystemVariables()
			sql.SystemVariables.AddSystemVariables([]sql.SystemVariableInterface{
				&sql.SystemVariable{
					Name:    test.Name,
					Scope:   test.Scope,
					Dynamic: true,
					Type:    test.Type,
					Default: test.Default,
				},
			})
			serverConf := Config{}
			serverConf, err := serverConf.NewConfig()
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
