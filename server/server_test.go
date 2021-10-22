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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var testGlobals = []sql.SystemVariable{
	{
		Name:    "max_connections",
		Scope:   sql.SystemVariableScope_Global,
		Dynamic: true,
		Type:    sql.NewSystemIntType("max_connections", 1, 100000, false),
		Default: int64(1000),
	}, {
		Name:    "net_write_timeout",
		Scope:   sql.SystemVariableScope_Both,
		Dynamic: true,
		Type:    sql.NewSystemIntType("net_write_timeout", 1, 9223372036854775807, false),
		Default: int64(1),
	}, {
		Name:    "net_read_timeout",
		Scope:   sql.SystemVariableScope_Both,
		Dynamic: true,
		Type:    sql.NewSystemIntType("net_read_timeout", 1, 9223372036854775807, false),
		Default: int64(1),
	},
}

func newPersistedGlobals() []sql.SystemVariable {
	persistedGlobals := make([]sql.SystemVariable, len(testGlobals))
	for i, v := range testGlobals {
		persistedGlobals[i] = v.Copy()
	}
	return persistedGlobals
}

func TestConfigWithDefaults(t *testing.T) {
	sql.InitSystemVariables()
	sql.SystemVariables.AddSystemVariables(newPersistedGlobals())
	serverConf := Config{}
	serverConf, err := serverConf.WithGlobals()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1000), serverConf.MaxConnections)
	assert.Equal(t, time.Duration(1000000), serverConf.ConnReadTimeout)
	assert.Equal(t, time.Duration(1000000), serverConf.ConnWriteTimeout)
}
