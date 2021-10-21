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

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql/config"
)

func TestInitSystemVariablesWithDefaults(t *testing.T) {

	tests := []struct {
		name        string
		defaults    map[string]string
		err         bool
		expectedCmp map[string]interface{}
	}{
		{"set max_connections", map[string]string{"max_connections": "1000"}, false, map[string]interface{}{"max_connections": int64(1000)}},
		{"set two variables", map[string]string{"max_connections": "1000", "net_read_timeout": "1"}, false, map[string]interface{}{"max_connections": int64(1000), "net_read_timeout": int64(1)}},
		{"decode bad type", map[string]string{"net_read_timeout": "string"}, true, nil},
		{"unknown system variable", map[string]string{"noexist": "1000"}, true, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defaults := config.NewMapConfig(test.defaults)
			err := InitSystemVariablesWithDefaults(defaults)
			if test.err {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			for sysVarName, _ := range test.defaults {
				_, val, _ := SystemVariables.GetGlobal(sysVarName)
				assert.Equal(t, test.expectedCmp[sysVarName], val)
			}
		})
	}
}
