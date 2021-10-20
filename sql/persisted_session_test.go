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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql/config"
)

func newPersistedSqlContext() (*Context, config.ReadWriteConfig) {
	ctx, _ := context.WithCancel(context.TODO())
	sess := NewBaseSession()
	conf := config.NewMapConfig(map[string]string{"max_connections": "1000"})
	persistedSess := NewPersistedSession(sess, conf)
	sqlCtx := NewContext(ctx)
	sqlCtx.Session = persistedSess
	return sqlCtx, conf
}

func TestInitPersistedSession(t *testing.T) {
	sqlCtx, _ := newPersistedSqlContext()
	res, err := sqlCtx.Session.(*PersistedSession).defaultsConf.GetString("max_connections")
	require.NoError(t, err)
	assert.Equal(t, "1000", res)
}

func TestPersistVariable(t *testing.T) {
	persistTests := []struct {
		title       string
		name        string
		value       interface{}
		err         *errors.Kind
		expectedCmp string
	}{
		{"set variable", "max_connections", 10, nil, "10"},
		{"set bad variable", "nonexistent_var", 10, ErrUnknownSystemVariable, ""},
	}

	for _, test := range persistTests {
		t.Run(test.title, func(t *testing.T) {
			sqlCtx, conf := newPersistedSqlContext()
			err := sqlCtx.Session.(PersistableSession).PersistVariable(sqlCtx, test.name, test.value)
			if test.err != nil {
				assert.True(t, test.err.Is(err))
			} else {
				require.NoError(t, err)
				res := conf.GetStringOrDefault(test.name, "")
				assert.Equal(t, test.expectedCmp, res)
			}
		})
	}
}

func TestResetPersistVariable(t *testing.T) {
	sqlCtx, conf := newPersistedSqlContext()
	key := "auto_increment_increment"
	conf.SetStrings(map[string]string{key: "123"})
	err := sqlCtx.Session.(PersistableSession).ResetPersistVariable(sqlCtx, key)
	require.NoError(t, err)

	res := conf.GetStringOrDefault(key, "")
	assert.Equal(t, "", res)
}

func TestResetPersistAll(t *testing.T) {
	sqlCtx, conf := newPersistedSqlContext()
	key := "auto_increment_increment"
	conf.SetStrings(map[string]string{key: "123"})
	err := sqlCtx.Session.(PersistableSession).ResetPersistAll(sqlCtx)
	require.NoError(t, err)
	res := conf.GetStringOrDefault(key, "")
	assert.Equal(t, "", res)
}
