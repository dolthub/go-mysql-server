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

package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func newPersistedSqlContext() *sql.Context {
	ctx, _ := context.WithCancel(context.TODO())
	pro := NewDBProvider()
	sess := sql.NewBaseSession()

	persistedGlobals := GlobalsMap{"max_connections": 1000, "net_read_timeout": 1000, "auto_increment_increment": 123}
	persistedSess := NewSession(sess, pro).SetGlobals(persistedGlobals)
	sqlCtx := sql.NewContext(ctx)
	sqlCtx.Session = persistedSess
	return sqlCtx
}

func TestInitPersistedSession(t *testing.T) {
	sqlCtx := newPersistedSqlContext()
	pg := sqlCtx.Session.(*Session).persistedGlobals
	res, ok := pg["max_connections"]
	require.True(t, ok)
	assert.Equal(t, 1000, res)
}

func TestPersistVariable(t *testing.T) {
	persistTests := []struct {
		title       string
		name        string
		value       interface{}
		err         *errors.Kind
		expectedCmp interface{}
	}{
		{"set variable", "max_connections", int64(10), nil, int64(10)},
		{"set bad variable", "nonexistent_var", int64(10), sql.ErrUnknownSystemVariable, nil},
	}

	for _, test := range persistTests {
		t.Run(test.title, func(t *testing.T) {
			sqlCtx := newPersistedSqlContext()
			sess := sqlCtx.Session.(*Session)
			err := sqlCtx.Session.(sql.PersistableSession).PersistGlobal(test.name, test.value)
			if test.err != nil {
				assert.True(t, test.err.Is(err))
			} else {
				require.NoError(t, err)
				res := sess.persistedGlobals[test.name]
				assert.Equal(t, test.expectedCmp, res)
			}
		})
	}
}

func TestRemoveGlobal(t *testing.T) {
	sqlCtx := newPersistedSqlContext()
	sess := sqlCtx.Session.(*Session)

	key := "auto_increment_increment"
	err := sqlCtx.Session.(sql.PersistableSession).RemovePersistedGlobal(key)
	require.NoError(t, err)

	res := sess.persistedGlobals[key]
	assert.Equal(t, nil, res)
	assert.Equal(t, 2, len(sess.persistedGlobals))
}

func TestRemoveAllGlobals(t *testing.T) {
	sqlCtx := newPersistedSqlContext()
	sess := sqlCtx.Session.(*Session)
	err := sqlCtx.Session.(sql.PersistableSession).RemoveAllPersistedGlobals()
	require.NoError(t, err)
	assert.Equal(t, 0, len(sess.persistedGlobals))
}
