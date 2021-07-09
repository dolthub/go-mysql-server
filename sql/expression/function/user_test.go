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

package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestUser(t *testing.T) {
	userFunc := sql.NewFunction0("user", NewUser)
	fn := userFunc.Fn

	session := sql.NewSession("server", sql.Client{Address: "client", User: "root"}, 0)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err := fn(ctx).Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "root@client", user)

	session = sql.NewSession("server", sql.Client{Address: "client", User: "someguy"}, 0)
	ctx = sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err = fn(ctx).Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "someguy@client", user)

	ctx = sql.NewEmptyContext()

	user, err = fn(ctx).Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "", user)
}
