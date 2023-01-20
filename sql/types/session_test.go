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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestSessionConfig(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	sess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz", User: "bar"}, 1)
	typ, v, err := sess.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Nil(typ)
	require.Equal(nil, v)

	err = sess.SetUserVariable(ctx, "foo", int64(1), Int64)
	require.NoError(err)

	typ, v, err = sess.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(Int64, typ)
	require.Equal(int64(1), v)

	err = sess.SetUserVariable(ctx, "foo", nil, Int64)
	require.NoError(err)

	typ, v, err = sess.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(Int64, typ)
	require.Equal(nil, v)

	require.Equal(uint16(0), sess.WarningCount())

	sess.Warn(&sql.Warning{Code: 1})
	sess.Warn(&sql.Warning{Code: 2})
	sess.Warn(&sql.Warning{Code: 3})

	require.Equal(uint16(3), sess.WarningCount())

	require.Equal(3, sess.Warnings()[0].Code)
	require.Equal(2, sess.Warnings()[1].Code)
	require.Equal(1, sess.Warnings()[2].Code)
}
