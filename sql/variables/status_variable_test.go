// Copyright 2024 Dolthub, Inc.
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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestStatusVariables(t *testing.T) {
	defer InitStatusVariables()

	require := require.New(t)
	ctx := sql.NewEmptyContext()
	sess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz", User: "bar"}, 1)

	sessVal, err := sess.GetStatusVariable(ctx, "Aborted_clients")
	require.NoError(err)
	require.Equal(int64(0), sessVal)

	_, globalVal, ok := sql.StatusVariables.GetGlobal("Aborted_clients")
	require.True(ok)
	require.Equal(int64(0), globalVal)

	sess.SetStatusVariable(ctx, "Aborted_clients", int64(100))

	sessVal, err = sess.GetStatusVariable(ctx, "Aborted_clients")
	require.NoError(err)
	require.Equal(int64(100), sessVal)

	_, globalVal, ok = sql.StatusVariables.GetGlobal("Aborted_clients")
	require.True(ok)
	require.Equal(int64(0), globalVal)
}