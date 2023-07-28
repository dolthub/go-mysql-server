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

package mysql_db

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// This test enforces not only that RoleEdge round trips, but that the output is as expected.
func TestRoleEdgeJson(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testRoleEdge := &RoleEdge{
		FromHost:        "localhost",
		FromUser:        "some_role",
		ToHost:          "127.0.0.1",
		ToUser:          "some_user",
		WithAdminOption: true,
	}
	jsonStr, err := testRoleEdge.ToJson(ctx)
	require.NoError(t, err)
	require.Equal(t, `{"FromHost":"localhost","FromUser":"some_role","ToHost":"127.0.0.1","ToUser":"some_user","WithAdminOption":true}`, jsonStr)
	newRoleEdge, err := (&RoleEdge{}).FromJson(ctx, jsonStr)
	require.NoError(t, err)
	require.True(t, RoleEdgeEquals(testRoleEdge, newRoleEdge))

	testSlice := []*RoleEdge{testRoleEdge}
	jsonData, err := json.Marshal(testSlice)
	require.NoError(t, err)
	var newSlice []*RoleEdge
	err = json.Unmarshal(jsonData, &newSlice)
	require.NoError(t, err)
	require.Len(t, newSlice, len(testSlice))
	for i := range testSlice {
		require.True(t, RoleEdgeEquals(testSlice[i], newSlice[i]))
	}
}
