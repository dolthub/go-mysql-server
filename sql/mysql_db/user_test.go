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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// This test enforces that the User round trips.
func TestUserJson(t *testing.T) {
	// Time is converted differently by the JSON functions depending on the OS, therefore a string comparison cannot
	// be made without additional modifications.
	ctx := sql.NewEmptyContext()
	testUser := &User{
		User:                "tester",
		Host:                "localhost",
		PrivilegeSet:        NewPrivilegeSet(),
		Plugin:              "mysql_native_password",
		Password:            "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
		PasswordLastChanged: time.Unix(184301, 0),
		Locked:              false,
		Attributes:          nil,
		IsRole:              false,
	}
	testUser.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Super)
	testUser.PrivilegeSet.AddDatabase("some_db", sql.PrivilegeType_Select, sql.PrivilegeType_Insert)
	testUser.PrivilegeSet.AddTable("other_db", "some_tbl", sql.PrivilegeType_Update, sql.PrivilegeType_Delete)
	testUser.PrivilegeSet.AddColumn("some_db", "other_tbl", "some_col", sql.PrivilegeType_Create, sql.PrivilegeType_Drop)
	jsonStr, err := testUser.ToJson(ctx)
	require.NoError(t, err)
	newUser, err := (User{}).FromJson(ctx, jsonStr)
	require.NoError(t, err)
	require.True(t, UserEquals(testUser, newUser))

	testSlice := []*User{testUser}
	jsonData, err := json.Marshal(testSlice)
	require.NoError(t, err)
	var newSlice []*User
	err = json.Unmarshal(jsonData, &newSlice)
	require.NoError(t, err)
	require.Len(t, newSlice, len(testSlice))
	for i := range testSlice {
		require.True(t, UserEquals(testSlice[i], newSlice[i]))
	}
}
