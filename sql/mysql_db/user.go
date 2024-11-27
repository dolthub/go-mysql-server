// Copyright 2021-2022 Dolthub, Inc.
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
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// User represents a user from the user Grant Table.
type User struct {
	User                string
	Host                string
	PrivilegeSet        PrivilegeSet
	Plugin              string
	Password            string
	PasswordLastChanged time.Time
	Locked              bool
	Attributes          *string
	Identity            string
	IsSuperUser         bool
	//TODO: add the remaining fields

	// IsRole is an additional field that states whether the User represents a role or user. In MySQL this must be a
	// hidden column, therefore it's represented here as an additional field.
	IsRole bool
}

func UserToRow(ctx *sql.Context, u *User) (sql.Row, error) {
	row := sql.NewSqlRowWithLen(len(userTblSchema))
	var err error
	for i, col := range userTblSchema {
		var v interface{}
		v, err = col.Default.Eval(ctx, nil)
		row.SetValue(i, v)
		if err != nil {
			panic(err) // Should never happen, schema is static
		}
	}
	//TODO: once the remaining fields are added, fill those in as well
	row.SetValue(userTblColIndex_User, u.User)
	row.SetValue(userTblColIndex_Host, u.Host)
	row.SetValue(userTblColIndex_plugin, u.Plugin)
	row.SetValue(userTblColIndex_authentication_string, u.Password)
	row.SetValue(userTblColIndex_password_last_changed, u.PasswordLastChanged)
	row.SetValue(userTblColIndex_identity, u.Identity)
	if u.Locked {
		row.SetValue(userTblColIndex_account_locked, uint16(2))
	}
	if u.Attributes != nil {
		row.SetValue(userTblColIndex_User_attributes, *u.Attributes)
	}
	u.privSetToRow(ctx, row)
	return row, nil
}

func UserFromRow(ctx *sql.Context, row sql.Row) (*User, error) {
	if err := userTblSchema.CheckRow(row); err != nil {
		return nil, err
	}
	//TODO: once the remaining fields are added, fill those in as well
	var attributes *string
	passwordLastChanged := time.Now().UTC()
	if val, ok := row.GetValue(userTblColIndex_User_attributes).(string); ok {
		attributes = &val
	}
	if val, ok := row.GetValue(userTblColIndex_password_last_changed).(time.Time); ok {
		passwordLastChanged = val
	}
	return &User{
		User:                row.GetValue(userTblColIndex_User).(string),
		Host:                row.GetValue(userTblColIndex_Host).(string),
		PrivilegeSet:        UserRowToPrivSet(ctx, row),
		Plugin:              row.GetValue(userTblColIndex_plugin).(string),
		Password:            row.GetValue(userTblColIndex_authentication_string).(string),
		PasswordLastChanged: passwordLastChanged,
		Locked:              row.GetValue(userTblColIndex_account_locked).(uint16) == 2,
		Attributes:          attributes,
		Identity:            row.GetValue(userTblColIndex_identity).(string),
		IsRole:              false,
	}, nil
}

func UserUpdateWithRow(ctx *sql.Context, row sql.Row, u *User) (*User, error) {
	updatedUser, err := UserFromRow(ctx, row)
	if err != nil {
		return nil, err
	}
	updatedUser.IsRole = u.IsRole
	return updatedUser, nil
}

var UserOps = in_mem_table.ValueOps[*User]{
	ToRow:         UserToRow,
	FromRow:       UserFromRow,
	UpdateWithRow: UserUpdateWithRow,
}

func UserEquals(left, right *User) bool {
	// IsRole is not tested for equality, as it is additional information
	//TODO: once the remaining fields are added, fill those in as well
	if left.User != right.User ||
		left.Host != right.Host ||
		left.Plugin != right.Plugin ||
		left.Password != right.Password ||
		left.Identity != right.Identity ||
		!left.PasswordLastChanged.Equal(right.PasswordLastChanged) ||
		left.Locked != right.Locked ||
		!left.PrivilegeSet.Equals(right.PrivilegeSet) ||
		left.Attributes == nil && right.Attributes != nil ||
		left.Attributes != nil && right.Attributes == nil ||
		(left.Attributes != nil && *left.Attributes != *right.Attributes) {
		return false
	}
	return true
}

func UserCopy(u *User) *User {
	uu := *u
	uu.PrivilegeSet = NewPrivilegeSet()
	uu.PrivilegeSet.UnionWith(u.PrivilegeSet)
	return &uu
}

// FromJson implements the interface in_mem_table.Entry.
func (u User) FromJson(ctx *sql.Context, jsonStr string) (*User, error) {
	newUser := &User{}
	if err := json.Unmarshal([]byte(jsonStr), newUser); err != nil {
		return nil, err
	}
	return newUser, nil
}

// ToJson implements the interface in_mem_table.Entry.
func (u *User) ToJson(ctx *sql.Context) (string, error) {
	jsonData, err := json.Marshal(*u)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

// UserHostToString returns the user and host as a formatted string using the quotes given. Using the default root
// account with the backtick as the quote, root@localhost would become `root`@`localhost`. Different quotes are used
// in different places in MySQL. In addition, if the quote is used in a section as part of the name, it is escaped by
// doubling the quote (which also mimics MySQL behavior).
func (u User) UserHostToString(quote string) string {
	replacement := quote + quote
	user := strings.ReplaceAll(u.User, quote, replacement)
	host := strings.ReplaceAll(u.Host, quote, replacement)
	return fmt.Sprintf("%s%s%s@%s%s%s", quote, user, quote, quote, host, quote)
}

func UserRowToPrivSet(ctx *sql.Context, row sql.Row) PrivilegeSet {
	privSet := NewPrivilegeSet()
	for i, val := range row.Values() {
		switch i {
		case userTblColIndex_Select_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Select)
			}
		case userTblColIndex_Insert_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Insert)
			}
		case userTblColIndex_Update_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Update)
			}
		case userTblColIndex_Delete_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Delete)
			}
		case userTblColIndex_Create_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Create)
			}
		case userTblColIndex_Drop_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Drop)
			}
		case userTblColIndex_Reload_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Reload)
			}
		case userTblColIndex_Shutdown_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Shutdown)
			}
		case userTblColIndex_Process_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Process)
			}
		case userTblColIndex_File_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_File)
			}
		case userTblColIndex_Grant_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_GrantOption)
			}
		case userTblColIndex_References_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_References)
			}
		case userTblColIndex_Index_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Index)
			}
		case userTblColIndex_Alter_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Alter)
			}
		case userTblColIndex_Show_db_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_ShowDB)
			}
		case userTblColIndex_Super_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Super)
			}
		case userTblColIndex_Create_tmp_table_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateTempTable)
			}
		case userTblColIndex_Lock_tables_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_LockTables)
			}
		case userTblColIndex_Execute_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Execute)
			}
		case userTblColIndex_Repl_slave_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_ReplicationSlave)
			}
		case userTblColIndex_Repl_client_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_ReplicationClient)
			}
		case userTblColIndex_Create_view_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateView)
			}
		case userTblColIndex_Show_view_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_ShowView)
			}
		case userTblColIndex_Create_routine_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateRoutine)
			}
		case userTblColIndex_Alter_routine_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_AlterRoutine)
			}
		case userTblColIndex_Create_user_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateUser)
			}
		case userTblColIndex_Event_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Event)
			}
		case userTblColIndex_Trigger_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_Trigger)
			}
		case userTblColIndex_Create_tablespace_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateTablespace)
			}
		case userTblColIndex_Create_role_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_CreateRole)
			}
		case userTblColIndex_Drop_role_priv:
			if val.(uint16) == 2 {
				privSet.AddGlobalStatic(sql.PrivilegeType_DropRole)
			}
		}
	}
	return privSet
}

// privSetToRow applies the this User's set of privileges to the given row. Only sets privileges that exist to "Y",
// therefore any privileges that do not exist will have their default values.
func (u *User) privSetToRow(ctx *sql.Context, row sql.Row) {
	for _, priv := range u.PrivilegeSet.ToSlice() {
		switch priv {
		case sql.PrivilegeType_Select:
			row.SetValue(userTblColIndex_Select_priv, uint16(2))
		case sql.PrivilegeType_Insert:
			row.SetValue(userTblColIndex_Insert_priv, uint16(2))
		case sql.PrivilegeType_Update:
			row.SetValue(userTblColIndex_Update_priv, uint16(2))
		case sql.PrivilegeType_Delete:
			row.SetValue(userTblColIndex_Delete_priv, uint16(2))
		case sql.PrivilegeType_Create:
			row.SetValue(userTblColIndex_Create_priv, uint16(2))
		case sql.PrivilegeType_Drop:
			row.SetValue(userTblColIndex_Drop_priv, uint16(2))
		case sql.PrivilegeType_Reload:
			row.SetValue(userTblColIndex_Reload_priv, uint16(2))
		case sql.PrivilegeType_Shutdown:
			row.SetValue(userTblColIndex_Shutdown_priv, uint16(2))
		case sql.PrivilegeType_Process:
			row.SetValue(userTblColIndex_Process_priv, uint16(2))
		case sql.PrivilegeType_File:
			row.SetValue(userTblColIndex_File_priv, uint16(2))
		case sql.PrivilegeType_GrantOption:
			row.SetValue(userTblColIndex_Grant_priv, uint16(2))
		case sql.PrivilegeType_References:
			row.SetValue(userTblColIndex_References_priv, uint16(2))
		case sql.PrivilegeType_Index:
			row.SetValue(userTblColIndex_Index_priv, uint16(2))
		case sql.PrivilegeType_Alter:
			row.SetValue(userTblColIndex_Alter_priv, uint16(2))
		case sql.PrivilegeType_ShowDB:
			row.SetValue(userTblColIndex_Show_db_priv, uint16(2))
		case sql.PrivilegeType_Super:
			row.SetValue(userTblColIndex_Super_priv, uint16(2))
		case sql.PrivilegeType_CreateTempTable:
			row.SetValue(userTblColIndex_Create_tmp_table_priv, uint16(2))
		case sql.PrivilegeType_LockTables:
			row.SetValue(userTblColIndex_Lock_tables_priv, uint16(2))
		case sql.PrivilegeType_Execute:
			row.SetValue(userTblColIndex_Execute_priv, uint16(2))
		case sql.PrivilegeType_ReplicationSlave:
			row.SetValue(userTblColIndex_Repl_slave_priv, uint16(2))
		case sql.PrivilegeType_ReplicationClient:
			row.SetValue(userTblColIndex_Repl_client_priv, uint16(2))
		case sql.PrivilegeType_CreateView:
			row.SetValue(userTblColIndex_Create_view_priv, uint16(2))
		case sql.PrivilegeType_ShowView:
			row.SetValue(userTblColIndex_Show_view_priv, uint16(2))
		case sql.PrivilegeType_CreateRoutine:
			row.SetValue(userTblColIndex_Create_routine_priv, uint16(2))
		case sql.PrivilegeType_AlterRoutine:
			row.SetValue(userTblColIndex_Alter_routine_priv, uint16(2))
		case sql.PrivilegeType_CreateUser:
			row.SetValue(userTblColIndex_Create_user_priv, uint16(2))
		case sql.PrivilegeType_Event:
			row.SetValue(userTblColIndex_Event_priv, uint16(2))
		case sql.PrivilegeType_Trigger:
			row.SetValue(userTblColIndex_Trigger_priv, uint16(2))
		case sql.PrivilegeType_CreateTablespace:
			row.SetValue(userTblColIndex_Create_tablespace_priv, uint16(2))
		case sql.PrivilegeType_CreateRole:
			row.SetValue(userTblColIndex_Create_role_priv, uint16(2))
		case sql.PrivilegeType_DropRole:
			row.SetValue(userTblColIndex_Drop_role_priv, uint16(2))
		}
	}
}
