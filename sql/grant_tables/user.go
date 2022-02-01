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

package grant_tables

import (
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
	PrivilegeSet        UserGlobalStaticPrivileges
	Plugin              string
	Password            string
	PasswordLastChanged time.Time
	Locked              bool
	Attributes          *string
	//TODO: add the remaining fields

	// IsRole is an additional field that states whether the User represents a role or user. In MySQL this must be a
	// hidden column, therefore it's represented here as an additional field.
	IsRole bool
}

var _ in_mem_table.Entry = (*User)(nil)

// NewFromRow implements the interface in_mem_table.Entry.
func (u *User) NewFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	if err := userTblSchema.CheckRow(row); err != nil {
		return nil, err
	}
	//TODO: once the remaining fields are added, fill those in as well
	var attributes *string
	passwordLastChanged := time.Now().UTC()
	if val, ok := row[userTblColIndex_User_attributes].(string); ok {
		attributes = &val
	}
	if val, ok := row[userTblColIndex_password_last_changed].(time.Time); ok {
		passwordLastChanged = val
	}
	return &User{
		User:                row[userTblColIndex_User].(string),
		Host:                row[userTblColIndex_Host].(string),
		PrivilegeSet:        u.rowToPrivSet(ctx, row),
		Plugin:              row[userTblColIndex_plugin].(string),
		Password:            row[userTblColIndex_authentication_string].(string),
		PasswordLastChanged: passwordLastChanged,
		Locked:              row[userTblColIndex_account_locked].(string) == "Y",
		Attributes:          attributes,
		IsRole:              false,
	}, nil
}

// UpdateFromRow implements the interface in_mem_table.Entry.
func (u *User) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	updatedEntry, err := u.NewFromRow(ctx, row)
	if err != nil {
		return nil, err
	}
	updatedEntry.(*User).IsRole = u.IsRole
	return updatedEntry, nil
}

// ToRow implements the interface in_mem_table.Entry.
func (u *User) ToRow(ctx *sql.Context) sql.Row {
	row := make(sql.Row, len(userTblSchema))
	var err error
	for i, col := range userTblSchema {
		row[i], err = col.Default.Eval(ctx, nil)
		if err != nil {
			panic(err) // Should never happen, schema is static
		}
	}
	//TODO: once the remaining fields are added, fill those in as well
	row[userTblColIndex_User] = u.User
	row[userTblColIndex_Host] = u.Host
	row[userTblColIndex_plugin] = u.Plugin
	row[userTblColIndex_authentication_string] = u.Password
	row[userTblColIndex_password_last_changed] = u.PasswordLastChanged
	if u.Locked {
		row[userTblColIndex_account_locked] = "Y"
	}
	if u.Attributes != nil {
		row[userTblColIndex_User_attributes] = *u.Attributes
	}
	u.privSetToRow(ctx, row)
	return row
}

// Equals implements the interface in_mem_table.Entry.
func (u *User) Equals(ctx *sql.Context, otherEntry in_mem_table.Entry) bool {
	otherUser, ok := otherEntry.(*User)
	if !ok {
		return false
	}
	// IsRole is not tested for equality, as it is additional information
	//TODO: once the remaining fields are added, fill those in as well
	if u.User != otherUser.User ||
		u.Host != otherUser.Host ||
		u.Plugin != otherUser.Plugin ||
		u.Password != otherUser.Password ||
		!u.PasswordLastChanged.Equal(otherUser.PasswordLastChanged) ||
		u.Locked != otherUser.Locked ||
		!u.PrivilegeSet.Equals(otherUser.PrivilegeSet) ||
		u.Attributes == nil && otherUser.Attributes != nil ||
		u.Attributes != nil && otherUser.Attributes == nil ||
		(u.Attributes != nil && *u.Attributes != *otherUser.Attributes) {
		return false
	}
	return true
}

// UserHostToString returns the user and host as a formatted string using the quotes given. If a replacement is given,
// then also replaces any existing instances of the quotes with the replacement.
func (u *User) UserHostToString(quote string, replacement string) string {
	user := u.User
	host := u.Host
	if len(replacement) > 0 {
		user = strings.ReplaceAll(user, quote, replacement)
		host = strings.ReplaceAll(host, quote, replacement)
	}
	return fmt.Sprintf("%s%s%s@%s%s%s", quote, user, quote, quote, host, quote)
}

// rowToPrivSet returns a set of privileges for the given row.
func (u *User) rowToPrivSet(ctx *sql.Context, row sql.Row) UserGlobalStaticPrivileges {
	privSet := NewUserGlobalStaticPrivileges()
	for i, val := range row {
		switch i {
		case userTblColIndex_Select_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Select)
			}
		case userTblColIndex_Insert_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Insert)
			}
		case userTblColIndex_Update_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Update)
			}
		case userTblColIndex_Delete_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Delete)
			}
		case userTblColIndex_Create_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Create)
			}
		case userTblColIndex_Drop_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Drop)
			}
		case userTblColIndex_Reload_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Reload)
			}
		case userTblColIndex_Shutdown_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Shutdown)
			}
		case userTblColIndex_Process_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Process)
			}
		case userTblColIndex_File_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_File)
			}
		case userTblColIndex_Grant_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Grant)
			}
		case userTblColIndex_References_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_References)
			}
		case userTblColIndex_Index_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Index)
			}
		case userTblColIndex_Alter_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Alter)
			}
		case userTblColIndex_Show_db_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_ShowDB)
			}
		case userTblColIndex_Super_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Super)
			}
		case userTblColIndex_Create_tmp_table_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateTempTable)
			}
		case userTblColIndex_Lock_tables_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_LockTables)
			}
		case userTblColIndex_Execute_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Execute)
			}
		case userTblColIndex_Repl_slave_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_ReplicationSlave)
			}
		case userTblColIndex_Repl_client_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_ReplicationClient)
			}
		case userTblColIndex_Create_view_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateView)
			}
		case userTblColIndex_Show_view_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_ShowView)
			}
		case userTblColIndex_Create_routine_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateRoutine)
			}
		case userTblColIndex_Alter_routine_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_AlterRoutine)
			}
		case userTblColIndex_Create_user_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateUser)
			}
		case userTblColIndex_Event_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Event)
			}
		case userTblColIndex_Trigger_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_Trigger)
			}
		case userTblColIndex_Create_tablespace_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateTablespace)
			}
		case userTblColIndex_Create_role_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_CreateRole)
			}
		case userTblColIndex_Drop_role_priv:
			if val.(string) == "Y" {
				privSet.Add(PrivilegeType_DropRole)
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
		case PrivilegeType_Select:
			row[userTblColIndex_Select_priv] = "Y"
		case PrivilegeType_Insert:
			row[userTblColIndex_Insert_priv] = "Y"
		case PrivilegeType_Update:
			row[userTblColIndex_Update_priv] = "Y"
		case PrivilegeType_Delete:
			row[userTblColIndex_Delete_priv] = "Y"
		case PrivilegeType_Create:
			row[userTblColIndex_Create_priv] = "Y"
		case PrivilegeType_Drop:
			row[userTblColIndex_Drop_priv] = "Y"
		case PrivilegeType_Reload:
			row[userTblColIndex_Reload_priv] = "Y"
		case PrivilegeType_Shutdown:
			row[userTblColIndex_Shutdown_priv] = "Y"
		case PrivilegeType_Process:
			row[userTblColIndex_Process_priv] = "Y"
		case PrivilegeType_File:
			row[userTblColIndex_File_priv] = "Y"
		case PrivilegeType_Grant:
			row[userTblColIndex_Grant_priv] = "Y"
		case PrivilegeType_References:
			row[userTblColIndex_References_priv] = "Y"
		case PrivilegeType_Index:
			row[userTblColIndex_Index_priv] = "Y"
		case PrivilegeType_Alter:
			row[userTblColIndex_Alter_priv] = "Y"
		case PrivilegeType_ShowDB:
			row[userTblColIndex_Show_db_priv] = "Y"
		case PrivilegeType_Super:
			row[userTblColIndex_Super_priv] = "Y"
		case PrivilegeType_CreateTempTable:
			row[userTblColIndex_Create_tmp_table_priv] = "Y"
		case PrivilegeType_LockTables:
			row[userTblColIndex_Lock_tables_priv] = "Y"
		case PrivilegeType_Execute:
			row[userTblColIndex_Execute_priv] = "Y"
		case PrivilegeType_ReplicationSlave:
			row[userTblColIndex_Repl_slave_priv] = "Y"
		case PrivilegeType_ReplicationClient:
			row[userTblColIndex_Repl_client_priv] = "Y"
		case PrivilegeType_CreateView:
			row[userTblColIndex_Create_view_priv] = "Y"
		case PrivilegeType_ShowView:
			row[userTblColIndex_Show_view_priv] = "Y"
		case PrivilegeType_CreateRoutine:
			row[userTblColIndex_Create_routine_priv] = "Y"
		case PrivilegeType_AlterRoutine:
			row[userTblColIndex_Alter_routine_priv] = "Y"
		case PrivilegeType_CreateUser:
			row[userTblColIndex_Create_user_priv] = "Y"
		case PrivilegeType_Event:
			row[userTblColIndex_Event_priv] = "Y"
		case PrivilegeType_Trigger:
			row[userTblColIndex_Trigger_priv] = "Y"
		case PrivilegeType_CreateTablespace:
			row[userTblColIndex_Create_tablespace_priv] = "Y"
		case PrivilegeType_CreateRole:
			row[userTblColIndex_Create_role_priv] = "Y"
		case PrivilegeType_DropRole:
			row[userTblColIndex_Drop_role_priv] = "Y"
		}
	}
}

// PrivilegeType represents a privilege.
type PrivilegeType int

const (
	PrivilegeType_Select PrivilegeType = iota
	PrivilegeType_Insert
	PrivilegeType_Update
	PrivilegeType_Delete
	PrivilegeType_Create
	PrivilegeType_Drop
	PrivilegeType_Reload
	PrivilegeType_Shutdown
	PrivilegeType_Process
	PrivilegeType_File
	PrivilegeType_Grant
	PrivilegeType_References
	PrivilegeType_Index
	PrivilegeType_Alter
	PrivilegeType_ShowDB
	PrivilegeType_Super
	PrivilegeType_CreateTempTable
	PrivilegeType_LockTables
	PrivilegeType_Execute
	PrivilegeType_ReplicationSlave
	PrivilegeType_ReplicationClient
	PrivilegeType_CreateView
	PrivilegeType_ShowView
	PrivilegeType_CreateRoutine
	PrivilegeType_AlterRoutine
	PrivilegeType_CreateUser
	PrivilegeType_Event
	PrivilegeType_Trigger
	PrivilegeType_CreateTablespace
	PrivilegeType_CreateRole
	PrivilegeType_DropRole
)
