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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// User represents a user from the user Grant Table.
type User struct {
	User                string
	Host                string
	PrivilegeSet        map[PrivilegeType]struct{}
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
	if val, ok := row[userTblColIdxMap["User_attributes"]].(string); ok {
		attributes = &val
	}
	if val, ok := row[userTblColIdxMap["password_last_changed"]].(time.Time); ok {
		passwordLastChanged = val
	}
	return &User{
		User:                row[userTblColIdxMap["User"]].(string),
		Host:                row[userTblColIdxMap["Host"]].(string),
		PrivilegeSet:        u.rowToPrivSet(ctx, row),
		Plugin:              row[userTblColIdxMap["plugin"]].(string),
		Password:            row[userTblColIdxMap["authentication_string"]].(string),
		PasswordLastChanged: passwordLastChanged,
		Locked:              row[userTblColIdxMap["account_locked"]].(string) == "Y",
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
	row[userTblColIdxMap["User"]] = u.User
	row[userTblColIdxMap["Host"]] = u.Host
	row[userTblColIdxMap["plugin"]] = u.Plugin
	row[userTblColIdxMap["authentication_string"]] = u.Password
	row[userTblColIdxMap["password_last_changed"]] = u.PasswordLastChanged
	if u.Locked {
		row[userTblColIdxMap["account_locked"]] = "Y"
	}
	if u.Attributes != nil {
		row[userTblColIdxMap["User_attributes"]] = *u.Attributes
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
		u.Locked != otherUser.Locked {
		return false
	}
	if len(u.PrivilegeSet) != len(otherUser.PrivilegeSet) {
		return false
	}
	for priv := range u.PrivilegeSet {
		if _, ok := otherUser.PrivilegeSet[priv]; !ok {
			return false
		}
	}
	if u.Attributes == nil && otherUser.Attributes != nil ||
		u.Attributes != nil && otherUser.Attributes == nil ||
		(u.Attributes != nil && *u.Attributes != *otherUser.Attributes) {
		return false
	}
	return true
}

// rowToPrivSet returns a set of privileges for the given row.
func (u *User) rowToPrivSet(ctx *sql.Context, row sql.Row) map[PrivilegeType]struct{} {
	privSet := make(map[PrivilegeType]struct{})
	for i, val := range row {
		switch i {
		case userTblColIdxMap["Select_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Select] = struct{}{}
			}
		case userTblColIdxMap["Insert_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Insert] = struct{}{}
			}
		case userTblColIdxMap["Update_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Update] = struct{}{}
			}
		case userTblColIdxMap["Delete_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Delete] = struct{}{}
			}
		case userTblColIdxMap["Create_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Create] = struct{}{}
			}
		case userTblColIdxMap["Drop_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Drop] = struct{}{}
			}
		case userTblColIdxMap["Reload_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Reload] = struct{}{}
			}
		case userTblColIdxMap["Shutdown_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Shutdown] = struct{}{}
			}
		case userTblColIdxMap["Process_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Process] = struct{}{}
			}
		case userTblColIdxMap["File_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_File] = struct{}{}
			}
		case userTblColIdxMap["Grant_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Grant] = struct{}{}
			}
		case userTblColIdxMap["References_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_References] = struct{}{}
			}
		case userTblColIdxMap["Index_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Index] = struct{}{}
			}
		case userTblColIdxMap["Alter_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Alter] = struct{}{}
			}
		case userTblColIdxMap["Show_db_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_ShowDB] = struct{}{}
			}
		case userTblColIdxMap["Super_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Super] = struct{}{}
			}
		case userTblColIdxMap["Create_tmp_table_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateTempTable] = struct{}{}
			}
		case userTblColIdxMap["Lock_tables_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_LockTables] = struct{}{}
			}
		case userTblColIdxMap["Execute_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Execute] = struct{}{}
			}
		case userTblColIdxMap["Repl_slave_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_ReplicationSlave] = struct{}{}
			}
		case userTblColIdxMap["Repl_client_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_ReplicationClient] = struct{}{}
			}
		case userTblColIdxMap["Create_view_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateView] = struct{}{}
			}
		case userTblColIdxMap["Show_view_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_ShowView] = struct{}{}
			}
		case userTblColIdxMap["Create_routine_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateRoutine] = struct{}{}
			}
		case userTblColIdxMap["Alter_routine_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_AlterRoutine] = struct{}{}
			}
		case userTblColIdxMap["Create_user_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateUser] = struct{}{}
			}
		case userTblColIdxMap["Event_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Event] = struct{}{}
			}
		case userTblColIdxMap["Trigger_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_Trigger] = struct{}{}
			}
		case userTblColIdxMap["Create_tablespace_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateTablespace] = struct{}{}
			}
		case userTblColIdxMap["Create_role_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_CreateRole] = struct{}{}
			}
		case userTblColIdxMap["Drop_role_priv"]:
			if val.(string) == "Y" {
				privSet[PrivilegeType_DropRole] = struct{}{}
			}
		}
	}
	return privSet
}

// privSetToRow applies the this User's set of privileges to the given row. Only sets privileges that exist to "Y",
// therefore any privileges that do not exist will have their default values.
func (u *User) privSetToRow(ctx *sql.Context, row sql.Row) {
	for priv := range u.PrivilegeSet {
		switch priv {
		case PrivilegeType_Select:
			row[userTblColIdxMap["Select_priv"]] = "Y"
		case PrivilegeType_Insert:
			row[userTblColIdxMap["Insert_priv"]] = "Y"
		case PrivilegeType_Update:
			row[userTblColIdxMap["Update_priv"]] = "Y"
		case PrivilegeType_Delete:
			row[userTblColIdxMap["Delete_priv"]] = "Y"
		case PrivilegeType_Create:
			row[userTblColIdxMap["Create_priv"]] = "Y"
		case PrivilegeType_Drop:
			row[userTblColIdxMap["Drop_priv"]] = "Y"
		case PrivilegeType_Reload:
			row[userTblColIdxMap["Reload_priv"]] = "Y"
		case PrivilegeType_Shutdown:
			row[userTblColIdxMap["Shutdown_priv"]] = "Y"
		case PrivilegeType_Process:
			row[userTblColIdxMap["Process_priv"]] = "Y"
		case PrivilegeType_File:
			row[userTblColIdxMap["File_priv"]] = "Y"
		case PrivilegeType_Grant:
			row[userTblColIdxMap["Grant_priv"]] = "Y"
		case PrivilegeType_References:
			row[userTblColIdxMap["References_priv"]] = "Y"
		case PrivilegeType_Index:
			row[userTblColIdxMap["Index_priv"]] = "Y"
		case PrivilegeType_Alter:
			row[userTblColIdxMap["Alter_priv"]] = "Y"
		case PrivilegeType_ShowDB:
			row[userTblColIdxMap["Show_db_priv"]] = "Y"
		case PrivilegeType_Super:
			row[userTblColIdxMap["Super_priv"]] = "Y"
		case PrivilegeType_CreateTempTable:
			row[userTblColIdxMap["Create_tmp_table_priv"]] = "Y"
		case PrivilegeType_LockTables:
			row[userTblColIdxMap["Lock_tables_priv"]] = "Y"
		case PrivilegeType_Execute:
			row[userTblColIdxMap["Execute_priv"]] = "Y"
		case PrivilegeType_ReplicationSlave:
			row[userTblColIdxMap["Repl_slave_priv"]] = "Y"
		case PrivilegeType_ReplicationClient:
			row[userTblColIdxMap["Repl_client_priv"]] = "Y"
		case PrivilegeType_CreateView:
			row[userTblColIdxMap["Create_view_priv"]] = "Y"
		case PrivilegeType_ShowView:
			row[userTblColIdxMap["Show_view_priv"]] = "Y"
		case PrivilegeType_CreateRoutine:
			row[userTblColIdxMap["Create_routine_priv"]] = "Y"
		case PrivilegeType_AlterRoutine:
			row[userTblColIdxMap["Alter_routine_priv"]] = "Y"
		case PrivilegeType_CreateUser:
			row[userTblColIdxMap["Create_user_priv"]] = "Y"
		case PrivilegeType_Event:
			row[userTblColIdxMap["Event_priv"]] = "Y"
		case PrivilegeType_Trigger:
			row[userTblColIdxMap["Trigger_priv"]] = "Y"
		case PrivilegeType_CreateTablespace:
			row[userTblColIdxMap["Create_tablespace_priv"]] = "Y"
		case PrivilegeType_CreateRole:
			row[userTblColIdxMap["Create_role_priv"]] = "Y"
		case PrivilegeType_DropRole:
			row[userTblColIdxMap["Drop_role_priv"]] = "Y"
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
