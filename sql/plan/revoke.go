// Copyright 2021 Dolthub, Inc.
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

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/grant_tables"

	"github.com/dolthub/go-mysql-server/sql"
)

// Revoke represents the statement REVOKE [privilege...] ON [item] FROM [user...].
type Revoke struct {
	Privileges     []Privilege
	ObjectType     ObjectType
	PrivilegeLevel PrivilegeLevel
	Users          []UserName
	GrantTables    sql.Database
}

var _ sql.Node = (*Revoke)(nil)
var _ sql.Databaser = (*Revoke)(nil)

// NewRevoke returns a new Revoke node.
func NewRevoke(privileges []Privilege, objType ObjectType, level PrivilegeLevel, users []UserName) *Revoke {
	return &Revoke{
		Privileges:     privileges,
		ObjectType:     objType,
		PrivilegeLevel: level,
		Users:          users,
		GrantTables:    sql.UnresolvedDatabase("mysql"),
	}
}

// Schema implements the interface sql.Node.
func (n *Revoke) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *Revoke) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.String("")
	}
	return fmt.Sprintf("Revoke(On: %s, From: %s)", n.PrivilegeLevel.String(), strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *Revoke) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *Revoke) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *Revoke) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *Revoke) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *Revoke) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *Revoke) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("mysql", "", "", sql.PrivilegeType_Update)) {
		return true
	}
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		if n.Privileges[0].Type == PrivilegeType_All {
			return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("", "", "",
				sql.PrivilegeType_Select,
				sql.PrivilegeType_Insert,
				sql.PrivilegeType_Update,
				sql.PrivilegeType_Delete,
				sql.PrivilegeType_Create,
				sql.PrivilegeType_Drop,
				sql.PrivilegeType_Reload,
				sql.PrivilegeType_Shutdown,
				sql.PrivilegeType_Process,
				sql.PrivilegeType_File,
				sql.PrivilegeType_References,
				sql.PrivilegeType_Index,
				sql.PrivilegeType_Alter,
				sql.PrivilegeType_ShowDB,
				sql.PrivilegeType_Super,
				sql.PrivilegeType_CreateTempTable,
				sql.PrivilegeType_LockTables,
				sql.PrivilegeType_Execute,
				sql.PrivilegeType_ReplicationSlave,
				sql.PrivilegeType_ReplicationClient,
				sql.PrivilegeType_CreateView,
				sql.PrivilegeType_ShowView,
				sql.PrivilegeType_CreateRoutine,
				sql.PrivilegeType_AlterRoutine,
				sql.PrivilegeType_CreateUser,
				sql.PrivilegeType_Event,
				sql.PrivilegeType_Trigger,
				sql.PrivilegeType_CreateTablespace,
				sql.PrivilegeType_CreateRole,
				sql.PrivilegeType_DropRole,
				sql.PrivilegeType_Grant,
			))
		}
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("", "", "",
			convertToSqlPrivilegeType(true, n.Privileges...)...))
	} else if n.PrivilegeLevel.Database != "*" && n.PrivilegeLevel.TableRoutine == "*" {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
		}
		if n.Privileges[0].Type == PrivilegeType_All {
			return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(database, "", "",
				sql.PrivilegeType_Alter,
				sql.PrivilegeType_AlterRoutine,
				sql.PrivilegeType_Create,
				sql.PrivilegeType_CreateRoutine,
				sql.PrivilegeType_CreateTempTable,
				sql.PrivilegeType_CreateView,
				sql.PrivilegeType_Delete,
				sql.PrivilegeType_Drop,
				sql.PrivilegeType_Event,
				sql.PrivilegeType_Execute,
				sql.PrivilegeType_Index,
				sql.PrivilegeType_Insert,
				sql.PrivilegeType_LockTables,
				sql.PrivilegeType_References,
				sql.PrivilegeType_Select,
				sql.PrivilegeType_ShowView,
				sql.PrivilegeType_Trigger,
				sql.PrivilegeType_Update,
				sql.PrivilegeType_Grant,
			))
		}
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(database, "", "",
			convertToSqlPrivilegeType(true, n.Privileges...)...))
	} else {
		//TODO: add column checks
		if n.Privileges[0].Type == PrivilegeType_All {
			return opChecker.UserHasPrivileges(ctx,
				sql.NewPrivilegedOperation(n.PrivilegeLevel.Database, n.PrivilegeLevel.TableRoutine, "",
					sql.PrivilegeType_Alter,
					sql.PrivilegeType_Create,
					sql.PrivilegeType_CreateView,
					sql.PrivilegeType_Delete,
					sql.PrivilegeType_Drop,
					sql.PrivilegeType_Index,
					sql.PrivilegeType_Insert,
					sql.PrivilegeType_References,
					sql.PrivilegeType_Select,
					sql.PrivilegeType_ShowView,
					sql.PrivilegeType_Trigger,
					sql.PrivilegeType_Update,
					sql.PrivilegeType_Grant,
				))
		}
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(n.PrivilegeLevel.Database, n.PrivilegeLevel.TableRoutine, "",
				convertToSqlPrivilegeType(true, n.Privileges...)...))
	}
}

// RowIter implements the interface sql.Node.
func (n *Revoke) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		if n.ObjectType != ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		for _, revokeUser := range n.Users {
			user := grantTables.GetUser(revokeUser.Name, revokeUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleGlobalPrivileges(user); err != nil {
				return nil, err
			}
		}
	} else if n.PrivilegeLevel.Database != "*" && n.PrivilegeLevel.TableRoutine == "*" {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		for _, revokeUser := range n.Users {
			user := grantTables.GetUser(revokeUser.Name, revokeUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleDatabasePrivileges(user, database); err != nil {
				return nil, err
			}
		}
	} else {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != ObjectType_Any {
			//TODO: implement object types
			return nil, fmt.Errorf("GRANT has not yet implemented object types")
		}
		for _, grantUser := range n.Users {
			user := grantTables.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleTablePrivileges(user, database, n.PrivilegeLevel.TableRoutine); err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// handleGlobalPrivileges handles removing global privileges from a user.
func (n *Revoke) handleGlobalPrivileges(user *grant_tables.User) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				user.PrivilegeSet.ClearGlobal()
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Create)
		case PrivilegeType_CreateRole:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateRole)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTablespace:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateTablespace)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateUser:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateUser)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Drop)
		case PrivilegeType_DropRole:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_DropRole)
		case PrivilegeType_Event:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Execute)
		case PrivilegeType_File:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_File)
		case PrivilegeType_Index:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_LockTables)
		case PrivilegeType_Process:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Process)
		case PrivilegeType_References:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_References)
		case PrivilegeType_Reload:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Reload)
		case PrivilegeType_ReplicationClient:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_ReplicationClient)
		case PrivilegeType_ReplicationSlave:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_ReplicationSlave)
		case PrivilegeType_Select:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Select)
		case PrivilegeType_ShowDatabases:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_ShowDB)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_ShowView)
		case PrivilegeType_Shutdown:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Shutdown)
		case PrivilegeType_Super:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Super)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.RemoveGlobalStatic(sql.PrivilegeType_Update)
		case PrivilegeType_Usage:
			// Usage is equal to no privilege
		case PrivilegeType_Dynamic:
			return fmt.Errorf("GRANT does not yet support dynamic privileges")
		default:
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
	}
	return nil
}

// handleDatabasePrivileges  handles removing database privileges from a user.
func (n *Revoke) handleDatabasePrivileges(user *grant_tables.User, dbName string) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				user.PrivilegeSet.ClearDatabase(dbName)
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Create)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Drop)
		case PrivilegeType_Event:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Execute)
		case PrivilegeType_Index:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_LockTables)
		case PrivilegeType_References:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.RemoveDatabase(dbName, sql.PrivilegeType_Update)
		case PrivilegeType_Usage:
			// Usage is equal to no privilege
		default:
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
	}
	return nil
}

// handleTablePrivileges  handles removing table privileges from a user.
func (n *Revoke) handleTablePrivileges(user *grant_tables.User, dbName string, tblName string) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return fmt.Errorf("GRANT has not yet implemented column privileges")
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				user.PrivilegeSet.ClearTable(dbName, tblName)
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Alter)
		case PrivilegeType_Create:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Create)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Drop)
		case PrivilegeType_Index:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Insert)
		case PrivilegeType_References:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.RemoveTable(dbName, tblName, sql.PrivilegeType_Update)
		case PrivilegeType_Usage:
			// Usage is equal to no privilege
		default:
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
	}
	return nil
}

// RevokeAll represents the statement REVOKE ALL PRIVILEGES.
type RevokeAll struct {
	Users []UserName
}

var _ sql.Node = (*RevokeAll)(nil)

// NewRevokeAll returns a new RevokeAll node.
func NewRevokeAll(users []UserName) *RevokeAll {
	return &RevokeAll{
		Users: users,
	}
}

// Schema implements the interface sql.Node.
func (n *RevokeAll) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *RevokeAll) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.String("")
	}
	return fmt.Sprintf("RevokeAll(From: %s)", strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *RevokeAll) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *RevokeAll) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *RevokeAll) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *RevokeAll) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_CreateUser)) ||
		opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Super)) ||
		opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation("mysql", "", "", sql.PrivilegeType_Update))
}

// RowIter implements the interface sql.Node.
func (n *RevokeAll) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// RevokeRole represents the statement REVOKE [role...] FROM [user...].
type RevokeRole struct {
	Roles       []UserName
	TargetUsers []UserName
	GrantTables sql.Database
}

var _ sql.Node = (*RevokeRole)(nil)
var _ sql.Databaser = (*RevokeRole)(nil)

// NewRevokeRole returns a new RevokeRole node.
func NewRevokeRole(roles []UserName, users []UserName) *RevokeRole {
	return &RevokeRole{
		Roles:       roles,
		TargetUsers: users,
		GrantTables: sql.UnresolvedDatabase("mysql"),
	}
}

// Schema implements the interface sql.Node.
func (n *RevokeRole) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *RevokeRole) String() string {
	roles := make([]string, len(n.Roles))
	for i, role := range n.Roles {
		roles[i] = role.String("")
	}
	users := make([]string, len(n.TargetUsers))
	for i, user := range n.TargetUsers {
		users[i] = user.String("")
	}
	return fmt.Sprintf("RevokeRole(Roles: %s, From: %s)", strings.Join(roles, ", "), strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *RevokeRole) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *RevokeRole) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *RevokeRole) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *RevokeRole) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *RevokeRole) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *RevokeRole) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Super)) {
		return true
	}
	//TODO: only active roles may be revoked if the SUPER privilege is not held
	grantTables := n.GrantTables.(*grant_tables.GrantTables)
	client := ctx.Session.Client()
	user := grantTables.GetUser(client.User, client.Address, false)
	if user == nil {
		return false
	}
	roleEntries := grantTables.RoleEdgesTable().Data().Get(grant_tables.RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	for _, roleName := range n.Roles {
		role := grantTables.GetUser(roleName.Name, roleName.Host, true)
		if role == nil {
			return false
		}
		foundMatch := false
		for _, roleEntry := range roleEntries {
			roleEdge := roleEntry.(*grant_tables.RoleEdge)
			if roleEdge.FromUser == role.User && roleEdge.FromHost == role.Host {
				if roleEdge.WithAdminOption {
					foundMatch = true
				} else {
					return false
				}
			}
		}
		if !foundMatch {
			return false
		}
	}
	return true
}

// RowIter implements the interface sql.Node.
func (n *RevokeRole) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	roleEdgesData := grantTables.RoleEdgesTable().Data()
	for _, targetUser := range n.TargetUsers {
		user := grantTables.GetUser(targetUser.Name, targetUser.Host, false)
		if user == nil {
			return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetUser.String("`"))
		}
		for _, targetRole := range n.Roles {
			role := grantTables.GetUser(targetRole.Name, targetRole.Host, true)
			if role == nil {
				return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetRole.String("`"))
			}
			//TODO: if a role is mentioned in the "mandatory_roles" system variable then they cannot be revoked
			err := roleEdgesData.Remove(ctx, grant_tables.RoleEdgesPrimaryKey{
				FromHost: role.Host,
				FromUser: role.User,
				ToHost:   user.Host,
				ToUser:   user.User,
			}, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// RevokeProxy represents the statement REVOKE PROXY.
type RevokeProxy struct {
	On   UserName
	From []UserName
}

var _ sql.Node = (*RevokeProxy)(nil)

// NewRevokeProxy returns a new RevokeProxy node.
func NewRevokeProxy(on UserName, from []UserName) *RevokeProxy {
	return &RevokeProxy{
		On:   on,
		From: from,
	}
}

// Schema implements the interface sql.Node.
func (n *RevokeProxy) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *RevokeProxy) String() string {
	users := make([]string, len(n.From))
	for i, user := range n.From {
		users[i] = user.String("")
	}
	return fmt.Sprintf("RevokeProxy(On: %s, From: %s)", n.On.String(""), strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *RevokeProxy) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *RevokeProxy) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *RevokeProxy) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *RevokeProxy) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: add this when proxy support is added
	return true
}

// RowIter implements the interface sql.Node.
func (n *RevokeProxy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
