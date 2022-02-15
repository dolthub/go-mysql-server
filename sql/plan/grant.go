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

// Grant represents the statement GRANT [privilege...] ON [item] TO [user...].
type Grant struct {
	Privileges      []Privilege
	ObjectType      ObjectType
	PrivilegeLevel  PrivilegeLevel
	Users           []UserName
	WithGrantOption bool
	As              *GrantUserAssumption
	GrantTables     sql.Database
}

var _ sql.Node = (*Grant)(nil)
var _ sql.Databaser = (*Grant)(nil)

// NewGrant returns a new Grant node.
func NewGrant(db sql.Database, privileges []Privilege, objType ObjectType, level PrivilegeLevel, users []UserName, withGrant bool, as *GrantUserAssumption) *Grant {
	return &Grant{
		Privileges:      privileges,
		ObjectType:      objType,
		PrivilegeLevel:  level,
		Users:           users,
		WithGrantOption: withGrant,
		As:              as,
		GrantTables:     db,
	}
}

// Schema implements the interface sql.Node.
func (n *Grant) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *Grant) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.String("")
	}
	return fmt.Sprintf("Grant(On: %s, To: %s)", n.PrivilegeLevel.String(), strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *Grant) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *Grant) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *Grant) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *Grant) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *Grant) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *Grant) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
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
func (n *Grant) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		if n.ObjectType != ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := grantTables.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleGlobalPrivileges(user); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Grant)
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
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := grantTables.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleDatabasePrivileges(user, database); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddDatabase(database, sql.PrivilegeType_Grant)
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
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := grantTables.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.handleTablePrivileges(user, database, n.PrivilegeLevel.TableRoutine); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddTable(database, n.PrivilegeLevel.TableRoutine, sql.PrivilegeType_Grant)
			}
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// grantAllGlobalPrivileges adds all global static privileges to the given user, except for the grant privilege (which
// has special rules for its assignment).
func (n *Grant) grantAllGlobalPrivileges(user *grant_tables.User) {
	user.PrivilegeSet.AddGlobalStatic(
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
	)
}

// grantAllDatabasePrivileges adds all database privileges to the given user, except for the grant privilege (which has
// special rules for its assignment).
func (n *Grant) grantAllDatabasePrivileges(user *grant_tables.User, dbName string) {
	user.PrivilegeSet.AddDatabase(
		dbName,
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
	)
}

// grantAllTablePrivileges adds all table privileges to the given user, except for the grant privilege (which has
// special rules for its assignment).
func (n *Grant) grantAllTablePrivileges(user *grant_tables.User, dbName string, tblName string) {
	user.PrivilegeSet.AddTable(
		dbName,
		tblName,
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
	)
}

// handleGlobalPrivileges handles giving a user their global privileges.
func (n *Grant) handleGlobalPrivileges(user *grant_tables.User) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				n.grantAllGlobalPrivileges(user)
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Create)
		case PrivilegeType_CreateRole:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateRole)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTablespace:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateTablespace)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateUser:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateUser)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Drop)
		case PrivilegeType_DropRole:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_DropRole)
		case PrivilegeType_Event:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Execute)
		case PrivilegeType_File:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_File)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_LockTables)
		case PrivilegeType_Process:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Process)
		case PrivilegeType_References:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_References)
		case PrivilegeType_Reload:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Reload)
		case PrivilegeType_ReplicationClient:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_ReplicationClient)
		case PrivilegeType_ReplicationSlave:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_ReplicationSlave)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Select)
		case PrivilegeType_ShowDatabases:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_ShowDB)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_ShowView)
		case PrivilegeType_Shutdown:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Shutdown)
		case PrivilegeType_Super:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Super)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_Update)
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

// handleDatabasePrivileges handles giving a user their database privileges.
func (n *Grant) handleDatabasePrivileges(user *grant_tables.User, dbName string) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				n.grantAllDatabasePrivileges(user, dbName)
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Create)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Drop)
		case PrivilegeType_Event:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Execute)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_LockTables)
		case PrivilegeType_References:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddDatabase(dbName, sql.PrivilegeType_Update)
		case PrivilegeType_Usage:
			// Usage is equal to no privilege
		default:
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
	}
	return nil
}

// handleTablePrivileges handles giving a user their table privileges.
func (n *Grant) handleTablePrivileges(user *grant_tables.User, dbName string, tblName string) error {
	for i, priv := range n.Privileges {
		if len(priv.Columns) > 0 {
			return fmt.Errorf("GRANT has not yet implemented column privileges")
		}
		switch priv.Type {
		case PrivilegeType_All:
			// If ALL is present, then no other privileges may be provided.
			// This should be enforced by the parser, so this is a backup check just in case
			if i == 0 && len(n.Privileges) == 1 {
				n.grantAllTablePrivileges(user, dbName, tblName)
			} else {
				return sql.ErrGrantRevokeIllegalPrivilege.New()
			}
		case PrivilegeType_Alter:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Alter)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Create)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Drop)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Insert)
		case PrivilegeType_References:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddTable(dbName, tblName, sql.PrivilegeType_Update)
		case PrivilegeType_Usage:
			// Usage is equal to no privilege
		default:
			return sql.ErrGrantRevokeIllegalPrivilege.New()
		}
	}
	return nil
}

// GrantRole represents the statement GRANT [role...] TO [user...].
type GrantRole struct {
	Roles           []UserName
	TargetUsers     []UserName
	WithAdminOption bool
	GrantTables     sql.Database
}

var _ sql.Node = (*GrantRole)(nil)
var _ sql.Databaser = (*GrantRole)(nil)

// NewGrantRole returns a new GrantRole node.
func NewGrantRole(roles []UserName, users []UserName, withAdmin bool) *GrantRole {
	return &GrantRole{
		Roles:           roles,
		TargetUsers:     users,
		WithAdminOption: withAdmin,
		GrantTables:     sql.UnresolvedDatabase("mysql"),
	}
}

// Schema implements the interface sql.Node.
func (n *GrantRole) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *GrantRole) String() string {
	roles := make([]string, len(n.Roles))
	for i, role := range n.Roles {
		roles[i] = role.String("")
	}
	users := make([]string, len(n.TargetUsers))
	for i, user := range n.TargetUsers {
		users[i] = user.String("")
	}
	return fmt.Sprintf("GrantRole(Roles: %s, To: %s)", strings.Join(roles, ", "), strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *GrantRole) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *GrantRole) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *GrantRole) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *GrantRole) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *GrantRole) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *GrantRole) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Super)) {
		return true
	}
	//TODO: only active roles may be assigned if the SUPER privilege is not held
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
func (n *GrantRole) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
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
			err := roleEdgesData.Put(ctx, &grant_tables.RoleEdge{
				FromHost:        role.Host,
				FromUser:        role.User,
				ToHost:          user.Host,
				ToUser:          user.User,
				WithAdminOption: n.WithAdminOption,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// GrantProxy represents the statement GRANT PROXY.
type GrantProxy struct {
	On              UserName
	To              []UserName
	WithGrantOption bool
}

var _ sql.Node = (*GrantProxy)(nil)

// NewGrantProxy returns a new GrantProxy node.
func NewGrantProxy(on UserName, to []UserName, withGrant bool) *GrantProxy {
	return &GrantProxy{
		On:              on,
		To:              to,
		WithGrantOption: withGrant,
	}
}

// Schema implements the interface sql.Node.
func (n *GrantProxy) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *GrantProxy) String() string {
	users := make([]string, len(n.To))
	for i, user := range n.To {
		users[i] = user.String("")
	}
	return fmt.Sprintf("GrantProxy(On: %s, To: %s)", n.On.String(""), strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *GrantProxy) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *GrantProxy) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *GrantProxy) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *GrantProxy) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: add this when proxy support is added
	return true
}

// RowIter implements the interface sql.Node.
func (n *GrantProxy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
