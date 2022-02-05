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
				user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Grant)
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
				user.PrivilegeSet.AddDatabase(database, grant_tables.PrivilegeType_Grant)
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
				user.PrivilegeSet.AddTable(database, n.PrivilegeLevel.TableRoutine, grant_tables.PrivilegeType_Grant)
			}
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// grantAllGlobalPrivileges adds all global static privileges to the given user, except for the grant privilege (which
// has special rules for its assignment).
func (n *Grant) grantAllGlobalPrivileges(user *grant_tables.User) {
	user.PrivilegeSet.AddGlobalStatic(
		grant_tables.PrivilegeType_Select,
		grant_tables.PrivilegeType_Insert,
		grant_tables.PrivilegeType_Update,
		grant_tables.PrivilegeType_Delete,
		grant_tables.PrivilegeType_Create,
		grant_tables.PrivilegeType_Drop,
		grant_tables.PrivilegeType_Reload,
		grant_tables.PrivilegeType_Shutdown,
		grant_tables.PrivilegeType_Process,
		grant_tables.PrivilegeType_File,
		grant_tables.PrivilegeType_References,
		grant_tables.PrivilegeType_Index,
		grant_tables.PrivilegeType_Alter,
		grant_tables.PrivilegeType_ShowDB,
		grant_tables.PrivilegeType_Super,
		grant_tables.PrivilegeType_CreateTempTable,
		grant_tables.PrivilegeType_LockTables,
		grant_tables.PrivilegeType_Execute,
		grant_tables.PrivilegeType_ReplicationSlave,
		grant_tables.PrivilegeType_ReplicationClient,
		grant_tables.PrivilegeType_CreateView,
		grant_tables.PrivilegeType_ShowView,
		grant_tables.PrivilegeType_CreateRoutine,
		grant_tables.PrivilegeType_AlterRoutine,
		grant_tables.PrivilegeType_CreateUser,
		grant_tables.PrivilegeType_Event,
		grant_tables.PrivilegeType_Trigger,
		grant_tables.PrivilegeType_CreateTablespace,
		grant_tables.PrivilegeType_CreateRole,
		grant_tables.PrivilegeType_DropRole,
	)
}

// grantAllDatabasePrivileges adds all database privileges to the given user, except for the grant privilege (which has
// special rules for its assignment).
func (n *Grant) grantAllDatabasePrivileges(user *grant_tables.User, dbName string) {
	user.PrivilegeSet.AddDatabase(
		dbName,
		grant_tables.PrivilegeType_Alter,
		grant_tables.PrivilegeType_AlterRoutine,
		grant_tables.PrivilegeType_Create,
		grant_tables.PrivilegeType_CreateRoutine,
		grant_tables.PrivilegeType_CreateTempTable,
		grant_tables.PrivilegeType_CreateView,
		grant_tables.PrivilegeType_Delete,
		grant_tables.PrivilegeType_Drop,
		grant_tables.PrivilegeType_Event,
		grant_tables.PrivilegeType_Execute,
		grant_tables.PrivilegeType_Index,
		grant_tables.PrivilegeType_Insert,
		grant_tables.PrivilegeType_LockTables,
		grant_tables.PrivilegeType_References,
		grant_tables.PrivilegeType_Select,
		grant_tables.PrivilegeType_ShowView,
		grant_tables.PrivilegeType_Trigger,
		grant_tables.PrivilegeType_Update,
	)
}

// grantAllTablePrivileges adds all table privileges to the given user, except for the grant privilege (which has
// special rules for its assignment).
func (n *Grant) grantAllTablePrivileges(user *grant_tables.User, dbName string, tblName string) {
	user.PrivilegeSet.AddTable(
		dbName,
		tblName,
		grant_tables.PrivilegeType_Alter,
		grant_tables.PrivilegeType_Create,
		grant_tables.PrivilegeType_CreateView,
		grant_tables.PrivilegeType_Delete,
		grant_tables.PrivilegeType_Drop,
		grant_tables.PrivilegeType_Index,
		grant_tables.PrivilegeType_Insert,
		grant_tables.PrivilegeType_References,
		grant_tables.PrivilegeType_Select,
		grant_tables.PrivilegeType_ShowView,
		grant_tables.PrivilegeType_Trigger,
		grant_tables.PrivilegeType_Update,
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
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Create)
		case PrivilegeType_CreateRole:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateRole)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTablespace:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateTablespace)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateUser:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateUser)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Drop)
		case PrivilegeType_DropRole:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_DropRole)
		case PrivilegeType_Event:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Execute)
		case PrivilegeType_File:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_File)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_LockTables)
		case PrivilegeType_Process:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Process)
		case PrivilegeType_References:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_References)
		case PrivilegeType_Reload:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Reload)
		case PrivilegeType_ReplicationClient:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_ReplicationClient)
		case PrivilegeType_ReplicationSlave:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_ReplicationSlave)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Select)
		case PrivilegeType_ShowDatabases:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_ShowDB)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_ShowView)
		case PrivilegeType_Shutdown:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Shutdown)
		case PrivilegeType_Super:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Super)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddGlobalStatic(grant_tables.PrivilegeType_Update)
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
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Alter)
		case PrivilegeType_AlterRoutine:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_AlterRoutine)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Create)
		case PrivilegeType_CreateRoutine:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_CreateRoutine)
		case PrivilegeType_CreateTemporaryTables:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_CreateTempTable)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Drop)
		case PrivilegeType_Event:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Event)
		case PrivilegeType_Execute:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Execute)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Insert)
		case PrivilegeType_LockTables:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_LockTables)
		case PrivilegeType_References:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddDatabase(dbName, grant_tables.PrivilegeType_Update)
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
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Alter)
		case PrivilegeType_Create:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Create)
		case PrivilegeType_CreateView:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_CreateView)
		case PrivilegeType_Delete:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Delete)
		case PrivilegeType_Drop:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Drop)
		case PrivilegeType_Index:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Index)
		case PrivilegeType_Insert:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Insert)
		case PrivilegeType_References:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_References)
		case PrivilegeType_Select:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Select)
		case PrivilegeType_ShowView:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_ShowView)
		case PrivilegeType_Trigger:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Trigger)
		case PrivilegeType_Update:
			user.PrivilegeSet.AddTable(dbName, tblName, grant_tables.PrivilegeType_Update)
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

// RowIter implements the interface sql.Node.
func (n *GrantProxy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
