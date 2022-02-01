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
	privileges := make([]string, len(n.Privileges))
	for i, privilege := range n.Privileges {
		privileges[i] = privilege.String()
	}
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.StringWithQuote("", "")
	}
	return fmt.Sprintf("Revoke(Privileges: %s, On: %s, From: %s)",
		strings.Join(privileges, ", "), n.PrivilegeLevel.String(), strings.Join(users, ", "))
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

// RowIter implements the interface sql.Node.
func (n *Revoke) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	//TODO: allow for db and table-level privileges
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		//TODO: return actual errors here that are tested against
		if n.ObjectType != ObjectType_Any {
			return nil, fmt.Errorf("global privileges do not have an applicable object type")
		}
		for _, revokeUser := range n.Users {
			user := grantTables.GetUser(revokeUser.Name, revokeUser.Host, false)
			if user == nil {
				return nil, sql.ErrRevokeUserDoesNotExist.New(revokeUser.Name, revokeUser.Host)
			}
			for _, priv := range n.Privileges {
				if len(priv.Columns) > 0 {
					//TODO: return actual error here that is tested against
					return nil, fmt.Errorf("global privileges may not have columns")
				}
				//TODO: enforce that, if ALL is present, that no others may be present
				switch priv.Type {
				case PrivilegeType_All:
					user.PrivilegeSet.Clear()
				case PrivilegeType_Insert:
					user.PrivilegeSet.Remove(grant_tables.PrivilegeType_Insert)
				case PrivilegeType_References:
					user.PrivilegeSet.Remove(grant_tables.PrivilegeType_References)
				case PrivilegeType_Select:
					user.PrivilegeSet.Remove(grant_tables.PrivilegeType_Select)
				case PrivilegeType_Update:
					user.PrivilegeSet.Remove(grant_tables.PrivilegeType_Update)
				default:
					//TODO: implement the rest of the privileges
					return nil, fmt.Errorf("REVOKE has not yet implemented all global privileges")
				}
			}
		}
	} else {
		return nil, fmt.Errorf("REVOKE has not yet implemented non-global privileges")
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
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
		users[i] = user.StringWithQuote("", "")
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
		roles[i] = role.StringWithQuote("", "")
	}
	users := make([]string, len(n.TargetUsers))
	for i, user := range n.TargetUsers {
		users[i] = user.StringWithQuote("", "")
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
			return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetUser.StringWithQuote("`", ""))
		}
		for _, targetRole := range n.Roles {
			role := grantTables.GetUser(targetRole.Name, targetRole.Host, true)
			if role == nil {
				return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetRole.StringWithQuote("`", ""))
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
		users[i] = user.StringWithQuote("", "")
	}
	return fmt.Sprintf("RevokeProxy(On: %s, From: %s)", n.On.StringWithQuote("", ""), strings.Join(users, ", "))
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

// RowIter implements the interface sql.Node.
func (n *RevokeProxy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
