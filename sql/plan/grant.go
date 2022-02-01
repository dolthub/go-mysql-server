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
	privileges := make([]string, len(n.Privileges))
	for i, privilege := range n.Privileges {
		privileges[i] = privilege.String()
	}
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.StringWithQuote("", "")
	}
	return fmt.Sprintf("Grant(Privileges: %s, On: %s, To: %s)",
		strings.Join(privileges, ", "), n.PrivilegeLevel.String(), strings.Join(users, ", "))
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
	//TODO: allow for db and table-level privileges
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		//TODO: return actual errors here that are tested against
		if n.ObjectType != ObjectType_Any {
			return nil, fmt.Errorf("global privileges do not have an applicable object type")
		}
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := grantTables.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			for _, priv := range n.Privileges {
				if len(priv.Columns) > 0 {
					//TODO: return actual error here that is tested against
					return nil, fmt.Errorf("global privileges may not have columns")
				}
				//TODO: enforce that, if ALL is present, that no others may be present
				switch priv.Type {
				case PrivilegeType_All:
					n.grantAllPrivileges(user)
				case PrivilegeType_Insert:
					user.PrivilegeSet.Add(grant_tables.PrivilegeType_Insert)
				case PrivilegeType_References:
					user.PrivilegeSet.Add(grant_tables.PrivilegeType_References)
				case PrivilegeType_Select:
					user.PrivilegeSet.Add(grant_tables.PrivilegeType_Select)
				case PrivilegeType_Update:
					user.PrivilegeSet.Add(grant_tables.PrivilegeType_Update)
				default:
					//TODO: implement the rest of the privileges
					return nil, fmt.Errorf("GRANT has not yet implemented all global privileges")
				}
			}
			if n.WithGrantOption {
				user.PrivilegeSet.Add(grant_tables.PrivilegeType_Grant)
			}
		}
	} else {
		return nil, fmt.Errorf("GRANT has not yet implemented non-global privileges")
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

// grantAllPrivileges adds all static privileges to the given user, except for the grant privilege (which has special
// rules for its assignment).
func (n *Grant) grantAllPrivileges(user *grant_tables.User) {
	user.PrivilegeSet.Add(
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

// GrantRole represents the statement GRANT [role...] TO [user...].
type GrantRole struct {
	Roles           []UserName
	TargetUsers     []UserName
	WithAdminOption bool
}

var _ sql.Node = (*GrantRole)(nil)

// NewGrantRole returns a new GrantRole node.
func NewGrantRole(roles []UserName, users []UserName, withAdmin bool) *GrantRole {
	return &GrantRole{
		Roles:           roles,
		TargetUsers:     users,
		WithAdminOption: withAdmin,
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
		roles[i] = role.StringWithQuote("", "")
	}
	users := make([]string, len(n.TargetUsers))
	for i, user := range n.TargetUsers {
		users[i] = user.StringWithQuote("", "")
	}
	return fmt.Sprintf("GrantRole(Roles: %s, To: %s)", strings.Join(roles, ", "), strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *GrantRole) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
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
		users[i] = user.StringWithQuote("", "")
	}
	return fmt.Sprintf("GrantProxy(On: %s, To: %s)", n.On.StringWithQuote("", ""), strings.Join(users, ", "))
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
