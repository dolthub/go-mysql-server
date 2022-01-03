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
}

// NewGrant returns a new Grant node.
func NewGrant(privileges []Privilege, objType ObjectType, level PrivilegeLevel, users []UserName, withGrant bool, as *GrantUserAssumption) *Grant {
	return &Grant{
		Privileges:      privileges,
		ObjectType:      objType,
		PrivilegeLevel:  level,
		Users:           users,
		WithGrantOption: withGrant,
		As:              as,
	}
}

var _ sql.Node = (*Grant)(nil)

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

// Resolved implements the interface sql.Node.
func (n *Grant) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
}

// GrantRole represents the statement GRANT [role...] TO [user...].
type GrantRole struct {
	Roles           []UserName
	TargetUsers     []UserName
	WithAdminOption bool
}

// NewGrantRole returns a new GrantRole node.
func NewGrantRole(roles []UserName, users []UserName, withAdmin bool) *GrantRole {
	return &GrantRole{
		Roles:           roles,
		TargetUsers:     users,
		WithAdminOption: withAdmin,
	}
}

var _ sql.Node = (*GrantRole)(nil)

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

// NewGrantProxy returns a new GrantProxy node.
func NewGrantProxy(on UserName, to []UserName, withGrant bool) *GrantProxy {
	return &GrantProxy{
		On:              on,
		To:              to,
		WithGrantOption: withGrant,
	}
}

var _ sql.Node = (*GrantProxy)(nil)

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
