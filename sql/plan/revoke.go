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

// Revoke represents the statement REVOKE [privilege...] ON [item] FROM [user...].
type Revoke struct {
	Privileges     []Privilege
	ObjectType     ObjectType
	PrivilegeLevel PrivilegeLevel
	Users          []UserName
}

var _ sql.Node = (*Revoke)(nil)

// NewRevoke returns a new Revoke node.
func NewRevoke(privileges []Privilege, objType ObjectType, level PrivilegeLevel, users []UserName) *Revoke {
	return &Revoke{
		Privileges:     privileges,
		ObjectType:     objType,
		PrivilegeLevel: level,
		Users:          users,
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

// Resolved implements the interface sql.Node.
func (n *Revoke) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
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
}

var _ sql.Node = (*RevokeRole)(nil)

// NewRevokeRole returns a new RevokeRole node.
func NewRevokeRole(roles []UserName, users []UserName) *RevokeRole {
	return &RevokeRole{
		Roles:       roles,
		TargetUsers: users,
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

// Resolved implements the interface sql.Node.
func (n *RevokeRole) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
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
