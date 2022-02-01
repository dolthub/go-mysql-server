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
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

// DropRole represents the statement DROP ROLE.
type DropRole struct {
	IfExists    bool
	Roles       []UserName
	GrantTables sql.Database
}

// NewDropRole returns a new DropRole node.
func NewDropRole(ifExists bool, roles []UserName) *DropRole {
	return &DropRole{
		IfExists:    ifExists,
		Roles:       roles,
		GrantTables: sql.UnresolvedDatabase("mysql"),
	}
}

var _ sql.Node = (*DropRole)(nil)
var _ sql.Databaser = (*DropRole)(nil)

// Schema implements the interface sql.Node.
func (n *DropRole) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *DropRole) String() string {
	roles := make([]string, len(n.Roles))
	for i, role := range n.Roles {
		roles[i] = role.StringWithQuote("", "")
	}
	ifExists := ""
	if n.IfExists {
		ifExists = "IfExists: "
	}
	return fmt.Sprintf("DropRole(%s%s)", ifExists, strings.Join(roles, ", "))
}

// Database implements the interface sql.Databaser.
func (n *DropRole) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *DropRole) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *DropRole) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *DropRole) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *DropRole) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *DropRole) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	userTableData := grantTables.UserTable().Data()
	roleEdgesData := grantTables.RoleEdgesTable().Data()
	for _, role := range n.Roles {
		userPk := grant_tables.UserPrimaryKey{
			Host: role.Host,
			User: role.Name,
		}
		if role.AnyHost {
			userPk.Host = "%"
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) == 0 {
			if n.IfExists {
				continue
			}
			return nil, sql.ErrRoleDeletionFailure.New(role.StringWithQuote("'", ""))
		}
		existingUser := existingRows[0].(*grant_tables.User)

		//TODO: if a role is mentioned in the "mandatory_roles" system variable then they cannot be dropped
		err := userTableData.Remove(ctx, userPk, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, grant_tables.RoleEdgesFromKey{
			FromHost: existingUser.Host,
			FromUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, grant_tables.RoleEdgesToKey{
			ToHost: existingUser.Host,
			ToUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
	}
	err := grantTables.Persist(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}
