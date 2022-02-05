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
	"time"

	"github.com/dolthub/go-mysql-server/sql/grant_tables"

	"github.com/dolthub/go-mysql-server/sql"
)

// CreateRole represents the statement CREATE ROLE.
type CreateRole struct {
	IfNotExists bool
	Roles       []UserName
	GrantTables sql.Database
}

// NewCreateRole returns a new CreateRole node.
func NewCreateRole(ifNotExists bool, roles []UserName) *CreateRole {
	return &CreateRole{
		IfNotExists: ifNotExists,
		Roles:       roles,
		GrantTables: sql.UnresolvedDatabase("mysql"),
	}
}

var _ sql.Node = (*CreateRole)(nil)

// Schema implements the interface sql.Node.
func (n *CreateRole) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *CreateRole) String() string {
	roles := make([]string, len(n.Roles))
	for i, role := range n.Roles {
		roles[i] = role.String("")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateRole(%s%s)", ifNotExists, strings.Join(roles, ", "))
}

// Database implements the interface sql.Databaser.
func (n *CreateRole) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *CreateRole) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *CreateRole) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *CreateRole) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *CreateRole) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *CreateRole) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	userTableData := grantTables.UserTable().Data()
	for _, role := range n.Roles {
		userPk := grant_tables.UserPrimaryKey{
			Host: role.Host,
			User: role.Name,
		}
		if role.AnyHost {
			userPk.Host = "%"
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) > 0 {
			if n.IfNotExists {
				continue
			}
			return nil, sql.ErrRoleCreationFailure.New(role.String("'"))
		}

		//TODO: When password expiration is implemented, make sure that roles have an expired password on creation
		err := userTableData.Put(ctx, &grant_tables.User{
			User:                userPk.User,
			Host:                userPk.Host,
			PrivilegeSet:        grant_tables.NewPrivilegeSet(),
			Plugin:              "mysql_native_password",
			Password:            "",
			PasswordLastChanged: time.Now().UTC(),
			Locked:              true,
			Attributes:          nil,
			IsRole:              true,
		})
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
