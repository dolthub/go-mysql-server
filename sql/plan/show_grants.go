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

// ShowGrants represents the statement SHOW GRANTS.
type ShowGrants struct {
	CurrentUser bool
	For         *UserName
	Using       []UserName
	GrantTables sql.Database
}

var _ sql.Node = (*ShowGrants)(nil)
var _ sql.Databaser = (*ShowGrants)(nil)

// NewShowGrants returns a new ShowGrants node.
func NewShowGrants(currentUser bool, targetUser *UserName, using []UserName) *ShowGrants {
	return &ShowGrants{
		CurrentUser: currentUser,
		For:         targetUser,
		Using:       using,
		GrantTables: sql.UnresolvedDatabase("mysql"),
	}
}

// Schema implements the interface sql.Node.
func (n *ShowGrants) Schema() sql.Schema {
	user := n.For
	if user == nil {
		user = &UserName{
			Name:    "root",
			Host:    "localhost",
			AnyHost: true,
		}
	}
	return sql.Schema{{
		Name: fmt.Sprintf("Grants for %s", user.String("")),
		Type: sql.LongText,
	}}
}

// String implements the interface sql.Node.
func (n *ShowGrants) String() string {
	user := n.For
	if user == nil {
		user = &UserName{
			Name:    "root",
			Host:    "localhost",
			AnyHost: true,
		}
	}
	return fmt.Sprintf("ShowGrants(%s)", user.String(""))
}

// Database implements the interface sql.Databaser.
func (n *ShowGrants) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *ShowGrants) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *ShowGrants) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *ShowGrants) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *ShowGrants) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *ShowGrants) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if n.CurrentUser {
		return true
	} else {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation("mysql", "", "", sql.PrivilegeType_Select))
	}
}

// RowIter implements the interface sql.Node.
func (n *ShowGrants) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.For == nil || n.CurrentUser {
		client := ctx.Session.Client()
		n.For = &UserName{
			Name: client.User,
			Host: client.Address,
		}
	}
	user := grantTables.GetUser(n.For.Name, n.For.Host, false)
	if user == nil {
		return nil, sql.ErrShowGrantsUserDoesNotExist.New(n.For.Name, n.For.Host)
	}

	//TODO: implement USING, perhaps by creating a new context with the chosen roles set as the active roles
	var rows []sql.Row
	sb := strings.Builder{}
	withGrantOption := ""
	for i, priv := range user.PrivilegeSet.ToSortedSlice() {
		privStr := priv.String()
		if privStr == sql.PrivilegeType_Grant.String() {
			withGrantOption = " WITH GRANT OPTION"
		} else {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(privStr)
		}
	}
	if sb.Len() == 0 {
		sb.WriteString("USAGE")
	}
	rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s ON *.* TO %s%s", sb.String(), user.UserHostToString("`"), withGrantOption)})
	//TODO: display the database privileges
	//TODO: display the table and column privileges

	sb.Reset()
	roleEdges := grantTables.RoleEdgesTable().Data().Get(grant_tables.RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	for i, roleEdge := range roleEdges {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(roleEdge.(*grant_tables.RoleEdge).FromString("`"))
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s TO %s", sb.String(), user.UserHostToString("`"))})
	}
	return sql.RowsToRowIter(rows...), nil
}
