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

	"github.com/gabereiser/go-mysql-server/sql/mysql_db"
	"github.com/gabereiser/go-mysql-server/sql/types"

	"github.com/gabereiser/go-mysql-server/sql"
)

// ShowGrants represents the statement SHOW GRANTS.
type ShowGrants struct {
	CurrentUser bool
	For         *UserName
	Using       []UserName
	MySQLDb     sql.Database
}

var _ sql.Node = (*ShowGrants)(nil)
var _ sql.Databaser = (*ShowGrants)(nil)

// NewShowGrants returns a new ShowGrants node.
func NewShowGrants(currentUser bool, targetUser *UserName, using []UserName) *ShowGrants {
	return &ShowGrants{
		CurrentUser: currentUser,
		For:         targetUser,
		Using:       using,
		MySQLDb:     sql.UnresolvedDatabase("mysql"),
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
		Type: types.LongText,
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
	return n.MySQLDb
}

// WithDatabase implements the interface sql.Databaser.
func (n *ShowGrants) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.MySQLDb = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *ShowGrants) Resolved() bool {
	_, ok := n.MySQLDb.(sql.UnresolvedDatabase)
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

// generatePrivStrings creates a formatted GRANT <privilege_list> on <global/database/table> to <user@host> string
func generatePrivStrings(db, tbl, user string, privs []sql.PrivilegeType) string {
	sb := strings.Builder{}
	withGrantOption := ""
	for i, priv := range privs {
		privStr := priv.String()
		if privStr == sql.PrivilegeType_GrantOption.String() {
			if len(privs) > 1 {
				withGrantOption = " WITH GRANT OPTION"
			}
		} else {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(privStr)
		}
	}
	// handle special case for empty global and database privileges
	privStr := sb.String()
	if len(privStr) == 0 {
		if db == "*" {
			privStr = "USAGE"
		} else {
			return ""
		}
	}
	return fmt.Sprintf("GRANT %s ON %s.%s TO %s%s", privStr, db, tbl, user, withGrantOption)
}

// RowIter implements the interface sql.Node.
func (n *ShowGrants) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
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
	user := mysqlDb.GetUser(n.For.Name, n.For.Host, false)
	if user == nil {
		return nil, sql.ErrShowGrantsUserDoesNotExist.New(n.For.Name, n.For.Host)
	}

	//TODO: implement USING, perhaps by creating a new context with the chosen roles set as the active roles
	var rows []sql.Row
	userStr := user.UserHostToString("`")
	privStr := generatePrivStrings("*", "*", userStr, user.PrivilegeSet.ToSlice())
	rows = append(rows, sql.Row{privStr})

	for _, db := range user.PrivilegeSet.GetDatabases() {
		dbStr := fmt.Sprintf("`%s`", db.Name())
		if privStr = generatePrivStrings(dbStr, "*", userStr, db.ToSlice()); len(privStr) != 0 {
			rows = append(rows, sql.Row{privStr})
		}

		for _, tbl := range db.GetTables() {
			tblStr := fmt.Sprintf("`%s`", tbl.Name())
			privStr = generatePrivStrings(dbStr, tblStr, userStr, tbl.ToSlice())
			rows = append(rows, sql.Row{privStr})
		}
	}

	// TODO: display column privileges

	sb := strings.Builder{}
	roleEdges := mysqlDb.RoleEdgesTable().Data().Get(mysql_db.RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	for i, roleEdge := range roleEdges {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(roleEdge.(*mysql_db.RoleEdge).FromString("`"))
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s TO %s", sb.String(), user.UserHostToString("`"))})
	}

	sb.Reset()
	for i, dynamicPrivWithWgo := range user.PrivilegeSet.ToSliceDynamic(true) {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(dynamicPrivWithWgo)
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s ON *.* TO %s WITH GRANT OPTION", sb.String(), user.UserHostToString("`"))})
	}
	sb.Reset()
	for i, dynamicPrivWithoutWgo := range user.PrivilegeSet.ToSliceDynamic(false) {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(dynamicPrivWithoutWgo)
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s ON *.* TO %s", sb.String(), user.UserHostToString("`"))})
	}
	return sql.RowsToRowIter(rows...), nil
}
