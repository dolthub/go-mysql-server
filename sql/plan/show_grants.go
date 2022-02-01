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

	"github.com/dolthub/go-mysql-server/sql"
)

// ShowGrants represents the statement SHOW GRANTS.
type ShowGrants struct {
	CurrentUser bool
	For         *UserName
	Using       []UserName
}

var _ sql.Node = (*ShowGrants)(nil)

// NewShowGrants returns a new ShowGrants node.
func NewShowGrants(currentUser bool, targetUser *UserName, using []UserName) *ShowGrants {
	return &ShowGrants{
		CurrentUser: currentUser,
		For:         targetUser,
		Using:       using,
	}
}

// Schema implements the interface sql.Node.
func (n *ShowGrants) Schema() sql.Schema {
	user := n.For
	if user == nil {
		user = &UserName{
			Name:    "root",
			Host:    "",
			AnyHost: true,
		}
	}
	return sql.Schema{{
		Name: fmt.Sprintf("Grants for %s", user.StringWithQuote("", "")),
		Type: sql.LongText,
	}}
}

// String implements the interface sql.Node.
func (n *ShowGrants) String() string {
	user := n.For
	if user == nil {
		user = &UserName{
			Name:    "root",
			Host:    "",
			AnyHost: true,
		}
	}
	return fmt.Sprintf("ShowGrants(%s)", user.StringWithQuote("", ""))
}

// Resolved implements the interface sql.Node.
func (n *ShowGrants) Resolved() bool {
	return true
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

// RowIter implements the interface sql.Node.
func (n *ShowGrants) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO: actually show grants here
	user := n.For
	if user == nil {
		user = &UserName{
			Name:    "root",
			Host:    "",
			AnyHost: true,
		}
	}
	return sql.RowsToRowIter(sql.Row{
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO %s WITH GRANT OPTION", user.StringWithQuote("'", ""))}), nil
}
