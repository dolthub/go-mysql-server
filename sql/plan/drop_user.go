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

// DropUser represents the statement DROP USER.
type DropUser struct {
	IfExists bool
	Users    []UserName
}

var _ sql.Node = (*DropUser)(nil)

// NewDropUser returns a new DropUser node.
func NewDropUser(ifExists bool, users []UserName) *DropUser {
	return &DropUser{
		IfExists: ifExists,
		Users:    users,
	}
}

// Schema implements the interface sql.Node.
func (n *DropUser) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *DropUser) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.StringWithQuote("", "")
	}
	ifExists := ""
	if n.IfExists {
		ifExists = "IfExists: "
	}
	return fmt.Sprintf("DropUser(%s%s)", ifExists, strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *DropUser) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *DropUser) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *DropUser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *DropUser) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
