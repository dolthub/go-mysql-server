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

// CreateUser represents the statement CREATE USER.
type CreateUser struct {
	IfNotExists     bool
	Users           []AuthenticatedUser
	DefaultRoles    []UserName
	TLSOptions      *TLSOptions
	AccountLimits   *AccountLimits
	PasswordOptions *PasswordOptions
	Locked          bool
	Attribute       string
}

var _ sql.Node = (*CreateUser)(nil)

// Schema implements the interface sql.Node.
func (n *CreateUser) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *CreateUser) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.UserName.StringWithQuote("", "")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateUser(%s%s)", ifNotExists, strings.Join(users, ", "))
}

// Resolved implements the interface sql.Node.
func (n *CreateUser) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *CreateUser) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *CreateUser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *CreateUser) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
