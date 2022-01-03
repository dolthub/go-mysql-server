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

// CreateRole represents the statement CREATE ROLE.
type CreateRole struct {
	IfNotExists bool
	Roles       []UserName
}

// NewCreateRole returns a new CreateRole node.
func NewCreateRole(ifNotExists bool, roles []UserName) *CreateRole {
	return &CreateRole{
		IfNotExists: ifNotExists,
		Roles:       roles,
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
		roles[i] = role.StringWithQuote("", "")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateRole(%s%s)", ifNotExists, strings.Join(roles, ", "))
}

// Resolved implements the interface sql.Node.
func (n *CreateRole) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
}
