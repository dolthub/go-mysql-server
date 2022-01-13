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

// DropRole represents the statement DROP ROLE.
type DropRole struct {
	IfExists bool
	Roles    []UserName
}

// NewDropRole returns a new DropRole node.
func NewDropRole(ifExists bool, roles []UserName) *DropRole {
	return &DropRole{
		IfExists: ifExists,
		Roles:    roles,
	}
}

var _ sql.Node = (*DropRole)(nil)

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

// Resolved implements the interface sql.Node.
func (n *DropRole) Resolved() bool {
	return true
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
	return nil, fmt.Errorf("not yet implemented")
}
