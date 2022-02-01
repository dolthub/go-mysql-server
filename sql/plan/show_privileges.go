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

// ShowPrivileges represents the statement SHOW PRIVILEGES.
type ShowPrivileges struct{}

var _ sql.Node = (*ShowPrivileges)(nil)

// NewShowPrivileges returns a new ShowPrivileges node.
func NewShowPrivileges() *ShowPrivileges {
	return &ShowPrivileges{}
}

// Schema implements the interface sql.Node.
func (n *ShowPrivileges) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Privilege", Type: sql.LongText},
		&sql.Column{Name: "Context", Type: sql.LongText},
		&sql.Column{Name: "Comment", Type: sql.LongText},
	}
}

// String implements the interface sql.Node.
func (n *ShowPrivileges) String() string {
	return "SHOW PRIVILEGES"
}

// Resolved implements the interface sql.Node.
func (n *ShowPrivileges) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *ShowPrivileges) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *ShowPrivileges) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *ShowPrivileges) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}
