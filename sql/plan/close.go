// Copyright 2022 Dolthub, Inc.
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
	"io"

	"github.com/gabereiser/go-mysql-server/sql/expression"

	"github.com/gabereiser/go-mysql-server/sql"
)

// Close represents the CLOSE statement, which closes a cursor.
type Close struct {
	Name string
	pRef *expression.ProcedureReference
}

var _ sql.Node = (*Close)(nil)
var _ expression.ProcedureReferencable = (*Close)(nil)

// NewClose returns a new *Close node.
func NewClose(name string) *Close {
	return &Close{
		Name: name,
	}
}

// Resolved implements the interface sql.Node.
func (c *Close) Resolved() bool {
	return true
}

// String implements the interface sql.Node.
func (c *Close) String() string {
	return fmt.Sprintf("CLOSE %s", c.Name)
}

// Schema implements the interface sql.Node.
func (c *Close) Schema() sql.Schema {
	return nil
}

// Children implements the interface sql.Node.
func (c *Close) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (c *Close) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (c *Close) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// RowIter implements the interface sql.Node.
func (c *Close) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &closeIter{c}, nil
}

// WithParamReference implements the interface expression.ProcedureReferencable.
func (c *Close) WithParamReference(pRef *expression.ProcedureReference) sql.Node {
	nc := *c
	nc.pRef = pRef
	return &nc
}

// closeIter is the sql.RowIter of *Close.
type closeIter struct {
	c *Close
}

var _ sql.RowIter = (*closeIter)(nil)

// Next implements the interface sql.RowIter.
func (c *closeIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := c.c.pRef.CloseCursor(ctx, c.c.Name); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (c *closeIter) Close(ctx *sql.Context) error {
	return nil
}
