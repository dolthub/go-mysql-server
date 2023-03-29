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

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

// Open represents the OPEN statement, which opens a cursor.
type Open struct {
	Name string
	pRef *expression.ProcedureReference
}

var _ sql.Node = (*Open)(nil)
var _ sql.CollationCoercible = (*Open)(nil)
var _ expression.ProcedureReferencable = (*Open)(nil)

// NewOpen returns a new *Open node.
func NewOpen(name string) *Open {
	return &Open{
		Name: name,
	}
}

// Resolved implements the interface sql.Node.
func (o *Open) Resolved() bool {
	return true
}

// String implements the interface sql.Node.
func (o *Open) String() string {
	return fmt.Sprintf("OPEN %s", o.Name)
}

// Schema implements the interface sql.Node.
func (o *Open) Schema() sql.Schema {
	return nil
}

// Children implements the interface sql.Node.
func (o *Open) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (o *Open) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(o, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (o *Open) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Open) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// RowIter implements the interface sql.Node.
func (o *Open) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &openIter{o, row}, nil
}

// WithParamReference implements the interface expression.ProcedureReferencable.
func (o *Open) WithParamReference(pRef *expression.ProcedureReference) sql.Node {
	no := *o
	no.pRef = pRef
	return &no
}

// openIter is the sql.RowIter of *Open.
type openIter struct {
	*Open
	row sql.Row
}

var _ sql.RowIter = (*openIter)(nil)

// Next implements the interface sql.RowIter.
func (o *openIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := o.pRef.OpenCursor(ctx, o.Name, o.row); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (o *openIter) Close(ctx *sql.Context) error {
	return nil
}
